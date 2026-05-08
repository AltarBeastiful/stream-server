Read [](file:///home/remi/projects/streamio/stream-server/enginefs/src/backend/libtorrent/handle.rs#121-121), lines 121 to 210

Read [](file:///home/remi/projects/streamio/stream-server/enginefs/src/backend/libtorrent/stream.rs#92-92), lines 92 to 110

Now I have everything. Here is the full plan:

---

## Implementation Plan

### Context from the logs

The new file (`Mr.Inbetween`, 1875 pieces × 524KB, ~982MB) is **incomplete** at time of seek. The log burst at **14:41:03–14:41:04** shows the player firing 6 rapid range requests when the user opens/seeks the file. All of them arrive before even a single piece downloads, so `download_rate = 0` everywhere.

```
14:41:04 URGENT      piece 0    deadline 0ms   (offset=0 — initial play)
14:41:04 CONTAINER   piece 1874 deadline 200ms (offset=982542071 — moov/cues)
14:41:04 CRITICAL    piece 0    deadline 600ms ← OVERWRITES the 0ms URGENT deadline
14:41:04 CRITICAL    piece 37   deadline 600ms (offset=19414280)
14:41:04 CRITICAL    piece 18   deadline 600ms (offset=9577242)
```

The file then downloads in sequential libtorrent order at whatever pace it chooses. The user sees 100% download (~43 seconds later) and only then starts playing.

Then the **complete** file is observed at 14:42:04 and serves pieces 432–489 at **~7ms each** via `memory_read_piece_for_hash` — that's the C++ byte-by-byte copy bug.

---

### Bug 1 — `start_offset=5655` treated as UserScrub → overwrites URGENT deadline on piece 0 with 600ms

**Root cause**: `seek_type` is determined from `start_offset` raw value. `start_offset=5655 > 0` → `UserScrub`. But 5655 bytes is still inside piece 0. The CRITICAL block sets `set_piece_deadline(0, 600ms)`, overwriting the previous `set_piece_deadline(0, 0ms)` from the URGENT request.

**Fix** (handle.rs): Classify seek type by **start piece**, not raw offset. If the seek lands on `first_piece`, it still needs the head data urgently — treat it as `InitialPlayback`.

```rust
// BEFORE:
let seek_type = if start_offset == 0 {
    SeekType::InitialPlayback
} else { ... };

// AFTER: compute start_piece first, then classify by piece
let start_piece_for_seek = ((global_file_offset + start_offset) / piece_length) as i32;
let seek_type = if start_piece_for_seek == first_piece {
    SeekType::InitialPlayback
} else if start_offset >= end_threshold {
    SeekType::ContainerMetadata
} else {
    SeekType::UserScrub
};
```

---

### Bug 2 — UserScrub deadline is 600ms when torrent is paused (speed=0 → factor=2.0)

**Root cause**: `speed_factor` is computed from `download_rate`. When the torrent was just paused (monitor pauses every 30s), `download_rate = 0 → speed_factor = 2.0 → adjusted_deadline = 300 × 2 = 600ms`. This is backwards: paused means we just resumed it for streaming; we need the seek pieces **immediately**, not 600ms later.

**Fix** (handle.rs): Only penalize deadlines when the torrent was genuinely downloading slowly. A speed of zero because it was paused gets treated as "normal speed" (factor 1.0):

```rust
// BEFORE:
let speed_factor = if download_speed > 5_000_000 { 0.5 }
    else if download_speed > 1_000_000 { 1.0 }
    else if download_speed > 100_000  { 1.5 }
    else { 2.0 }; // 0 speed = 2x penalty — WRONG for just-resumed

// AFTER:
let speed_factor = if download_speed > 5_000_000 { 0.5 }
    else if download_speed > 1_000_000 { 1.0 }
    else if download_speed > 0        { 1.5 }
    else { 1.0 }; // zero = was paused/just started, don't penalise
```

Also, set the **first piece of every UserScrub window** to deadline 0ms (ASAP), not the base deadline. The first piece is what the player is blocked on:

```rust
SeekType::UserScrub => {
    // First piece = deadline 0 (ASAP), subsequent pieces get adjusted_deadline
    handle.set_piece_priority(actual_start_piece, 7);
    handle.set_piece_deadline(actual_start_piece, 0); // URGENT
    for i in 1..window_size {
        let p = actual_start_piece + i;
        if p <= last_piece {
            handle.set_piece_priority(p, 7);
            handle.set_piece_deadline(p, adjusted_deadline + (i - 1) * 10);
        }
    }
}
```

---

### Bug 3 — `clear_piece_deadlines()` removed from UserScrub, so old sequential deadlines choke the piece picker

**Root cause**: My previous fix removed `clear_piece_deadlines()` for UserScrub to prevent stalling piece 8 while another stream waited. But for an **incomplete** file this is fatal: pieces 0 to (seek_piece - 1) still have 0ms URGENT deadlines from earlier sequential streaming. The piece picker downloads them in order before reaching the seek position.

**Why it's safe to restore it now**: The `poll_read` wait loop I added in the previous session re-asserts `priority 7` + `deadline 0ms` on every wakeup (every 50ms). So even if `clear_piece_deadlines()` wipes an actively-waited piece, `poll_read` will re-assert it within 50ms. The stall is capped at 50ms, not 47 seconds.

**Fix** (handle.rs + stream.rs): Restore `clear_piece_deadlines()` for UserScrub:

```rust
// handle.rs — restore for incomplete files:
if matches!(seek_type, SeekType::InitialPlayback | SeekType::UserScrub) {
    handle.clear_piece_deadlines();
}

// stream.rs set_priorities():
SeekType::UserScrub => {
    self.handle.clear_piece_deadlines(); // restore this
}
```

---

### Bug 4 — `set_file_priority()` runs on every seek even for complete files

**Root cause**: The `for (idx, f) in all_files.iter()` priority-setting loop runs **outside** the `if !skip_prioritization` guard. For a 100% complete file, this is 100% wasted FFI work on every seek request. Each `set_file_priority` call crosses the Rust→C++→libtorrent boundary. With multi-file torrents this is many calls per seek.

**Fix** (handle.rs): Move the loop inside `if !skip_prioritization`:

```rust
if !skip_prioritization {
    // set file priorities
    for (idx, f) in all_files.iter().enumerate() { ... }
    // ... rest of prioritization
}
```

---

### Bug 5 — `memory_read_piece_for_hash` copies piece data byte-by-byte: O(n) at ~7ms/piece

**Root cause** (`memory_storage.cpp`):
```cpp
auto* buf = new uint8_t[data.size()];
std::memcpy(buf, data.data(), data.size());
for (size_t i = 0; i < data.size(); i++) {
    result.push_back(buf[i]);   // 524,288 individual push_back calls!
}
delete[] buf;
```
For 524KB pieces this is 524,288 loop iterations + an extra allocation. Observed cost: ~7ms per piece, meaning reading the entire 982MB file takes ~13 seconds of copy overhead alone.

**Fix** (`memory_storage.cpp`): Replace with a single `rust::Vec` reserve + insert range:

```cpp
result.reserve(data.size());
result.insert(result.begin(), 
    reinterpret_cast<const uint8_t*>(data.data()),
    reinterpret_cast<const uint8_t*>(data.data()) + data.size());
```

Or if `rust::Vec` doesn't support `insert` from iterators, use the `rust::Vec` backing approach:
```cpp
result.reserve(data.size());
for (char c : data) {
    result.push_back(static_cast<uint8_t>(c));
}
// removes the intermediate allocation at least
```

---

### Summary

| # | File | Bug | Impact |
|---|------|-----|--------|
| 1 | handle.rs | `offset=5655` → UserScrub → 600ms on piece 0 (overwrites 0ms URGENT) | Piece 0 takes 600ms min, all subsequent sequencing wrong |
| 2 | handle.rs | `speed=0` → `factor=2.0` → 600ms on seek piece | Seek piece waits 600ms before libtorrent even tries |
| 3 | handle.rs+stream.rs | No `clear_piece_deadlines` on UserScrub → old sequential 0ms deadlines choke picker | Piece picker downloads start→seek sequentially instead of jumping |
| 4 | handle.rs | `set_file_priority` runs for complete files | 20-40ms wasted FFI per seek on complete files |
| 5 | `memory_storage.cpp` | Byte-by-byte push_back loop + extra allocation | ~7ms per piece → complete file reads 10× slower than necessary |

Shall I implement all 5?