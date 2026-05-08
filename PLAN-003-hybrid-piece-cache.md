# PLAN-003: Hybrid Piece Cache (Memory → Disk Tiering)

## Problem

The current piece caching has two unbounded memory accumulators:

| Layer | Where | Issue |
|---|---|---|
| C++ `memory_torrent_storage` | `bindings/libtorrent-sys/cpp/memory_storage.cpp` | `std::map<piece_index_t, vector<char>>` — **no eviction**, every piece ever downloaded stays in RAM |
| Rust `PieceCacheManager` | `enginefs/src/piece_cache.rs` | moka cache with 5-min TTI, per-piece individual files — tolerable but inefficient (O(N) disk I/O calls, no zero-copy) |

A 1-hour HD movie at 1MB/piece = ~3,500 pieces × 1MB = **3.5 GB RAM** just in the C++ map, unbounded.

Additionally, pieces already watched are just as expensive to keep as pieces about to be watched — there is no streaming-awareness.

---

## Goals

1. **Cap memory** at a configurable window (e.g. 512 MB or N pieces around playback head).
2. **Evict cooler pieces to disk** — not drop them — so seek-back / re-watch works without re-downloading.
3. **Zero-copy HTTP serving** from both tiers (`Bytes` in memory, `mmap` slice from disk).
4. **Infinite disk** to start with (no GC), with clearly defined hooks for GC later.
5. **No regression** on the alert pump / `poll_read` latency improvements from PLAN-002.

---

## High-Level Architecture

```
libtorrent alert pump (piece_finished_alert)
         │
         ▼
  ┌─────────────────────────────────────────┐
  │           HybridPieceCache              │
  │                                         │
  │  ┌──────────────────────────────────┐   │
  │  │  Tier 1 — Hot Ring (RAM)         │   │ ← window of W pieces
  │  │  DashMap<PieceKey, Bytes>        │   │   centred on playback head
  │  └──────────────────────────────────┘   │
  │              │  evict (outside window)  │
  │              ▼                          │
  │  ┌──────────────────────────────────┐   │
  │  │  Tier 2 — Warm Flat File (disk)  │   │ ← flat file, fixed-slot layout
  │  │  mmap, offset = idx * piece_len  │   │   present-bitmap: roaring bitmap
  │  └──────────────────────────────────┘   │
  └─────────────────────────────────────────┘
         │
         ▼
  HTTP range handler (zero-copy slice)
         │
         ▼
  Player (VLC / mpv / browser)
```

The C++ `memory_torrent_storage` is the **write sink** for libtorrent. Once a piece is promoted to Tier 1, we call back into C++ to free it from the map, returning the memory to the OS immediately.

---

## Data Structures

### Tier 1 — Hot Ring

```rust
// enginefs/src/hybrid_cache.rs

use bytes::Bytes;
use dashmap::DashMap;

type PieceKey = (String, i32); // (info_hash_lower, piece_index)

struct HotTier {
    /// Piece data. Arc inside DashMap entry — cheap clone for HTTP serving.
    pieces: Arc<DashMap<PieceKey, Bytes>>,
    /// Per-torrent: current playback head (piece index).
    /// Updated by the stream layer on every seek / position advance.
    heads: Arc<DashMap<String, i32>>,
    /// Half-width of the sliding window in pieces.
    window_radius: i32,
    /// Hard cap: max total bytes across all torrents.
    max_bytes: u64,
}
```

**Window semantics**: a piece `p` is "hot" if  
`|p - head| <= window_radius`  
Pieces outside the window become candidates for disk promotion.

The window is **not a circular buffer in the traditional sense** — it's a logical range. The `DashMap` is the backing store; pieces are removed (promoted to disk) lazily by a background task.

Choosing `window_radius`:
- Forward bias: `[head - 8, head + window_radius]` (small look-behind for re-reads, large look-ahead for buffering).
- Default: `window_radius = 64` pieces (64 MB at 1 MB/piece).
- Configurable via `cache_size` settings already exposed.

### Tier 2 — Warm Flat File

```
{cache_dir}/{info_hash}/pieces.flat     ← data, sparse file
{cache_dir}/{info_hash}/pieces.bitmap   ← which slots are populated (roaring bitmap, serialised)
```

Layout:
```
offset(piece_idx) = piece_idx * piece_length_padded
```

`piece_length_padded` = the torrent's piece length, rounded up to 4096 (page-aligned).  
This makes every slot an exact multiple of the OS page size → `mmap` slice is zero-copy.

```rust
struct WarmTier {
    /// Per-torrent open flat file + its mmap.
    files: Arc<DashMap<String, WarmFile>>,
    /// Total bytes used on disk across all torrents.
    disk_bytes: Arc<AtomicU64>,
    /// Optional cap; 0 = unlimited (GC left for later).
    max_disk_bytes: u64,
}

struct WarmFile {
    /// The flat file, kept open for mmap.
    file: std::fs::File,
    /// Read-only memory map of the entire file.
    /// Refreshed (re-mapped) when the file grows.
    mmap: Arc<memmap2::Mmap>,
    piece_length_padded: u64,
    num_pieces: i32,
    /// Bitset: which pieces are written.
    present: roaring::RoaringBitmap,
    /// Lock protecting bitmap + file growth.
    write_lock: tokio::sync::Mutex<()>,
    /// Last-access time per piece (for future GC).
    last_access: DashMap<i32, std::time::Instant>,
}
```

**Why a flat file and not per-piece files?**
- Single `open()` / `mmap()` call per torrent vs. thousands.
- `mmap` slice is directly usable as `Bytes::from_static` (zero alloc in the read path).
- Disk pre-allocation (`fallocate`) makes writes O(1) regardless of order.
- Simple GC: evict a slot by zeroing its page and clearing the bitmap bit.

---

## Component Map — What Changes

```
enginefs/src/
├── hybrid_cache.rs        ← NEW: HybridPieceCache (replaces piece_cache.rs)
├── piece_cache.rs         ← KEEP as is until migration is complete, then remove
├── disk_cache.rs          ← KEEP (end-of-download file persistence, orthogonal)
├── cache.rs               ← KEEP (DataCache for HTTP 512KB chunks, unchanged)
└── backend/libtorrent/
    ├── mod.rs             ← alert pump: call hybrid_cache.put() instead of piece_cache
    ├── stream.rs          ← poll_read: call hybrid_cache.get_sync()
    └── handle.rs          ← expose head-update API to update window on seek

bindings/libtorrent-sys/cpp/
├── memory_storage.hpp     ← add evict_piece() declaration
├── memory_storage.cpp     ← implement evict_piece() (erase from map)
└── wrapper.h / wrapper.cpp← expose evict_piece_for_hash() to Rust via cxx
```

---

## Detailed Implementation Steps

### Step 1 — C++ side: `evict_piece()`

Add to `memory_torrent_storage`:
```cpp
// memory_storage.hpp
struct memory_torrent_storage {
    // ... existing fields ...

    /// Remove a piece from the map. Returns true if it was present.
    bool evict_piece(lt::piece_index_t piece);
};
```

```cpp
// memory_storage.cpp
bool memory_torrent_storage::evict_piece(lt::piece_index_t piece) {
    std::lock_guard<std::mutex> lock(mutex);
    return pieces.erase(piece) > 0;
}
```

Add a global entry point (like `memory_read_piece_for_hash`):
```cpp
// wrapper.h (cxx bridge)
void memory_evict_piece_for_hash(rust::Str info_hash, int32_t piece);
```

```cpp
// wrapper.cpp
void memory_evict_piece_for_hash(rust::Str info_hash, int32_t piece) {
    std::lock_guard<std::mutex> lock(g_dio_mutex);
    if (!g_memory_disk_io) return;
    auto st = g_memory_disk_io->get_storage_for_hash(std::string(info_hash));
    if (st) st->evict_piece(static_cast<lt::piece_index_t>(piece));
}
```

The `async_read` path in `memory_disk_io` must still work correctly if a piece was evicted — if the piece is missing it already returns an error (libtorrent will re-request from peers). This is safe because we only evict pieces that have already been acknowledged as finished by libtorrent.

> **Safety invariant**: `evict_piece()` is called **only after** the Rust side has safely stored the piece in Tier 2 (disk), verified via `fsync` / flush.

---

### Step 2 — Rust: `HybridPieceCache`

New file `enginefs/src/hybrid_cache.rs`:

```rust
pub struct HybridPieceCache {
    hot: HotTier,
    warm: WarmTier,
    /// Background eviction task handle.
    _eviction_task: tokio::task::JoinHandle<()>,
}

impl HybridPieceCache {
    pub fn new(config: HybridCacheConfig) -> Arc<Self> { ... }

    // Called by alert pump when piece_finished_alert fires.
    // Stores in hot tier; background task handles promotion to disk.
    pub fn put(&self, info_hash: &str, piece_idx: i32, data: Bytes) { ... }

    // Called by poll_read (sync context). Hot tier only.
    pub fn get_sync(&self, info_hash: &str, piece_idx: i32) -> Option<Bytes> { ... }

    // Called by get_file_reader (async context). Both tiers.
    pub async fn get(&self, info_hash: &str, piece_idx: i32) -> Option<Bytes> { ... }

    // Called by stream.rs on every seek / position advance.
    // Updates the window center; does NOT trigger eviction directly.
    pub fn update_head(&self, info_hash: &str, piece_idx: i32) { ... }

    // Called when torrent is removed from the engine.
    pub async fn remove_torrent(&self, info_hash: &str) { ... }
}
```

#### `put()` — alert pump path (async-friendly, but called from sync context)

```rust
pub fn put(&self, info_hash: &str, piece_idx: i32, data: Bytes) {
    let key = (info_hash.to_lowercase(), piece_idx as i32);
    self.hot.pieces.insert(key, data);
    // Eviction is handled separately by background task — no work here.
}
```

No `async`, no locks in the critical path. `DashMap::insert` is `O(1)` with fine-grained sharding.

#### `get_sync()` — poll_read path

```rust
pub fn get_sync(&self, info_hash: &str, piece_idx: i32) -> Option<Bytes> {
    let key = (info_hash.to_lowercase(), piece_idx);
    // 1. Hot tier (lock-free read via DashMap)
    if let Some(r) = self.hot.pieces.get(&key) {
        return Some(r.value().clone()); // Bytes::clone is O(1) — just bumps refcount
    }
    // 2. Warm tier (mmap slice — also sync, no async I/O needed)
    self.warm.get_sync(info_hash, piece_idx)
}
```

The warm tier `get_sync` returns a `Bytes` that is a sub-slice of the `mmap` region. Using `memmap2::Mmap` + `Bytes::from_static` (unsafe, but safe as long as the mmap is kept alive via `Arc<Mmap>` tied to the `Bytes` with a custom vtable or `bytes::Bytes::from_owner`).

#### `update_head()` — stream path

```rust
pub fn update_head(&self, info_hash: &str, piece_idx: i32) {
    self.hot.heads.insert(info_hash.to_lowercase(), piece_idx);
    // The eviction task will pick this up on its next tick.
}
```

---

### Step 3 — Background Eviction Task

Spawned once on `HybridPieceCache::new()`. Runs on a 1-second interval.

```
loop:
  for each torrent in hot.pieces (grouped by info_hash):
    head = hot.heads[info_hash] or 0
    for each piece in hot.pieces[info_hash]:
      if |piece - head| > window_radius AND piece < head:
        // Behind the playback head and outside window → promote to disk
        data = hot.pieces.remove(piece)
        warm.write(info_hash, piece, data)   // pwrite() at fixed offset
        memory_evict_piece_for_hash(info_hash, piece)   // free C++ RAM
      elif |piece - head| > window_radius AND piece > head:
        // Far ahead of head (pre-fetched but not yet watched)
        // Keep in hot tier; don't evict — seeking forward should be fast
  
  // Optional: if total hot bytes > max_bytes, also evict ahead-pieces by distance
  
  sleep(1s)
```

The logic gives a **forward bias**: pieces behind the head are the first to be evicted to disk. Pieces ahead (pre-fetched, about to be needed) stay in RAM longest.

For a re-watch / seek-back scenario, pieces on disk are served via the warm tier's mmap — no re-download from peers.

---

### Step 4 — Warm Tier: Flat File Write

```rust
impl WarmFile {
    async fn write_piece(&self, piece_idx: i32, data: &[u8]) -> io::Result<()> {
        let _guard = self.write_lock.lock().await;
        let offset = piece_idx as u64 * self.piece_length_padded;
        
        // Grow file if needed (sparse, so this is cheap on Linux)
        let required_len = offset + self.piece_length_padded;
        if self.file.metadata()?.len() < required_len {
            self.file.set_len(required_len)?;
        }
        
        // pwrite — no seek, no file position mutation
        use std::os::unix::fs::FileExt;
        self.file.write_at(data, offset)?;
        
        // Update bitmap
        self.present.insert(piece_idx as u32);
        
        // Re-map only if we grew the file
        // (existing mmap slices remain valid for the old region)
        self.remap_if_grown(required_len)?;
        
        Ok(())
    }
}
```

**No `fsync` on every write** — the OS page cache handles durability guarantees here. We only care about durability for pieces we intend to evict from C++ RAM. We add a targeted `fdatasync` before calling `memory_evict_piece_for_hash`:

```rust
// In eviction task, before evicting from C++:
warm_file.file.sync_data()?;   // flush dirty pages to disk
memory_evict_piece_for_hash(&info_hash, piece_idx);
```

---

### Step 5 — Warm Tier: Zero-Copy mmap Read

```rust
impl WarmFile {
    fn get_piece(&self, piece_idx: i32, data_len: usize) -> Option<Bytes> {
        if !self.present.contains(piece_idx as u32) {
            return None;
        }
        let offset = piece_idx as u64 * self.piece_length_padded;
        let mmap = self.mmap.load(); // Arc<Mmap>
        
        // Slice the mmap region. Bytes::from_owner ties lifetime to Arc<Mmap>.
        let slice = &mmap[offset as usize .. offset as usize + data_len];
        
        // Safe: mmap Arc keeps the mapping alive as long as Bytes is alive.
        let mmap_clone = Arc::clone(&mmap);
        let bytes = Bytes::from_owner(MmapBytes { mmap: mmap_clone, slice_ptr: slice.as_ptr(), len: slice.len() });
        Some(bytes)
    }
}
```

`Bytes::from_owner` (stable in `bytes` 1.6+) accepts any `T: AsRef<[u8]>` value and takes ownership, calling `drop` when the last reference is gone. We wrap `Arc<Mmap>` + offset + length into a small struct that implements `AsRef<[u8]>` by returning the slice. This is fully safe and zero-copy.

---

### Step 6 — Integration: Alert Pump

In `enginefs/src/backend/libtorrent/mod.rs`, replace:

```rust
// BEFORE
piece_cache.put_piece(&info_hash, piece_idx, piece_data).await;
```

with:

```rust
// AFTER
let bytes = Bytes::copy_from_slice(&piece_data); // one allocation, from the cxx Vec
hybrid_cache.put(&info_hash, piece_idx, bytes);
waiter_registry.notify_piece_finished(&info_hash, piece_idx);
```

The `Bytes::copy_from_slice` is unavoidable here because the data comes from a `rust::Vec<u8>` returned by the cxx FFI call. This is a single allocation per piece (as before).

---

### Step 7 — Integration: `poll_read` / stream.rs

Replace `piece_cache.get_piece_sync()` calls with `hybrid_cache.get_sync()`. The signature is identical (returns `Option<Bytes>`).

Replace `piece_cache.has_piece_sync()` with `hybrid_cache.has_piece_sync()` which checks both tiers (disk bitmap is in-memory, so the check is lock-free).

Update `update_head` call site — call `hybrid_cache.update_head(info_hash, current_piece)` on every new stream start and every seek (in `get_file_reader` and on seek in stream.rs).

---

### Step 8 — Configuration

Add to the existing settings structure:

```rust
pub struct HybridCacheConfig {
    /// Half-width of the hot (RAM) window in pieces.
    /// Pieces outside [head - back_window, head + front_window] are evicted to disk.
    pub front_window_pieces: i32,   // default: 64 (≈64 MB at 1 MB/piece)
    pub back_window_pieces: i32,    // default: 8  (small look-behind)
    
    /// Hard cap on total RAM used by the hot tier, across all torrents.
    /// 0 = use window sizing only.
    pub max_hot_bytes: u64,         // default: 512 MB
    
    /// Root directory for flat files (warm tier).
    pub warm_dir: PathBuf,
    
    /// Maximum disk usage for the warm tier.
    /// 0 = unlimited (GC disabled; add GC later).
    pub max_warm_bytes: u64,        // default: 0 (unlimited)
    
    /// Eviction task interval.
    pub eviction_interval_secs: u64, // default: 2
}
```

---

## GC Hook (future work)

The warm tier already tracks `last_access: DashMap<i32, Instant>` per piece per torrent. When GC is eventually added:

```
// GC task (not implemented now)
loop:
  if warm.disk_bytes > max_warm_bytes:
    // Find LRU piece across all torrents
    candidate = warm.files.iter()
        .flat_map(|f| f.last_access.iter())
        .min_by_key(|(_, t)| *t)
    // Zero the slot in the flat file
    warm.zero_slot(candidate.info_hash, candidate.piece_idx)
    warm.present.remove(candidate.piece_idx)
    warm.disk_bytes -= piece_length_padded
```

Zeroing a slot on a sparse file on Linux returns the disk blocks to the OS (`fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`). This is more efficient than `truncate` and preserves the fixed-slot layout.

---

## Migration Plan

To avoid a flag day, keep `PieceCacheManager` alive during the migration:

| Phase | What changes | Verifiable outcome |
|---|---|---|
| 1 | Add C++ `evict_piece_for_hash()` (Step 1), compile, run tests | C++ can free individual pieces; no regression |
| 2 | Implement `WarmTier` (Steps 2–5), unit tests for flat file read/write/mmap | Flat file stores and retrieves pieces correctly |
| 3 | Implement `HotTier` + eviction task (Steps 2–3), wire up | Pieces evict from RAM → disk; memory cap respected |
| 4 | Integrate alert pump (Step 6) — parallel: both caches receive pieces | Both caches hot; compare behaviour |
| 5 | Switch `poll_read` to `hybrid_cache.get_sync()` (Step 7) | stream.rs reads from hybrid cache |
| 6 | Remove `PieceCacheManager` | Code simplification |
| 7 | Verify `memory_storage.cpp` map size stays bounded under a long stream | Main goal validated |

---

## Key Invariants / Gotchas

### Invariant 1: Evict from C++ only after warm write is durable
`fdatasync` must complete before `memory_evict_piece_for_hash`. Otherwise a crash between eviction and flush means the piece is lost and libtorrent will need to re-download it.

### Invariant 2: mmap validity
The `Arc<Mmap>` must outlive any `Bytes` slice derived from it. The `Bytes::from_owner` pattern handles this. Never use `Bytes::from_static` on a mmap region.

### Invariant 3: Bitmap and flat file are consistent
The bitmap is in-memory (rebuilt on startup by scanning the flat file header or a sidecar). On clean shutdown, write the bitmap to `pieces.bitmap`. On dirty startup (crash recovery), either re-scan the file or conservatively mark all slots as absent and let libtorrent re-download (the data is still there, but we don't know which slots are valid).

### Invariant 4: No eviction of pieces libtorrent still needs
libtorrent may call `async_read()` for a piece it wrote (e.g. during hash verification). We must not evict a piece from C++ until libtorrent has emitted `piece_finished_alert` for it. Since we only call `evict_piece` from the eviction task, which only runs after `put()` (triggered by `piece_finished_alert`), this is satisfied.

### Invariant 5: Window update on seek, not just on advance
`update_head()` must be called on every new stream instantiation and every seek. If it is only called when reading advances naturally, the window will lag the actual playback position and pieces may be evicted too aggressively ahead of a seek destination.

---

## Expected Memory Behaviour

Before (current state):
```
Time →
Pieces in C++ map: 0──────────────────────────→ N  (monotonically growing)
RAM: 0 → movie_size (e.g. 3.5 GB for 1h HD film)
```

After (hybrid cache):
```
Time →
RAM (hot tier):  [head-8 ────────── head+64]   (sliding, bounded ≈ 72 MB)
Disk (warm tier): [0 ──────────────── head-9]   (growing, unbounded until GC)
C++ map:         [head-8 ─────────── head+64]   (same window as hot tier)
```

---

## File List Summary

| File | Action | Notes |
|---|---|---|
| `bindings/libtorrent-sys/cpp/memory_storage.hpp` | edit | Add `evict_piece()` to struct |
| `bindings/libtorrent-sys/cpp/memory_storage.cpp` | edit | Implement `evict_piece()` |
| `bindings/libtorrent-sys/cpp/wrapper.h` | edit | Export `memory_evict_piece_for_hash` |
| `bindings/libtorrent-sys/cpp/wrapper.cpp` | edit | Implement global entry point |
| `bindings/libtorrent-sys/src/lib.rs` | edit | Add cxx bridge declaration |
| `enginefs/src/hybrid_cache.rs` | **new** | `HybridPieceCache`, `HotTier`, `WarmTier`, `WarmFile` |
| `enginefs/src/backend/libtorrent/mod.rs` | edit | Alert pump: use `hybrid_cache.put()` |
| `enginefs/src/backend/libtorrent/stream.rs` | edit | `poll_read`: use `hybrid_cache.get_sync()` |
| `enginefs/src/backend/libtorrent/handle.rs` | edit | `update_head()` on seek |
| `enginefs/src/lib.rs` | edit | Register `hybrid_cache` module, wire config |
| `enginefs/Cargo.toml` | edit | Add `memmap2`, `roaring`, `bytes`, `dashmap` |

---

## Cargo.toml Additions

```toml
[dependencies]
bytes    = "1.6"    # Bytes::from_owner (zero-copy slicing)
memmap2  = "0.9"    # mmap for warm tier
roaring  = "0.10"   # compact present-bitmap
dashmap  = "6"      # lock-free concurrent map for hot tier
```

(`bytes` and `dashmap` may already be present — check before adding.)
