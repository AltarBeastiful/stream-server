# PLAN-002: Seek Delay on Incomplete Torrents

## Problem Statement

When a user seeks/scrolls while a torrent is still downloading, there is a **4–9 second delay** between the seek action and the server starting to serve the new position. The video appears frozen, then eventually jumps to the new position.

---

## Evidence from Logs

```
# Old stream stuck waiting for piece 752 (sequential download)
WAITING for piece 752 (pos=788529152, peers=10, speed=2.6MB/s, ...)
2026-05-05T15:15:07.438Z  WAITING for piece 752 ...
2026-05-05T15:15:07.490Z  WAITING for piece 752 ...
...
2026-05-05T15:15:07.798Z  WAITING for piece 752 ...

# Piece 752 FINALLY arrives
2026-05-05T15:15:07.858Z  poll_read: Direct read piece 752 (1048576 bytes)

# NEW seek request handler fires 2ms later — was queued for ~5s
2026-05-05T15:15:07.860Z  Stream started for 7dcf3ecc... file_idx=0
2026-05-05T15:15:07.862Z  [DIRECT STREAMING] Preparing file 0 (offset=251631811)
2026-05-05T15:15:07.864Z  get_file_reader: CRITICAL - 4 pieces from piece 239 (deadlines 300ms+)
```

The **"Stream started"** log is emitted at the very top of `stream_video()`, before any work is done. The 2ms correlation between piece 752 arriving and the new handler starting is not coincidental — it is causal.

---

## Root Cause Analysis

### Causal Chain

1. **User seeks** at ~15:15:02 (estimated) to byte offset 251,631,811 (piece 239).
2. **Player sends new HTTP range request** to the server.
3. **Server cannot process the new request** — all tokio worker threads are momentarily blocked, OR the request queues behind a shared resource the old stream holds.
4. Piece 752 downloads and the old stream's `poll_read` returns `Ready`.
5. The old HTTP connection delivers piece 752 data to the player.
6. **Simultaneously**, the new HTTP request handler finally starts executing.

### Why Does the New Request Wait for Piece 752?

There are **two compounding causes**:

#### Cause A: `futures::executor::block_on` inside `poll_read` blocks tokio worker threads

Every 50ms, `poll_read` is called. At the very top, **before** the `have_piece()` check:

```rust
// stream.rs — runs on every poll_read invocation
if let Some(piece_data) =
    futures::executor::block_on(self.piece_cache.get_piece(&self.info_hash, piece))
```

`moka::future::Cache::get()` is an async function. `futures::executor::block_on()` creates a mini event loop and **parks the current tokio worker thread** until the future resolves. Moka's `get` involves:
- Acquiring internal segment locks
- Running maintenance tasks (eviction, TTL processing)
- Returning `None` for cache misses

Under high load (multiple concurrent streams, each calling `block_on` every 50ms), this can block all available tokio threads simultaneously. When all threads are blocked, **new HTTP request handlers cannot start** — they sit in the tokio scheduler queue with no thread to run them.

This explains the delay: the new seek request arrived and was accepted by the TCP stack, but the tokio scheduler couldn't assign a thread to handle it because all threads were in `block_on` for the ongoing `poll_read` loops.

#### Cause B: `session.write()` in the alert pump starves `get_file_reader`

The alert pump runs every 5ms and takes an **exclusive write lock** on the `LibtorrentSession`:

```rust
// mod.rs — alert pump
let mut s = alert_session.write().await;  // EXCLUSIVE write lock
let alerts = s.pop_alerts();
// For each piece_finished_alert:
let piece_data = libtorrent_sys::memory_read_piece_for_hash(...); // FFI under write lock
```

`get_file_reader` (called when handling the new seek request) needs a **read lock**:

```rust
// handle.rs
let session = self.session.read().await;  // blocked until write lock releases
```

With `tokio::sync::RwLock` (which is **write-preferring**): once a writer is waiting, all NEW readers are blocked too. If the alert pump fires frequently AND processes multiple piece alerts (which happens at 2.6MB/s with 1MB pieces), the write lock is held long enough to visibly delay `get_file_reader`.

The combined effect: Cause A starves the executor so the new request handler can't even START; Cause B then adds additional delay when it does start.

---

## Fixes

### Fix 1 — Replace `futures::executor::block_on` with a synchronous moka cache lookup

**File**: `enginefs/src/piece_cache.rs` + `enginefs/src/backend/libtorrent/stream.rs`

`moka::future::Cache` also exposes **synchronous** methods. The `get` operation on a moka future cache can be made synchronous if we use the `moka::sync::Cache` instead, OR if we call the blocking-compatible `get_blocking` variant.

Since we're in a `poll_read` (a synchronous `AsyncRead` impl), the correct approach is:

**Option A** (preferred): Add a `get_piece_sync` method to `PieceCacheManager` that uses `Cache::get_blocking()` — available on moka's future Cache via the `blocking()` accessor:

```rust
// piece_cache.rs
pub fn get_piece_sync(&self, info_hash: &str, piece_idx: i32) -> Option<Arc<Vec<u8>>> {
    let key = (info_hash.to_lowercase(), piece_idx);
    self.cache.blocking().get(&key)
}

pub fn has_piece_sync(&self, info_hash: &str, piece_idx: i32) -> bool {
    let key = (info_hash.to_lowercase(), piece_idx);
    self.cache.blocking().contains_key(&key)
}
```

Then in `stream.rs`, replace:
```rust
// BEFORE — blocks tokio thread
if let Some(piece_data) =
    futures::executor::block_on(self.piece_cache.get_piece(&self.info_hash, piece))

// AFTER — synchronous, does not block tokio thread
if let Some(piece_data) = self.piece_cache.get_piece_sync(&self.info_hash, piece)
```

Also replace the prefetch `has_piece` check in the spawn block (that one is inside `tokio::spawn` so async is fine there — leave it).

**Impact**: Eliminates tokio thread starvation from `block_on`. New HTTP requests can be scheduled immediately on a free thread.

---

### Fix 2 — Move alert pump FFI calls outside the write lock

**File**: `enginefs/src/backend/libtorrent/mod.rs`

Currently the alert pump holds `session.write()` while calling `memory_read_piece_for_hash` for every finished piece:

```rust
let mut s = alert_session.write().await;  // write lock acquired
let alerts = s.pop_alerts();              // fast
// For each piece_finished_alert:         // write lock STILL HELD during all of this:
let piece_data = libtorrent_sys::memory_read_piece_for_hash(...);  // FFI + g_dio_mutex
```

Fix: collect alerts while holding the write lock, then release it **before** processing piece data:

```rust
// AFTER
let piece_alerts: Vec<_> = {
    let mut s = alert_session.write().await;
    let alerts = s.pop_alerts();
    // Collect only what we need, drop write lock
    alerts.into_iter()
        .filter(|a| a.alert_type == piece_finished_alert_type && a.piece_index >= 0)
        .collect()
};
// write lock released here — get_file_reader can now acquire read lock

// Process piece data without holding session write lock
for alert in piece_alerts {
    let piece_data = libtorrent_sys::memory_read_piece_for_hash(&alert.info_hash, alert.piece_index);
    // ... cache and notify
}
```

**Impact**: `get_file_reader`'s `session.read()` no longer has to wait for piece data FFI calls. Reduces the starvation window from potentially tens of ms to microseconds (just `pop_alerts()`).

---

### Fix 3 — Proactive stream cancellation on seek

**Files**: `enginefs/src/lib.rs`, `enginefs/src/backend/libtorrent/stream.rs`, `enginefs/src/engine.rs`, `enginefs/src/backend/libtorrent/handle.rs`

Even after Fixes 1 and 2, the old stream's connection remains open while piece 752 downloads. If the browser hit its per-origin connection limit (6 for HTTP/1.1), the new request could still queue. More importantly: there is never a good reason to keep the old sequential stream open once a new stream for the same file starts.

**Mechanism**: `CancellationToken` per active file stream.

#### 3a. Add cancellation tokens to `BackendEngineFS` (`lib.rs`)

```rust
use tokio_util::sync::CancellationToken;

pub struct BackendEngineFS<B: TorrentBackend> {
    // ... existing fields ...
    /// Cancellation tokens for active streams; keyed by (info_hash, file_idx)
    stream_cancel_tokens: Arc<RwLock<HashMap<(String, usize), CancellationToken>>>,
}
```

#### 3b. In `on_stream_start`, cancel old stream and issue new token (`lib.rs`)

```rust
pub async fn on_stream_start(&self, info_hash: &str, file_idx: usize) 
    -> CancellationToken 
{
    // ... existing active_file / active_streams / active_file_streams logic ...

    let new_token = CancellationToken::new();
    let mut tokens = self.stream_cancel_tokens.write().await;
    let key = (info_hash.to_lowercase(), file_idx);

    // Cancel any existing stream for this (file, idx)
    if let Some(old_token) = tokens.remove(&key) {
        old_token.cancel();  // signals old poll_read to abort
        tracing::info!(
            "on_stream_start: cancelled old stream for {} idx={} (seek preemption)",
            info_hash, file_idx
        );
    }
    tokens.insert(key, new_token.clone());
    new_token
}
```

`on_stream_start` must return the token so it flows into the stream reader.

#### 3c. Thread token through `engine.get_file()` and `get_file_reader()` (`engine.rs`, `handle.rs`)

```rust
// engine.rs
pub async fn get_file(
    self: &Arc<Self>,
    file_idx: usize,
    start_offset: u64,
    priority: u8,
    cancel: CancellationToken,   // new param
) -> Option<FileHandle<H>> {
    // ...
    let reader = self.handle
        .get_file_reader(file_idx, start_offset, priority, bitrate, cancel)
        .await.ok()?;
    // ...
}
```

#### 3d. Store token in `LibtorrentFileStream` and check in `poll_read` (`stream.rs`)

```rust
pub(crate) struct LibtorrentFileStream {
    // ... existing fields ...
    cancel: CancellationToken,
}

// In poll_read, at the very top:
fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>)
    -> Poll<io::Result<()>>
{
    // Abort immediately if the stream has been preempted by a newer seek
    if self.cancel.is_cancelled() {
        return Poll::Ready(Ok(())); // EOF — closes the HTTP connection cleanly
    }
    // ... rest of poll_read ...
}
```

Returning `Poll::Ready(Ok(()))` with 0 bytes written signals EOF. axum/hyper will close the HTTP connection cleanly, immediately freeing the connection slot.

#### 3e. Clean up token in `on_stream_end` (`lib.rs`)

```rust
pub async fn on_stream_end(&self, info_hash: &str, file_idx: usize) {
    // ... existing stream counter cleanup ...
    
    // Remove token if it's still for this stream (not already replaced by a newer one)
    let mut tokens = self.stream_cancel_tokens.write().await;
    let key = (info_hash.to_lowercase(), file_idx);
    tokens.remove(&key);
}
```

**Impact**: When the user seeks, the old connection closes within one `poll_read` cycle (≤50ms). The browser's connection slot is freed instantly. The new seek request is handled with zero queueing.

---

### Fix 4 — Also fix the 5 previously identified bugs (from PLAN-001)

These were identified in the previous analysis but not yet implemented. They compound the seek delay:

| # | Location | Bug | Effect |
|---|----------|-----|--------|
| 1 | `handle.rs` | `offset=5655` → UserScrub → 600ms on piece 0 | Overwrites 0ms URGENT deadline |
| 2 | `handle.rs` | `speed=0` → `factor=2.0` → 600ms seeks | All seek pieces get 600ms minimum deadline |
| 3 | `handle.rs`+`stream.rs` | No `clear_piece_deadlines` on UserScrub | Sequential deadlines block piece picker |
| 4 | `handle.rs` | `set_file_priority` runs for complete files | 20-40ms wasted FFI per seek |
| 5 | `memory_storage.cpp` | Byte-by-byte `push_back` loop + extra alloc | ~7ms per piece read instead of <1ms |

These are documented in PLAN-001 but included here as part of the complete seek-delay fix.

---

## Summary of Changes

| File | Change | Fixes |
|------|--------|-------|
| `enginefs/src/piece_cache.rs` | Add `get_piece_sync()` and `has_piece_sync()` using `cache.blocking()` | Fix 1 |
| `enginefs/src/backend/libtorrent/stream.rs` | Replace `block_on(get_piece)` with `get_piece_sync()` | Fix 1 |
| `enginefs/src/backend/libtorrent/mod.rs` | Release `session.write()` before processing piece data in alert pump | Fix 2 |
| `enginefs/src/lib.rs` | Add `stream_cancel_tokens` map; return token from `on_stream_start`; clean up in `on_stream_end` | Fix 3 |
| `enginefs/src/engine.rs` | Thread `CancellationToken` through `get_file()` | Fix 3 |
| `enginefs/src/backend/libtorrent/handle.rs` | Thread `CancellationToken` through `get_file_reader()` | Fix 3 |
| `enginefs/src/backend/libtorrent/stream.rs` | Store token in `LibtorrentFileStream`; check at top of `poll_read` | Fix 3 |
| `server/src/routes/stream.rs` | Pass token returned from `on_stream_start` through to `get_file()` | Fix 3 |

## Expected Result After Fixes

| Metric | Before | After |
|--------|--------|-------|
| Seek delay (incomplete torrent) | 4–9 seconds | < 100ms |
| Time to cancel old stream on seek | Until piece downloads | ≤ 50ms (one poll cycle) |
| tokio thread blocking per poll_read | 1–5ms (block_on moka) | 0 (sync lookup) |
| Alert pump write lock hold time | ms × pieces | μs (pop_alerts only) |
| Complete torrent seek overhead | 20-40ms (FFI) | < 1ms |
| Per-piece read time (complete torrent) | ~7ms | < 1ms |
