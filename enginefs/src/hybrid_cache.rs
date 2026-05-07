//! Hybrid two-tier piece cache: bounded hot RAM tier + flat-file warm disk tier.
//!
//! ## Architecture
//!
//! ```text
//! piece_finished_alert
//!        │
//!        ▼ put()
//! ┌─────────────────────────────────┐
//! │  Hot Tier (moka::sync::Cache)   │  ← bounded by max_hot_bytes
//! │  LRU size-based eviction        │
//! └──────────────┬──────────────────┘
//!                │ eviction_listener
//!                ▼
//!     sync_channel<EvictedPiece>
//!                │
//!                ▼ drain task (spawn_blocking)
//! ┌─────────────────────────────────┐
//! │  Warm Tier (flat file / pwrite) │  ← unbounded, one file per torrent
//! │  offset = piece_idx * padded_pl │
//! └──────────────┬──────────────────┘
//!                │ after flush
//!                ▼
//!    memory_evict_piece_for_hash()   ← free C++ RSS
//! ```
//!
//! ## Read path (poll_read, sync)
//!
//! `get_sync()` checks hot tier first (DashMap-equivalent via moka), then warm tier
//! (pread from flat file). Both are O(1) and lock-free in the common case.
//!
//! ## Warm tier layout
//!
//! Each piece occupies a fixed-size slot in a per-torrent sparse file:
//! ```text
//! offset(piece_idx) = piece_idx * piece_length_padded
//! ```
//! `piece_length_padded` = torrent `piece_length` rounded up to 4096 (page boundary).
//! This guarantees 4 KiB-aligned pwrite calls on Linux.
//!
//! Actual data length (varies for the last piece) is stored in an in-memory
//! `HashMap<i32, usize>` so reads know exactly how many bytes to return.

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

type PieceKey = (String, i32); // (info_hash_lower, piece_index)

/// Shared map of piece pin counts.  Keyed by (info_hash_lower, piece_idx).
/// Each outstanding reader of a piece (via the direct C++ memory path) holds
/// one count increment for the duration of its read.  The drain task checks
/// this map before calling `memory_evict_piece_for_hash()`: if a piece is
/// pinned it defers the C++ free until all readers have finished.
type PinCounts = Arc<Mutex<HashMap<PieceKey, u32>>>;

/// RAII guard that decrements the pin count for `key` when dropped.
/// Returned by [`HybridPieceCache::pin_piece`].
pub struct PiecePinGuard {
    key: PieceKey,
    counts: PinCounts,
}

impl Drop for PiecePinGuard {
    fn drop(&mut self) {
        let mut map = self.counts.lock();
        if let Some(c) = map.get_mut(&self.key) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                map.remove(&self.key);
            }
        }
    }
}

// PiecePinGuard is Send because Arc<Mutex<...>>, String, and i32 are all Send.

/// A piece whose C++ eviction was deferred because a reader held a pin.
/// Only the identity is needed — the warm-tier write has already completed.
struct DeferredEviction {
    info_hash: String,
    piece_idx: i32,
}

// ============================================================================
// Eviction message
// ============================================================================

/// A piece evicted from the hot tier that needs to be written to the warm tier.
struct EvictedPiece {
    info_hash: String,
    piece_idx: i32,
    data: Bytes,
}

// ============================================================================
// Warm tier — per-torrent flat file
// ============================================================================

/// Per-torrent flat file on disk.
///
/// Each piece occupies a fixed `piece_length_padded`-byte slot at offset
/// `piece_idx * piece_length_padded`. Actual data lengths are stored separately
/// in `present` to handle the shorter last piece of a torrent.
struct WarmFile {
    file: std::fs::File,
    /// Torrent piece_length rounded up to the next 4096-byte boundary.
    piece_length_padded: u64,
    /// `piece_idx` → actual byte count written (varies for the last piece).
    present: HashMap<i32, usize>,
    /// Current allocated file length (bytes). Grows monotonically.
    allocated_len: u64,
}

impl WarmFile {
    fn new(path: &std::path::Path, piece_length: u64) -> std::io::Result<Self> {
        // Page-align the slot size so every pwrite hits page boundaries.
        let piece_length_padded = (piece_length + 4095) & !4095;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            file,
            piece_length_padded,
            present: HashMap::new(),
            allocated_len: 0,
        })
    }

    /// Write `data` to the slot for `piece_idx`. Grows the file if needed.
    fn write_piece(&mut self, piece_idx: i32, data: &[u8]) -> std::io::Result<()> {
        let offset = piece_idx as u64 * self.piece_length_padded;
        let required = offset + self.piece_length_padded;
        if self.allocated_len < required {
            // On Linux, set_len on a sparse file does not pre-allocate physical blocks.
            self.file.set_len(required)?;
            self.allocated_len = required;
        }
        self.file.write_at(data, offset)?;
        self.present.insert(piece_idx, data.len());
        Ok(())
    }

    /// Read the piece at `piece_idx`. Returns `None` if not present.
    fn read_piece(&self, piece_idx: i32) -> Option<Bytes> {
        let &data_len = self.present.get(&piece_idx)?;
        let offset = piece_idx as u64 * self.piece_length_padded;
        let mut buf = vec![0u8; data_len];
        // pread — no seek, no file position mutation, safe across threads under RwLock read.
        self.file.read_at(&mut buf, offset).ok()?;
        Some(Bytes::from(buf))
    }

    fn has_piece(&self, piece_idx: i32) -> bool {
        self.present.contains_key(&piece_idx)
    }
}

// ============================================================================
// Warm tier registry
// ============================================================================

/// Registry of per-torrent warm (disk) tier files.
struct WarmTier {
    dir: PathBuf,
    /// Torrent piece_length (actual, not padded) per info_hash.
    /// Populated by `register()` when metadata is first available.
    piece_lengths: RwLock<HashMap<String, u64>>,
    /// Open warm files per info_hash (created lazily on first write).
    files: RwLock<HashMap<String, WarmFile>>,
}

impl WarmTier {
    fn new(dir: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&dir);
        Self {
            dir,
            piece_lengths: RwLock::new(HashMap::new()),
            files: RwLock::new(HashMap::new()),
        }
    }

    /// Register a torrent's piece length. Must be called before pieces can be
    /// evicted to this tier. Idempotent (second call with same hash is a no-op).
    fn register(&self, info_hash: &str, piece_length: u64) {
        let mut pl = self.piece_lengths.write();
        pl.entry(info_hash.to_lowercase())
            .or_insert(piece_length);
    }

    /// Write a piece to disk. Returns `true` if the piece was successfully persisted,
    /// `false` if skipped or an error occurred. The caller must NOT free C++ RAM when
    /// this returns `false`. Called from the blocking drain task.
    fn write_piece_sync(&self, info_hash: &str, piece_idx: i32, data: &Bytes) -> bool {
        let ih = info_hash.to_lowercase();
        let piece_length = {
            let pl = self.piece_lengths.read();
            match pl.get(&ih).copied() {
                Some(l) => l,
                None => {
                    warn!(
                        "WarmTier: piece_length unknown for {} — skipping disk write of piece {}",
                        ih, piece_idx
                    );
                    return false;
                }
            }
        };

        let mut files = self.files.write();
        if !files.contains_key(&ih) {
            let path = self.dir.join(&ih).join("pieces.flat");
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match WarmFile::new(&path, piece_length) {
                Ok(wf) => { files.insert(ih.clone(), wf); }
                Err(e) => {
                    warn!(
                        "WarmTier: cannot create flat file for {} — piece {} stays in C++ RAM: {}",
                        ih, piece_idx, e
                    );
                    return false;
                }
            }
        }
        let wf = files.get_mut(&ih).expect("just inserted");

        match wf.write_piece(piece_idx, data) {
            Ok(()) => {
                debug!(
                    "WarmTier: piece {} for {} persisted ({} bytes)",
                    piece_idx, ih, data.len()
                );
                true
            }
            Err(e) => {
                warn!(
                    "WarmTier: write_piece failed for {}:{}: {}",
                    ih, piece_idx, e
                );
                false
            }
        }
    }

    /// Read a piece from disk. Returns `None` if not present.
    fn get_piece_sync(&self, info_hash: &str, piece_idx: i32) -> Option<Bytes> {
        let ih = info_hash.to_lowercase();
        let files = self.files.read();
        files.get(&ih)?.read_piece(piece_idx)
    }

    fn has_piece(&self, info_hash: &str, piece_idx: i32) -> bool {
        let ih = info_hash.to_lowercase();
        let files = self.files.read();
        files
            .get(&ih)
            .map_or(false, |f| f.has_piece(piece_idx))
    }

    /// Remove all data for a torrent (disk + in-memory state).
    fn remove(&self, info_hash: &str) {
        let ih = info_hash.to_lowercase();
        self.files.write().remove(&ih);
        self.piece_lengths.write().remove(&ih);
        // Remove the on-disk directory (best-effort; ignore errors).
        let _ = std::fs::remove_dir_all(self.dir.join(&ih));
    }
}

// ============================================================================
// Public configuration
// ============================================================================

/// Configuration for [`HybridPieceCache`].
#[derive(Debug, Clone)]
pub struct HybridCacheConfig {
    /// Maximum total bytes to keep in the hot (RAM) tier across all torrents.
    /// When exceeded, moka evicts least-recently-used pieces to the warm tier.
    /// Default: 512 MiB.
    pub max_hot_bytes: u64,
    /// Root directory for warm (disk) tier flat files. Created if absent.
    pub warm_dir: PathBuf,
}

impl Default for HybridCacheConfig {
    fn default() -> Self {
        Self {
            max_hot_bytes: 512 * 1024 * 1024,
            warm_dir: std::env::temp_dir().join("stremio_warm_cache"),
        }
    }
}

// ============================================================================
// HybridPieceCache
// ============================================================================

/// Two-tier hybrid piece cache.
///
/// ## Hot tier
/// [`moka::sync::Cache`] bounded by `max_hot_bytes`. LRU size-based eviction.
/// Reads are lock-free in the steady state (moka uses shard-per-key locking).
///
/// ## Warm tier
/// One flat file per torrent. Fixed-slot layout enables O(1) pread/pwrite.
/// Read via standard `pread` (no mmap needed — avoids re-mapping on file growth).
///
/// ## Eviction flow
/// Moka eviction listener → `sync_channel` → `spawn_blocking` drain task:
///   1. `write_piece_sync()` — pwrite to flat file
///   2. `memory_evict_piece_for_hash()` — free C++ RSS
///
/// The drain task is sequential per design: evictions are processed in FIFO order,
/// guaranteeing that a piece is durably written before C++ RAM is freed.
pub struct HybridPieceCache {
    hot: moka::sync::Cache<PieceKey, Bytes>,
    warm: Arc<WarmTier>,
    /// Per-piece pin counts. A non-zero count means at least one reader is
    /// currently accessing that piece's data from C++ memory storage.  The
    /// eviction drain task checks this before calling
    /// `memory_evict_piece_for_hash()` and defers the call until the count
    /// reaches zero.
    pin_counts: PinCounts,
}

impl HybridPieceCache {
    /// Create a new cache and start the background eviction drain task.
    ///
    /// Must be called from within a tokio runtime (uses `spawn_blocking` internally).
    pub fn new(config: HybridCacheConfig) -> Arc<Self> {
        let warm = Arc::new(WarmTier::new(config.warm_dir));

        // Bounded MPSC channel: 4096 slots. Each slot holds ~1 MB (Bytes refcount,
        // no copy). Under extreme eviction pressure, `try_send` failures mean the
        // piece is not persisted to disk and stays in C++ RAM — acceptable as
        // best-effort; the channel drains faster than 4096 pieces accumulate in practice.
        let (evict_tx, evict_rx) = std::sync::mpsc::sync_channel::<EvictedPiece>(4096);

        let warm_clone = warm.clone();

        // Shared pin-count map: passed to both the cache (for pin_piece()) and
        // the drain task (for checking before C++ eviction).
        let pin_counts: PinCounts = Arc::new(Mutex::new(HashMap::new()));
        let pin_counts_drain = pin_counts.clone();

        let hot = {
            let evict_tx_for_listener = evict_tx.clone();
            moka::sync::Cache::builder()
                .weigher(|_k: &PieceKey, v: &Bytes| {
                    // moka weigher must return u32; cap to avoid overflow on huge pieces.
                    v.len().min(u32::MAX as usize) as u32
                })
                .max_capacity(config.max_hot_bytes)
                // Required for invalidate_entries_if() used in remove_torrent().
                .support_invalidation_closures()
                .eviction_listener(
                    move |key: std::sync::Arc<PieceKey>, value: Bytes, _cause| {
                        // Called from moka's maintenance thread. Must not block.
                        let _ = evict_tx_for_listener.try_send(EvictedPiece {
                            info_hash: key.0.clone(),
                            piece_idx: key.1,
                            data: value, // O(1) — Bytes refcount transfer
                        });
                    },
                )
                .build()
        };

        // Drain task: sequentially writes evicted pieces to disk, then frees C++ RAM.
        // Uses spawn_blocking because pwrite is synchronous OS I/O.
        // Sequential by design: only one piece is processed at a time, so C++ RAM is
        // freed only AFTER the warm-tier write is confirmed durable (within the session).
        //
        // Pin-count protocol:
        //   - Readers that access C++ memory directly (not via hot/warm tier) increment
        //     the pin count for a piece before calling memory_read_piece_for_hash() and
        //     hold a PiecePinGuard that decrements on drop.
        //   - This task checks the pin count before calling memory_evict_piece_for_hash().
        //     If the piece is pinned, the C++ free is deferred into `deferred` and retried
        //     every 50 ms until all readers have finished.
        //   - The warm-tier write is always performed first (it reads from `evicted.data:
        //     Bytes`, an independent Rust allocation — safe regardless of pin state).
        tokio::task::spawn_blocking(move || {
            // Pieces whose warm write completed but whose C++ free was deferred
            // because a reader held a pin at eviction time.
            let mut deferred: Vec<DeferredEviction> = Vec::new();

            loop {
                // --- Retry deferred evictions ---
                if !deferred.is_empty() {
                    let mut still_pinned: Vec<DeferredEviction> = Vec::new();
                    {
                        let map = pin_counts_drain.lock();
                        for ev in deferred.drain(..) {
                            let count = map
                                .get(&(ev.info_hash.clone(), ev.piece_idx))
                                .copied()
                                .unwrap_or(0);
                            if count > 0 {
                                still_pinned.push(ev);
                            } else {
                                // Reader has finished — safe to free C++ memory now.
                                #[cfg(feature = "libtorrent")]
                                libtorrent_sys::memory_evict_piece_for_hash(
                                    &ev.info_hash,
                                    ev.piece_idx,
                                );
                            }
                        }
                    }
                    if !still_pinned.is_empty() {
                        if still_pinned.len() > 10 {
                            warn!(
                                "HybridPieceCache: {} pieces still pinned by readers; \
                                 hot-tier overshoot is acceptable — C++ eviction deferred",
                                still_pinned.len()
                            );
                        }
                        deferred = still_pinned;
                    } else {
                        deferred = Vec::new();
                    }
                }

                // --- Receive next evicted piece ---
                let evicted = if deferred.is_empty() {
                    // No deferred work: block until the next eviction.
                    match evict_rx.recv() {
                        Ok(e) => e,
                        Err(_) => {
                            debug!("HybridPieceCache: eviction drain task exiting");
                            break;
                        }
                    }
                } else {
                    // Have deferred work: use a timeout so we retry pinned pieces promptly.
                    match evict_rx.recv_timeout(std::time::Duration::from_millis(50)) {
                        Ok(e) => e,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("HybridPieceCache: eviction drain task exiting (with deferred)");
                            break;
                        }
                    }
                };

                // 1. Persist to warm tier. Returns true only if the write
                //    completed successfully. On failure the piece is NOT freed
                //    from C++ — it stays available at the cost of C++ RAM.
                let persisted = warm_clone.write_piece_sync(
                    &evicted.info_hash,
                    evicted.piece_idx,
                    &evicted.data,
                );

                if persisted {
                    // 2. Check pin count before freeing C++ in-memory bytes.
                    //    A non-zero count means poll_read is currently inside
                    //    memory_read_piece_for_hash() for this piece — defer
                    //    the free until all readers finish.
                    let is_pinned = pin_counts_drain
                        .lock()
                        .get(&(evicted.info_hash.clone(), evicted.piece_idx))
                        .copied()
                        .unwrap_or(0)
                        > 0;

                    if is_pinned {
                        debug!(
                            "HybridPieceCache: piece {}:{} pinned by reader — \
                             deferring C++ eviction",
                            evicted.info_hash, evicted.piece_idx
                        );
                        deferred.push(DeferredEviction {
                            info_hash: evicted.info_hash,
                            piece_idx: evicted.piece_idx,
                        });
                    } else {
                        // 3. Free C++ in-memory bytes — only when durably written
                        //    and no readers hold a pin.
                        //    libtorrent's bitfield is unaffected; have_piece() stays true.
                        #[cfg(feature = "libtorrent")]
                        libtorrent_sys::memory_evict_piece_for_hash(
                            &evicted.info_hash,
                            evicted.piece_idx,
                        );
                    }
                }
            }
        });

        Arc::new(Self { hot, warm, pin_counts })
    }

    // -------------------------------------------------------------------------
    // Write path
    // -------------------------------------------------------------------------

    /// Store a piece in the hot tier.
    ///
    /// Called at `piece_finished_alert` time (alert pump) and from the
    /// direct-read fallback in `poll_read`.
    ///
    /// If the hot tier is full, moka evicts an LRU piece, triggering the
    /// eviction listener which persists it to the warm tier.
    pub fn put(&self, info_hash: &str, piece_idx: i32, data: Bytes) {
        let key = (info_hash.to_lowercase(), piece_idx);
        self.hot.insert(key, data);
    }

    // -------------------------------------------------------------------------
    // Read path (sync — safe to call from poll_read)
    // -------------------------------------------------------------------------

    /// Synchronous read: hot tier → warm tier.
    ///
    /// Safe to call from `poll_read` (sync `AsyncRead` impl). Neither tier
    /// blocks the tokio thread in the steady state.
    pub fn get_sync(&self, info_hash: &str, piece_idx: i32) -> Option<Bytes> {
        let key = (info_hash.to_lowercase(), piece_idx);

        // Hot tier: moka sync get — lock-free in the common case.
        if let Some(data) = self.hot.get(&key) {
            return Some(data);
        }

        // Warm tier: pread from flat file. May page-fault on first access but
        // subsequent reads hit the OS page cache. No async I/O needed.
        self.warm.get_piece_sync(&key.0, piece_idx)
    }

    /// Whether a piece is available in either tier (no data allocation).
    pub fn has_piece_sync(&self, info_hash: &str, piece_idx: i32) -> bool {
        let key = (info_hash.to_lowercase(), piece_idx);
        self.hot.contains_key(&key) || self.warm.has_piece(&key.0, piece_idx)
    }

    // -------------------------------------------------------------------------
    // Torrent lifecycle
    // -------------------------------------------------------------------------

    /// Register a torrent's piece length so the warm tier can compute slot offsets.
    ///
    /// **Must** be called once metadata is available, before any pieces can be
    /// evicted to the warm tier. Idempotent — safe to call multiple times.
    pub fn register_torrent(&self, info_hash: &str, piece_length: u64) {
        self.warm.register(info_hash, piece_length);
    }

    /// Remove all cached data for a torrent (hot tier entries + warm tier flat file).
    ///
    /// Uses `invalidate_entries_if` to evict only this torrent's pieces from the hot
    /// tier. This requires `.support_invalidation_closures()` on the moka builder.
    ///
    /// Note: `invalidate_entries_if` is asynchronous — entries are invalidated on
    /// the next moka maintenance run, not immediately. For `remove_torrent` (a
    /// rare slow-path operation) this eventual consistency is acceptable.
    pub fn remove_torrent(&self, info_hash: &str) {
        let ih = info_hash.to_lowercase();
        // Evict only this torrent's entries from the hot tier, leaving all other
        // torrents' pieces intact.
        let _ = self.hot.invalidate_entries_if(move |k: &PieceKey, _v: &Bytes| k.0 == ih);
        self.warm.remove(info_hash);
    }

    // -------------------------------------------------------------------------
    // Pin-count API
    // -------------------------------------------------------------------------

    /// Increment the pin count for a piece and return a RAII guard that
    /// decrements it on drop.
    ///
    /// Call this **before** invoking `libtorrent_sys::memory_read_piece_for_hash()`
    /// (the direct C++ memory read path in `poll_read`). The guard prevents the
    /// eviction drain task from calling `memory_evict_piece_for_hash()` for the
    /// same piece while the C++ read is in progress.
    ///
    /// # Concurrency
    ///
    /// Pin and unpin operations are serialised by `self.pin_counts` (a
    /// `parking_lot::Mutex`). The critical section is brief (one HashMap
    /// lookup + integer increment), so contention is negligible even under
    /// concurrent streams.
    pub fn pin_piece(&self, info_hash: &str, piece_idx: i32) -> PiecePinGuard {
        let key = (info_hash.to_lowercase(), piece_idx);
        {
            let mut map = self.pin_counts.lock();
            *map.entry(key.clone()).or_insert(0) += 1;
        }
        PiecePinGuard {
            key,
            counts: self.pin_counts.clone(),
        }
    }

    /// Cache statistics: `(hot_entry_count, hot_weighted_size_bytes)`.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hot.entry_count(),
            self.hot.weighted_size(),
        )
    }
}
