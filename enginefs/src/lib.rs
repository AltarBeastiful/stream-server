use crate::engine::Engine;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::debug;

pub mod backend;
pub mod cache;
pub mod disk_cache;
pub mod engine;
pub mod files;
pub mod hls;
pub mod hybrid_cache;
pub mod hwaccel;
pub mod metadata_cache;
pub mod piece_cache;
pub mod piece_waiter;
pub mod subtitles;
pub mod tracker_prober;
pub mod trackers;

// Re-export TrackerStorage for use by server crate
pub use trackers::TrackerStorage;

#[cfg(all(feature = "librqbit", not(feature = "libtorrent")))]
use crate::backend::librqbit::LibrqbitBackend;
#[cfg(feature = "libtorrent")]
use crate::backend::libtorrent::LibtorrentBackend;

use crate::backend::{TorrentBackend, TorrentHandle, TorrentSource};

/// Fallback timeout for engines that never had any active streams.
/// These are torrents that were added (e.g. for metadata inspection) but
/// whose player never actually connected to `/stream`.  They are removed
/// after 5 minutes of `last_accessed` age, consistent with the original
/// behaviour.
const IDLE_ENGINE_TIMEOUT_SECS: u64 = 300;

static START_TIME: OnceLock<Instant> = OnceLock::new();

pub(crate) fn elapsed_secs() -> i64 {
    START_TIME.get_or_init(Instant::now).elapsed().as_secs() as i64
}

const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://9.rarbg.com:2810/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://ipv4.tracker.harry.lu:80/announce",
];

pub struct BackendEngineFS<B: TorrentBackend> {
    pub backend: Arc<B>,
    engines: Arc<RwLock<HashMap<String, Arc<Engine<B::Handle>>>>>,
    tracker_manager: Arc<crate::trackers::TrackerManager>,
    pub cache_dir: std::path::PathBuf,
    pub download_dir: std::path::PathBuf,
    /// Track active streams per info_hash for legacy compatibility
    active_streams: Arc<RwLock<HashMap<String, usize>>>,
    /// Track active requests per specific streamed file so cleanup does not race probe retries.
    active_file_streams: Arc<RwLock<HashMap<(String, usize), usize>>>,
    /// Tracks the currently active streamed file (info_hash, file_idx)
    /// Only ONE file should be actively downloading at any time
    active_file: Arc<RwLock<Option<(String, usize)>>>,
    /// Optional disk cache for persisting completed files
    disk_cache: Option<Arc<disk_cache::DiskCacheManager>>,
    /// Seconds of stream inactivity before piece downloading is paused (peers kept).
    /// Readable/writable after construction so `new_with_storage` can apply the
    /// value from `BackendConfig` before the background task first fires.
    pub grace_pause_secs: Arc<std::sync::atomic::AtomicU64>,
    /// Seconds of stream inactivity before the torrent is fully removed from the session.
    pub grace_remove_secs: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(all(feature = "librqbit", not(feature = "libtorrent")))]
pub type EngineFS = BackendEngineFS<LibrqbitBackend>;

#[cfg(feature = "libtorrent")]
pub type EngineFS = BackendEngineFS<LibtorrentBackend>;

impl<B: TorrentBackend + 'static> BackendEngineFS<B> {
    pub fn new_with_backend(
        backend: B,
        restored_handles: HashMap<String, B::Handle>,
        cache_dir: std::path::PathBuf,
        download_dir: std::path::PathBuf,
    ) -> Self {
        Self::new_with_backend_and_storage(backend, restored_handles, cache_dir, download_dir, None)
    }

    pub fn new_with_backend_and_storage(
        backend: B,
        restored_handles: HashMap<String, B::Handle>,
        cache_dir: std::path::PathBuf,
        download_dir: std::path::PathBuf,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Self {
        let mut engines_map = HashMap::new();
        for (hash, handle) in restored_handles {
            engines_map.insert(
                hash.clone(),
                Arc::new(Engine::new_with_handle(handle, &hash)),
            );
        }

        let engines = Arc::new(RwLock::new(engines_map));

        // Create tracker manager with or without storage
        let tracker_manager = match tracker_storage {
            Some(storage) => Arc::new(crate::trackers::TrackerManager::new_with_storage(storage)),
            None => Arc::new(crate::trackers::TrackerManager::new()),
        };

        let grace_pause_secs = Arc::new(std::sync::atomic::AtomicU64::new(
            crate::backend::BackendConfig::default_pause_secs(),
        ));
        let grace_remove_secs = Arc::new(std::sync::atomic::AtomicU64::new(
            crate::backend::BackendConfig::default_remove_secs(),
        ));

        let efs = Self {
            backend: Arc::new(backend),
            engines: engines.clone(),
            tracker_manager,
            cache_dir,
            download_dir: download_dir.clone(),
            active_streams: Arc::new(RwLock::new(HashMap::new())),
            active_file_streams: Arc::new(RwLock::new(HashMap::new())),
            active_file: Arc::new(RwLock::new(None)),
            disk_cache: None,
            grace_pause_secs: grace_pause_secs.clone(),
            grace_remove_secs: grace_remove_secs.clone(),
        };

        let engines_clone = engines.clone();
        let backend_clone = efs.backend.clone();
        // Grace-period teardown task — polls every 5 s.
        //
        // State machine per engine (driven by `engine.inactive_since`):
        //   inactive_since == 0   → engine was never streamed; use `last_accessed`
        //                           age against IDLE_ENGINE_TIMEOUT_SECS for removal
        //   inactive_since == -1  → at least one stream is currently active; skip
        //   inactive_since > 0    → timestamp (elapsed_secs()) when the last stream
        //                           closed; drive the pause/remove thresholds
        //
        // 0 – pause_secs  : do nothing (player reconnect window)
        // pause_secs      : call pause_downloads() to stop bandwidth, keep peers
        // remove_secs     : remove from libtorrent session (peers disconnected)
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            // Set of info_hashes for which pause_downloads() has already been called
            // this inactivity window.  Cleared when a new stream starts.
            let mut download_paused: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            loop {
                interval.tick().await;
                let pause_secs = grace_pause_secs.load(Ordering::Relaxed);
                let remove_secs = grace_remove_secs.load(Ordering::Relaxed);
                let now = elapsed_secs();

                // Collect actions under a short-lived read lock.
                let mut to_pause: Vec<(String, Arc<Engine<B::Handle>>)> = Vec::new();
                let mut to_remove: Vec<String> = Vec::new();

                {
                    let read = engines_clone.read().await;
                    for (hash, engine) in read.iter() {
                        let active = engine.active_streams.load(Ordering::SeqCst);
                        let inactive_since = engine.inactive_since.load(Ordering::SeqCst);

                        // A newly-reconnected stream clears the paused flag so that
                        // the next inactivity window will call pause_downloads() again.
                        if active > 0 {
                            download_paused.remove(hash);
                            continue;
                        }

                        // Compute seconds since the last stream ended.
                        let secs_inactive: u64 = match inactive_since {
                            // Engine was never streamed — use last_accessed age.
                            // These torrents are removed at the original 300-second
                            // timeout rather than the grace-period remove threshold.
                            0 => {
                                let last =
                                    engine.last_accessed.load(Ordering::SeqCst);
                                let secs = (now - last).max(0) as u64;
                                if secs >= IDLE_ENGINE_TIMEOUT_SECS {
                                    to_remove.push(hash.clone());
                                    download_paused.remove(hash);
                                }
                                continue;
                            }
                            // A stream is active right now (race: active was 0 but
                            // inactive_since is -1 due to load ordering).  Skip.
                            i if i < 0 => continue,
                            // Normal case: timestamp of last stream close.
                            ts => (now - ts).max(0) as u64,
                        };

                        if secs_inactive >= remove_secs {
                            // Full removal regardless of whether we already paused.
                            to_remove.push(hash.clone());
                            download_paused.remove(hash);
                        } else if inactive_since != 0
                            && secs_inactive >= pause_secs
                            && !download_paused.contains(hash)
                        {
                            // Only pause engines that have actually had streams
                            // (inactive_since != 0).  Never-streamed engines are just
                            // waiting to be removed — no piece deadlines to clear.
                            to_pause.push((hash.clone(), engine.clone()));
                        }
                    }
                }

                // Call pause_downloads() outside the read lock (it's async).
                for (hash, engine) in to_pause {
                    match engine.handle.pause_downloads().await {
                        Ok(()) => {
                            tracing::info!(
                                info_hash = %hash,
                                "grace-period: paused downloading \
                                 (peers kept, waiting for reconnect)"
                            );
                            download_paused.insert(hash);
                        }
                        Err(e) => {
                            tracing::warn!(
                                info_hash = %hash,
                                "grace-period: pause_downloads failed: {}",
                                e
                            );
                        }
                    }
                }

                if !to_remove.is_empty() {
                    let mut write = engines_clone.write().await;
                    for hash in &to_remove {
                        debug!(info_hash = %hash, "grace-period: removing inactive engine");
                        write.remove(hash);
                    }
                    drop(write);

                    for hash in to_remove {
                        if let Err(e) = backend_clone.remove_torrent(&hash).await {
                            tracing::warn!(
                                info_hash = %hash,
                                "grace-period: failed to remove torrent from backend: {}",
                                e
                            );
                        } else {
                            tracing::info!(
                                info_hash = %hash,
                                "grace-period: removed torrent from backend session"
                            );
                        }
                    }
                }
            }
        });

        efs
    }

    pub async fn add_torrent(
        &self,
        source: TorrentSource,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>> {
        // Start with default trackers
        let mut trackers: Vec<String> = DEFAULT_TRACKERS.iter().map(|s| s.to_string()).collect();

        // Add cached trackers from tracker manager (already ranked by RTT)
        let cached_trackers = self.tracker_manager.get_trackers().await;
        trackers.extend(cached_trackers);

        // Add any extra trackers provided
        if let Some(extra) = extra_trackers {
            trackers.extend(extra);
        }
        trackers.sort();
        trackers.dedup();

        debug!(count = trackers.len(), "Adding torrent with trackers");

        let handle = self.backend.add_torrent(source, trackers).await?;
        let info_hash = handle.info_hash();

        let mut engines = self.engines.write().await;
        if let Some(engine) = engines.get(&info_hash) {
            engine
                .last_accessed
                .store(elapsed_secs(), std::sync::atomic::Ordering::SeqCst);
            return Ok(engine.clone());
        }

        let engine = Arc::new(Engine::new_with_handle(handle, &info_hash));
        engines.insert(info_hash.clone(), engine.clone());
        Ok(engine)
    }

    pub async fn get_engine(&self, info_hash: &str) -> Option<Arc<Engine<B::Handle>>> {
        let engines = self.engines.read().await;
        engines.get(&info_hash.to_lowercase()).cloned()
    }

    pub async fn get_or_add_engine(&self, info_hash: &str) -> Result<Arc<Engine<B::Handle>>> {
        if let Some(engine) = self.get_engine(info_hash).await {
            return Ok(engine);
        }
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);
        self.add_torrent(TorrentSource::Url(magnet), None).await
    }

    pub async fn remove_engine(&self, info_hash: &str) {
        let mut engines = self.engines.write().await;
        engines.remove(&info_hash.to_lowercase());
    }

    /// Remove a torrent from the session **and** delete its downloaded files from disk.
    ///
    /// Called by the preload `DELETE ?delete=true` route.  Removes the engine from the
    /// in-memory map, then instructs the backend to delete the on-disk data.  Errors
    /// from the backend are surfaced to the caller so they can be returned as HTTP 500.
    pub async fn remove_engine_and_files(&self, info_hash: &str) -> Result<()> {
        let info_hash = info_hash.to_lowercase();
        {
            let mut engines = self.engines.write().await;
            engines.remove(&info_hash);
        }
        // Best-effort: if the torrent isn't known to the backend (e.g. already gone),
        // we swallow the error and treat the delete as successful.
        match self.backend.remove_torrent_with_files(&info_hash).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    info_hash = %info_hash,
                    "remove_engine_and_files: backend returned error (torrent may already be gone): {}",
                    e
                );
                // Return OK — if the torrent doesn't exist in the backend, the files are
                // already gone; there is nothing left to do.
                Ok(())
            }
        }
    }

    pub async fn get_all_statistics(&self) -> HashMap<String, crate::backend::EngineStats> {
        let engines = self.engines.read().await;
        let mut stats = HashMap::new();
        for (hash, engine) in engines.iter() {
            stats.insert(hash.clone(), engine.get_statistics().await);
        }
        stats
    }

    pub async fn list_engines(&self) -> Vec<String> {
        let engines = self.engines.read().await;
        engines.keys().cloned().collect()
    }

    /// Called when a stream starts for a torrent file.
    /// Implements exclusive single-file downloading - only ONE file downloads at a time.
    ///
    /// NOTE: We deliberately do NOT cancel previous streams for the same
    /// `(info_hash, file_idx)`. Players (e.g. Stremio) routinely open two
    /// concurrent connections to the same file — one for the head bytes
    /// (offset=0) and one for container metadata at the end (offset≈eof for
    /// MKV Cues / MP4 moov). Cancelling the first when the second arrives
    /// makes the player wait for the metadata read to finish before initial
    /// playback can start, which made initial playback take 27+ seconds.
    pub async fn on_stream_start(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();

        // Check if a different file was previously active and clear its priorities
        {
            let mut active = self.active_file.write().await;
            if let Some((prev_hash, prev_idx)) = active.take() {
                if prev_hash != info_hash || prev_idx != file_idx {
                    tracing::info!(
                        "Switching stream: clearing priorities for previous file {} idx={}",
                        prev_hash,
                        prev_idx
                    );
                    // Clear priorities for the previous file
                    self.clear_file_priorities(&prev_hash, prev_idx).await;
                }
            }
            // Set the new active file
            *active = Some((info_hash.clone(), file_idx));
        }

        // Also update legacy active_streams counter
        {
            let mut streams = self.active_streams.write().await;
            let count = streams.entry(info_hash.clone()).or_insert(0);
            *count += 1;
        }
        {
            let mut streams = self.active_file_streams.write().await;
            let count = streams.entry((info_hash.clone(), file_idx)).or_insert(0);
            *count += 1;
        }

        tracing::info!(
            "Stream started for {} file_idx={} (exclusive mode)",
            info_hash,
            file_idx
        );
    }

    /// Called when a stream ends for a torrent file
    pub async fn on_stream_end(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();

        // Update legacy active_streams counter
        {
            let mut streams = self.active_streams.write().await;
            if let Some(count) = streams.get_mut(&info_hash) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&info_hash);
                }
            }
        }

        let file_streams_remaining = {
            let mut streams = self.active_file_streams.write().await;
            let key = (info_hash.clone(), file_idx);
            if let Some(count) = streams.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&key);
                    0
                } else {
                    *count
                }
            } else {
                0
            }
        };

        if file_streams_remaining == 0 {
            self.schedule_file_cleanup(info_hash.clone(), file_idx);
        }

        let remaining = self.active_streams.read().await.values().sum::<usize>();
        tracing::info!(
            "Stream ended for {} file_idx={}, total active streams: {}, file streams remaining: {}",
            info_hash,
            file_idx,
            remaining,
            file_streams_remaining
        );
    }

    fn schedule_file_cleanup(&self, info_hash: String, file_idx: usize) {
        let active_file = self.active_file.clone();
        let active_file_streams = self.active_file_streams.clone();

        // Wait a short grace window before clearing the active_file state.  If the
        // player reconnects within this window (typical seek / stall recovery), the
        // guard in `on_stream_start` will cancel this task by finding `still_active`.
        //
        // NOTE: Piece priority cleanup (formerly done here) is now handled by the
        // 30-second grace-period task in `new_with_backend_and_storage`, which calls
        // `pause_downloads()`.  This avoids punishing players that reconnect between
        // 5 s and 30 s (common for seeks in mid-torrent files).
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let key = (info_hash.clone(), file_idx);
            let still_active = {
                let streams = active_file_streams.read().await;
                streams.get(&key).copied().unwrap_or(0) > 0
            };
            if still_active {
                tracing::debug!(
                    "Skipping delayed cleanup for {} idx={} because a new stream started",
                    info_hash,
                    file_idx
                );
                return;
            }

            // Clear the active_file slot so that the next on_stream_start call can
            // set it (and call clear_file_priorities for file switching if needed).
            {
                let mut active = active_file.write().await;
                if let Some((ref h, idx)) = *active {
                    if h == &info_hash && idx == file_idx {
                        tracing::info!(
                            "Delayed cleanup: clearing active file for {} file_idx={} \
                             (piece priorities deferred to 30-second grace period)",
                            info_hash,
                            file_idx
                        );
                        *active = None;
                    }
                }
            }
        });
    }

    /// Clear piece priorities for a file that is no longer being streamed
    async fn clear_file_priorities(&self, info_hash: &str, file_idx: usize) {
        if let Some(engine) = self.get_engine(info_hash).await {
            // Set file priority to 0 (skip) and clear piece deadlines
            if let Err(e) = engine.handle.clear_file_streaming(file_idx).await {
                tracing::warn!(
                    "Failed to clear file priorities for {} idx={}: {}",
                    info_hash,
                    file_idx,
                    e
                );
            }
        }
    }

    /// Get a reference to the backend for direct access
    pub fn get_backend(&self) -> &Arc<B> {
        &self.backend
    }
}

#[cfg(all(feature = "librqbit", not(feature = "libtorrent")))]
impl BackendEngineFS<LibrqbitBackend> {
    pub async fn new(
        root_dir: std::path::PathBuf,
        _cache_config: EngineCacheConfig,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let (backend, restored) = LibrqbitBackend::new(download_dir.clone()).await?;
        Ok(Self::new_with_backend(
            backend,
            restored,
            root_dir.join("cache"),
            download_dir,
        ))
    }
}

#[cfg(feature = "libtorrent")]
impl BackendEngineFS<LibtorrentBackend> {
    pub async fn new(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
    ) -> Result<Self> {
        Self::new_with_storage(root_dir, config, None).await
    }

    pub async fn new_with_storage(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Result<Self> {
        let download_dir = root_dir.join("libtorrent-downloads");
        let cache_size = config.cache.size;
        // Extract grace-period values before config is consumed by the backend.
        let pause_secs = config.stream_inactivity_pause_secs;
        let remove_secs = config.stream_inactivity_remove_secs;
        let backend = LibtorrentBackend::new(download_dir.clone(), config)?;

        let mut efs = Self::new_with_backend_and_storage(
            backend,
            HashMap::new(),
            download_dir.clone(),
            download_dir,
            tracker_storage,
        );

        // Apply grace-period config from BackendConfig.  The background task
        // initialises these to their defaults (30 s / 60 s); overwrite here with
        // the user-supplied values so subsequent ticks use the correct thresholds.
        efs.grace_pause_secs.store(pause_secs, Ordering::Relaxed);
        efs.grace_remove_secs.store(remove_secs, Ordering::Relaxed);

        // Set up disk cache for conditional file persistence
        let disk_cache_dir = root_dir.join("disk-cache");
        efs.disk_cache = Some(Arc::new(disk_cache::DiskCacheManager::new(
            disk_cache_dir,
            cache_size,
        )));

        Ok(efs)
    }

    /// Update session settings dynamically (called when user changes torrent profile)
    pub async fn update_speed_profile(&self, profile: &crate::backend::TorrentSpeedProfile) {
        self.backend.update_session_settings(profile).await;
    }

    /// Pause all torrents except the specified one to focus bandwidth on streaming
    /// Also enables streaming mode (limits upload to maximize download)
    pub async fn focus_torrent(&self, target_info_hash: &str) {
        // Enable streaming mode (limit uploads)
        self.backend.set_streaming_mode(true).await;
        self.backend.focus_torrent(target_info_hash).await;
    }

    /// Resume all paused torrents (called when streaming ends)
    /// Also disables streaming mode (restores normal upload)
    pub async fn resume_all_torrents(&self) {
        self.backend.resume_all_torrents().await;
    }

    /// Pause all torrents (called when no active streams remain)
    pub async fn pause_all_torrents(&self) {
        self.backend.pause_all_torrents().await;
    }

    /// Enable or disable streaming mode (limits uploads during streaming)
    pub async fn set_streaming_mode(&self, enabled: bool) {
        self.backend.set_streaming_mode(enabled).await;
    }
}
