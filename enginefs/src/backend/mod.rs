use anyhow::Result;
use tokio::io::{AsyncRead, AsyncSeek};

#[cfg(feature = "librqbit")]
pub mod librqbit;

#[cfg(feature = "libtorrent")]
pub mod libtorrent;

pub mod metadata;
pub mod priorities;

pub trait FileStreamTrait: AsyncRead + AsyncSeek + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send + Sync> FileStreamTrait for T {}

#[derive(Debug, Clone)]
pub enum TorrentSource {
    Url(String),
    Bytes(Vec<u8>),
}

#[async_trait::async_trait]
pub trait TorrentBackend: Send + Sync {
    type Handle: TorrentHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle>;

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle>;
    async fn remove_torrent(&self, info_hash: &str) -> Result<()>;
    /// Remove a torrent and delete all its downloaded files from disk.
    async fn remove_torrent_with_files(&self, info_hash: &str) -> Result<()>;
    async fn list_torrents(&self) -> Vec<String>;
}

#[async_trait::async_trait]
pub trait TorrentHandle: Send + Sync + Clone {
    fn info_hash(&self) -> String;
    fn name(&self) -> Option<String>;

    async fn stats(&self) -> EngineStats;
    async fn add_trackers(&self, trackers: Vec<String>) -> Result<()>;
    async fn get_file_reader(
        &self,
        file_idx: usize,
        start_offset: u64,
        priority: u8,
        bitrate: Option<u64>,
    ) -> Result<Box<dyn FileStreamTrait>>;
    async fn get_files(&self) -> Vec<BackendFileInfo>;
    /// Get the local filesystem path for a file (for probing without HTTP loopback)
    async fn get_file_path(&self, file_idx: usize) -> Option<String>;
    /// Prepare a file for streaming by setting its priority and waiting for initial pieces.
    /// This should be called BEFORE probing the file with ffprobe.
    /// Returns Ok(()) when initial pieces are available, or Err on timeout.
    async fn prepare_file_for_streaming(&self, file_idx: usize) -> Result<()>;
    /// Clear streaming state for a file (set priority to 0, clear piece deadlines).
    /// Called when switching to a different file to ensure exclusive downloading.
    async fn clear_file_streaming(&self, file_idx: usize) -> Result<()>;
    /// Pause all piece downloading for this torrent without disconnecting peers.
    ///
    /// Called by the grace-period background task after `stream_inactivity_pause_secs`
    /// of inactivity (default 30 s) to stop consuming bandwidth while keeping peer
    /// connections alive for a fast reconnect.  Sets all file priorities to 0 and
    /// clears all piece deadlines.  Peer connections are preserved so that a
    /// reconnecting player can resume at full speed without waiting for peer
    /// re-discovery.
    ///
    /// This is **not** the same as `handle.pause()` (which disconnects peers).
    async fn pause_downloads(&self) -> Result<()>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackendFileInfo {
    pub name: String,
    pub length: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStat {
    pub ip: String,
    pub down_speed: f64,
    pub up_speed: f64,
    pub rank_score: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubtitleTrack {
    pub id: usize,
    pub name: String,
    pub size: u64,
}

// Stremio-compatible stats structures
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsFile {
    pub name: String,
    pub path: String,
    pub length: u64,
    pub offset: u64,
    pub downloaded: u64,
    /// Progress 0.0 to 1.0 (from C++ file_progress)
    pub progress: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Growler {
    pub flood: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pulse: Option<u64>,
}

impl Default for Growler {
    fn default() -> Self {
        Self {
            flood: 0,
            pulse: None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSearch {
    pub max: u64,
    pub min: u64,
    pub sources: Vec<String>,
}

impl Default for PeerSearch {
    fn default() -> Self {
        Self {
            max: 200,
            min: 40,
            sources: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwarmCap {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_peers: Option<u64>,
}

impl Default for SwarmCap {
    fn default() -> Self {
        Self {
            max_speed: None,
            min_peers: None,
        }
    }
}

/// Torrent speed profile settings from frontend (stremio-web)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSpeedProfile {
    /// Hard limit on download speed (bytes/sec)
    pub bt_download_speed_hard_limit: f64,
    /// Soft limit on download speed (bytes/sec)
    pub bt_download_speed_soft_limit: f64,
    /// Handshake timeout (ms)
    pub bt_handshake_timeout: u64,
    /// Maximum connections
    pub bt_max_connections: u64,
    /// Minimum peers for stable
    pub bt_min_peers_for_stable: u64,
    /// Request timeout (ms)
    pub bt_request_timeout: u64,
}

impl Default for TorrentSpeedProfile {
    fn default() -> Self {
        // "MAXIMUM PERFORMANCE" - Remove arbitrary limits
        Self {
            bt_download_speed_hard_limit: 0.0, // 0 = unlimited
            bt_download_speed_soft_limit: 0.0, // 0 = unlimited
            bt_handshake_timeout: 20000,       // 20s - faster failure for dead peers
            // 65535 is a safe high number for "unlimited" without overflowing i32 when cast or causing OS issues.
            bt_max_connections: 65535,
            bt_min_peers_for_stable: 5, // Lower barrier to entry
            bt_request_timeout: 10000,  // 10s
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackendConfig {
    pub cache: priorities::EngineCacheConfig,
    pub growler: Growler,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    pub speed_profile: TorrentSpeedProfile,
    /// Seconds of inactivity (no active streams) before downloading is paused
    /// for a torrent.  Piece deadlines are cleared and all file priorities are
    /// set to 0 to stop consuming bandwidth.  Peer connections are **not**
    /// disconnected so that a reconnecting player gets full speed immediately.
    /// Default: 30 seconds.
    #[serde(default = "BackendConfig::default_pause_secs")]
    pub stream_inactivity_pause_secs: u64,
    /// Seconds of inactivity before the torrent is fully removed from the
    /// libtorrent session (peers disconnected, C++ state freed).  The hybrid
    /// piece cache warm tier is preserved on disk so subsequent re-opens
    /// can resume from it.  Default: 60 seconds.
    #[serde(default = "BackendConfig::default_remove_secs")]
    pub stream_inactivity_remove_secs: u64,
}

impl BackendConfig {
    pub fn default_pause_secs() -> u64 { 30 }
    pub fn default_remove_secs() -> u64 { 60 }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            cache: priorities::EngineCacheConfig::default(),
            growler: Growler::default(),
            peer_search: PeerSearch::default(),
            swarm_cap: SwarmCap::default(),
            speed_profile: TorrentSpeedProfile::default(),
            stream_inactivity_pause_secs: Self::default_pause_secs(),
            stream_inactivity_remove_secs: Self::default_remove_secs(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connections: Option<u64>,
    pub dht: bool,
    pub growler: Growler,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handshake_timeout: Option<u64>,
    pub path: String,
    pub peer_search: PeerSearch,
    pub swarm_cap: SwarmCap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    pub tracker: bool,
    pub r#virtual: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub last_started: String,
    pub num_found: u64,
    pub num_found_uniq: u64,
    pub num_requests: u64,
    pub url: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStats {
    pub name: String,
    pub info_hash: String,
    pub files: Vec<StatsFile>,
    pub sources: Vec<Source>,
    pub opts: StatsOptions,
    pub download_speed: f64,
    pub upload_speed: f64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub unchoked: u64,
    pub peers: u64,
    pub queued: u64,
    pub unique: u64,
    pub connection_tries: u64,
    pub peer_search_running: bool,
    pub stream_len: u64,
    pub stream_name: String,
    pub stream_progress: f64,
    pub swarm_connections: u64,
    pub swarm_paused: bool,
    pub swarm_size: u64,
}
