use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use enginefs::backend::TorrentHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared types (also stored in AppState)
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "status", content = "details", rename_all = "camelCase")]
pub enum PreloadProgress {
    Pending,
    Downloading { progress: f64, speed: f64 },
    Ready,
    Failed { reason: String },
}

/// Query parameters accepted by `DELETE /{infoHash}/{fileIdx}/preload`.
#[derive(Deserialize, Default)]
pub struct CancelQuery {
    /// When `true`, also delete all downloaded data from disk.
    #[serde(default)]
    delete: bool,
}

/// Live state for a single preload operation.
/// The DashMap value is not `Clone` (AbortHandle is Clone, JoinHandle is not),
/// so we wrap the mutable progress in Arc<Mutex> and store only the abort handle.
pub struct PreloadTask {
    pub file_idx: usize,
    pub progress: Arc<std::sync::Mutex<PreloadProgress>>,
    pub abort_handle: tokio::task::AbortHandle,
}

// ---------------------------------------------------------------------------
// POST /{infoHash}/{fileIdx}/preload  — start background full-file download
// ---------------------------------------------------------------------------

pub async fn start_preload(
    Path((info_hash, file_idx)): Path<(String, usize)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let info_hash = info_hash.to_lowercase();

    // If already running, return the current status instead of re-starting.
    if let Some(task) = state.preload_sessions.get(&info_hash) {
        let status = task.progress.lock().unwrap().clone();
        return Json(json!({ "status": "already_running", "current": status })).into_response();
    }

    // Add/get torrent engine — creates from magnet if not yet known.
    let engine = match state.engine.get_or_add_engine(&info_hash).await {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to add torrent: {}", e),
            )
                .into_response()
        }
    };

    let handle = engine.handle.clone();
    let hash_for_task = info_hash.clone();

    let progress = Arc::new(std::sync::Mutex::new(PreloadProgress::Pending));
    let progress_clone = progress.clone();

    let task = tokio::spawn(async move {
        tracing::info!(
            info_hash = %hash_for_task,
            file_idx,
            "Preload: starting full-file download"
        );

        // prepare_file_for_streaming waits for metadata and sets the initial piece
        // priorities (safety-net deadlines on the first 2 pieces).  After it returns,
        // the background metadata inspector is running inside a spawned task.
        if let Err(e) = handle.prepare_file_for_streaming(file_idx).await {
            tracing::warn!(info_hash = %hash_for_task, "Preload: prepare failed: {}", e);
            *progress_clone.lock().unwrap() = PreloadProgress::Failed {
                reason: e.to_string(),
            };
            return;
        }

        // Obtain the file size for accurate progress reporting.
        let files = handle.get_files().await;
        let file_size = match files.get(file_idx).map(|f| f.length) {
            Some(sz) if sz > 0 => sz,
            _ => {
                tracing::warn!(info_hash = %hash_for_task, file_idx, "Preload: file size unknown");
                *progress_clone.lock().unwrap() = PreloadProgress::Failed {
                    reason: "File size unknown".to_string(),
                };
                return;
            }
        };

        // Open a sequential reader at offset 0.
        //
        // Why this matters: `prepare_file_for_streaming` only sets safety-net
        // deadlines on the first MAX_STARTUP_PIECES (currently 2) pieces, leaving
        // pieces 2…N with no deadline.  The background metadata inspector then sets
        // an *urgent* 150 ms deadline on the Cues piece (near EOF), which libtorrent
        // prioritises over the undeadlined middle pieces.  The result: after the
        // initial 2 pieces download, the rest of the file stalls.
        //
        // Opening the reader here keeps a rolling window of URGENT piece deadlines
        // alive (via `set_priorities` / `calculate_priorities` inside `poll_read`)
        // that advances sequentially through the whole file.  Every downloaded piece
        // is stored by the alert pump in the hybrid cache, so when the user plays the
        // file all data is served from cache with zero additional download latency.
        let mut reader = match handle.get_file_reader(file_idx, 0, 0, None).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(info_hash = %hash_for_task, "Preload: failed to open reader: {}", e);
                *progress_clone.lock().unwrap() = PreloadProgress::Failed {
                    reason: format!("Reader open failed: {}", e),
                };
                return;
            }
        };

        // Drain the file sequentially.  Data is discarded — the alert pump has
        // already placed each finished piece in the hybrid cache before `poll_read`
        // returns it.  We only keep it alive here long enough to trigger the next
        // round of piece-deadline updates.
        use tokio::io::AsyncReadExt;
        // 1 MB buffer matches a typical piece size; larger buffers don't help because
        // the reader always serves exactly one piece at a time before returning.
        let mut buf = vec![0u8; 1024 * 1024];
        let mut bytes_read: u64 = 0;
        let mut last_progress_at = tokio::time::Instant::now();
        let mut bytes_since_update: u64 = 0;
        let mut current_speed: f64 = 0.0;
        let mut last_reported = -1.0_f64;

        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    // EOF — the whole file is now in the hybrid cache.
                    tracing::info!(info_hash = %hash_for_task, file_idx, "Preload: complete");
                    *progress_clone.lock().unwrap() = PreloadProgress::Ready;
                    break;
                }
                Ok(n) => {
                    bytes_read += n as u64;
                    bytes_since_update += n as u64;

                    // Refresh speed estimate once per second.
                    let now = tokio::time::Instant::now();
                    let elapsed = now.duration_since(last_progress_at).as_secs_f64();
                    if elapsed >= 1.0 {
                        current_speed = bytes_since_update as f64 / elapsed;
                        bytes_since_update = 0;
                        last_progress_at = now;
                    }

                    let p = (bytes_read as f64 / file_size as f64).min(1.0);
                    // Update the shared progress cell only when something meaningful changed.
                    if (p - last_reported).abs() >= 0.005 {
                        last_reported = p;
                        tracing::debug!(
                            info_hash = %hash_for_task,
                            file_idx,
                            progress = %format!("{:.1}%", p * 100.0),
                            speed = %format!("{:.1} MB/s", current_speed / 1_000_000.0),
                            "Preload progress"
                        );
                        *progress_clone.lock().unwrap() = PreloadProgress::Downloading {
                            progress: p,
                            speed: current_speed,
                        };
                    }
                }
                Err(e) => {
                    tracing::warn!(info_hash = %hash_for_task, "Preload: read error: {}", e);
                    *progress_clone.lock().unwrap() = PreloadProgress::Failed {
                        reason: format!("Read error: {}", e),
                    };
                    break;
                }
            }
        }
    });

    let abort_handle = task.abort_handle();

    // Insert before driving so the progress slot is visible immediately.
    state.preload_sessions.insert(
        info_hash,
        PreloadTask {
            file_idx,
            progress,
            abort_handle,
        },
    );

    // Drive the task; abort_handle in the map is the only external ref.
    tokio::spawn(task);

    Json(json!({ "status": "pending" })).into_response()
}

// ---------------------------------------------------------------------------
// GET /{infoHash}/{fileIdx}/preload  — query progress
// ---------------------------------------------------------------------------

pub async fn preload_progress(
    Path((info_hash, _file_idx)): Path<(String, usize)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let info_hash = info_hash.to_lowercase();

    match state.preload_sessions.get(&info_hash) {
        Some(task) => {
            let status = task.progress.lock().unwrap().clone();
            // Compute flat progress and speed values for convenience
            let progress_value = match &status {
                PreloadProgress::Pending => 0.0,
                PreloadProgress::Downloading { progress, .. } => *progress,
                PreloadProgress::Ready => 1.0,
                PreloadProgress::Failed { .. } => 0.0,
            };
            let speed_bps = match &status {
                PreloadProgress::Downloading { speed, .. } => *speed,
                _ => 0.0,
            };
            Json(json!({
                "infoHash": info_hash,
                "fileIdx":  task.file_idx,
                "progress": progress_value,
                "speedBps": speed_bps,
                "state":    status,
            }))
            .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------------------------------------------------------------------------
// DELETE /{infoHash}/{fileIdx}/preload  — cancel (files kept) or hard-delete
//
// Optional query param: ?delete=true
//   absent / false  — abort the download task but keep pieces on disk so that
//                     subsequent playback or a re-triggered preload can reuse them.
//   true            — abort the task AND call the backend to remove the torrent
//                     and delete all downloaded data from disk.
// ---------------------------------------------------------------------------

pub async fn cancel_preload(
    Path((info_hash, _file_idx)): Path<(String, usize)>,
    Query(params): Query<CancelQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let info_hash = info_hash.to_lowercase();
    let hard_delete = params.delete;

    if let Some((_, task)) = state.preload_sessions.remove(&info_hash) {
        task.abort_handle.abort();
        tracing::info!(info_hash = %info_hash, hard_delete, "Preload: cancelled");
    }

    if hard_delete {
        if let Err(e) = state.engine.remove_engine_and_files(&info_hash).await {
            tracing::warn!(info_hash = %info_hash, "Preload delete: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        tracing::info!(info_hash = %info_hash, "Preload: files deleted from disk");
    }
    // NOTE: when hard_delete=false we deliberately do NOT call engine.remove_engine()
    // here — pieces already downloaded (in cache or on disk) are kept so that
    // subsequent playback or a re-triggered preload can use them.

    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// GET /preload/disk-space  — report available bytes in the download directory
// ---------------------------------------------------------------------------

pub async fn disk_space(State(state): State<AppState>) -> impl IntoResponse {
    let settings = state.settings.read().await;
    let download_dir = settings.cache_root.clone();
    drop(settings);

    match fs2::available_space(&download_dir) {
        Ok(bytes) => Json(json!({ "available": bytes, "path": download_dir })).into_response(),
        Err(e) => {
            tracing::warn!("disk_space: failed to query {}: {}", download_dir, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Could not determine disk space: {}", e),
            )
                .into_response()
        }
    }
}
