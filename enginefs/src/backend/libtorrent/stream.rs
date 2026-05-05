//! File stream implementation for libtorrent backend

use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

use crate::backend::priorities::{EngineCacheConfig, calculate_priorities, container_metadata_start};

/// Type of seek operation - determines priority behavior
/// Used for DETERMINISTIC seek detection instead of heuristic piece jumps
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SeekType {
    /// Normal sequential reading (no seek)
    Sequential,
    /// Initial playback (offset=0 on first request)
    InitialPlayback,
    /// User scrubbing to a new position
    UserScrub,
    /// Container metadata read (moov, Cues at end of file)
    ContainerMetadata,
}

pub(crate) struct LibtorrentFileStream {
    pub(crate) handle: libtorrent_sys::LibtorrentHandle,
    pub(crate) first_piece: i32,
    pub(crate) last_piece: i32,
    pub(crate) piece_length: u64,
    pub(crate) file_offset: u64,
    pub(crate) current_pos: u64,
    pub(crate) is_complete: bool,
    pub(crate) last_priorities_piece: i32,
    pub(crate) cache_config: EngineCacheConfig,
    pub(crate) priority: u8,
    pub(crate) bitrate: Option<u64>,
    pub(crate) download_speed_ema: f64,
    pub(crate) stream_id: usize,
    /// Hybrid piece cache: hot RAM tier + warm disk flat-file tier.
    pub(crate) piece_cache: Arc<crate::hybrid_cache::HybridPieceCache>,
    /// Info hash for cache lookups
    pub(crate) info_hash: String,
    /// Currently cached piece data for fast serving (L0 per-stream cache).
    /// Avoids even a hot-tier lookup for sequential reads within the same piece.
    /// Tuple: (piece_idx, data, file_relative_start)
    pub(crate) cached_piece_data: Option<(i32, Bytes, u64)>,
    /// Last piece we triggered prefetch for (to avoid repeated requests)
    pub(crate) last_prefetch_piece: i32,
    /// Track pieces we've requested via read_piece() API to avoid duplicate requests
    pub(crate) requested_piece_via_api: std::collections::HashMap<i32, Instant>,
    /// Registry of wakers waiting for pieces to finish downloading
    pub(crate) piece_waiter: Arc<crate::piece_waiter::PieceWaiterRegistry>,
    /// Current seek type for DETERMINISTIC priority handling
    pub(crate) seek_type: SeekType,
    /// File size for container metadata detection
    pub(crate) file_size: u64,
    /// Stream creation time for startup instrumentation
    pub(crate) created_at: Instant,
    /// Whether we already logged the first successful read
    pub(crate) first_read_logged: bool,
    /// Whether we already logged the first startup wait
    pub(crate) first_wait_logged: bool,
    /// Last time we emitted a periodic "still waiting for piece" log line.
    /// Allows time-based throttling regardless of position alignment.
    pub(crate) last_wait_log: Option<Instant>,
    /// Last time we called reset_piece_deadline + set_piece_deadline to nudge
    /// libtorrent into re-evaluating the peer for a stuck piece.
    pub(crate) last_deadline_reset_at: Option<Instant>,
    /// Deadline (ms in the future) used by the wait loop in `poll_read` when
    /// re-asserting urgency on a piece. Lower = more urgent.
    ///
    /// Why per-stream: when a player opens an initial-playback connection AND
    /// a container-metadata connection (Cues at end-of-file) for the same file,
    /// both poll loops re-assert their wait pieces at libtorrent every 50ms.
    /// If both use deadline=0, libtorrent's piece picker breaks the tie by
    /// piece index — and since the Cues piece is at the END of the file, it
    /// gets sequentially queued *behind* the initial pieces, so it doesn't
    /// arrive until ~all earlier file pieces finish (observed: ~27 s wait).
    /// Giving InitialPlayback a small non-zero deadline (50ms) lets the
    /// ContainerMetadata stream keep deadline=0 win the ordering — the player
    /// can decode the Cues quickly and start playback while the head pieces
    /// trickle in normally.
    pub(crate) wait_deadline_ms: i32,
}

impl LibtorrentFileStream {
    fn set_priorities(&mut self, pos: u64) {
        // Skip if already complete
        if self.is_complete {
            return;
        }

        if self.piece_length == 0 {
            return;
        }

        // Correct calculation: file_offset is now the TRUE global byte offset of the file start.
        // pos is relative to file start.
        // So (file_offset + pos) is the global byte offset in the torrent.
        let current_piece = ((self.file_offset + pos) / self.piece_length) as i32;

        // Efficient cache check: if we are on the same piece, do nothing
        if current_piece == self.last_priorities_piece {
            return;
        }

        // DETERMINISTIC SEEK HANDLING: Use tracked SeekType instead of piece-jump heuristics
        match self.seek_type {
            SeekType::Sequential | SeekType::InitialPlayback => {
                // Sequential read or initial playback - just extend window, no cleanup
                tracing::trace!(
                    "set_priorities: {:?} at piece {} - extending window",
                    self.seek_type,
                    current_piece
                );
            }
            SeekType::ContainerMetadata => {
                // Container metadata read - ADD priorities, but preserve head pieces
                // This is critical: don't wipe out piece 0-7 priorities when reading moov/Cues
                tracing::debug!(
                    "set_priorities: ContainerMetadata at piece {} - preserving head priorities",
                    current_piece
                );
                // Don't clear deadlines - this is the key fix for container metadata!
            }
            SeekType::UserScrub => {
                // User scrub - add new deadlines for the new position but do NOT clear
                // existing ones. Other streams may be waiting for earlier pieces; clearing
                // their deadlines would stall those pieces indefinitely.
                tracing::debug!(
                    "set_priorities: UserScrub to piece {} - preserving existing deadlines",
                    current_piece
                );
            }
        }

        // After handling the seek, reset to sequential for subsequent reads
        self.seek_type = SeekType::Sequential;

        self.last_priorities_piece = current_piece;

        // Check if complete (all pieces downloaded)
        let mut all_downloaded = true;
        for p in self.first_piece..=self.last_piece {
            if !self.handle.have_piece(p) {
                all_downloaded = false;
                break;
            }
        }
        if all_downloaded {
            self.is_complete = true;
            return;
        }

        // Use centralized priorities calculation
        // Calculate dynamic EMA for download speed to avoid priority oscillations
        let status = self.handle.status();
        let _total_pieces = status.num_pieces; // Unused, kept for potential future use
        let current_speed = status.download_rate as f64;

        // Alpha of 0.2 means 20% weight to new sample, ~5 samples to converge
        if self.download_speed_ema == 0.0 {
            self.download_speed_ema = current_speed;
        } else {
            self.download_speed_ema = (self.download_speed_ema * 0.8) + (current_speed * 0.2);
        }

        let priorities = calculate_priorities(
            current_piece,
            self.last_piece + 1, // Use file's piece range, not torrent total
            self.piece_length,
            &self.cache_config,
            self.priority,
            self.download_speed_ema as u64,
            self.bitrate,
        );

        // Apply fair-sharing jitter (Shuffle Mirroring)
        // Adding a small unique offset to each stream's deadlines ensures that
        // when multiple streams are active, their "earliest" pieces are interleaved.
        let jitter = (self.stream_id % 10) as i32 * 5; // Up to 50ms jitter

        for item in priorities {
            if item.piece_idx >= self.first_piece
                && item.piece_idx <= self.last_piece
                && !self.handle.have_piece(item.piece_idx)
            {
                let shared_deadline = if item.deadline == 0 {
                    0
                } else if item.deadline >= 100000 {
                    // Don't jitter very long background deadlines
                    item.deadline
                } else {
                    item.deadline + jitter
                };
                self.handle
                    .set_piece_deadline(item.piece_idx, shared_deadline);
            }
        }
    }
}

impl tokio::io::AsyncRead for LibtorrentFileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pos = self.current_pos;
        self.set_priorities(pos);

        // Calculate which piece we need
        let piece = if self.piece_length > 0 {
            ((self.file_offset + pos) / self.piece_length) as i32
        } else {
            -1
        };

        // MEMORY-FIRST READING: Check if we have this piece in our local cache
        if piece >= 0 {
            // Check if we already have the right piece cached locally
            let have_cached = match &self.cached_piece_data {
                Some((cached_piece, _, _)) => *cached_piece == piece,
                None => false,
            };

            if have_cached {
                // Serve from local cache - FASTEST PATH
                if let Some((_, data, _)) = &self.cached_piece_data {
                    let offset_in_cached = ((self.file_offset + pos) % self.piece_length) as usize;
                    let available = data.len().saturating_sub(offset_in_cached);
                    let to_read = buf.remaining().min(available);

                    if to_read > 0 {
                        buf.put_slice(&data[offset_in_cached..offset_in_cached + to_read]);
                        self.current_pos += to_read as u64;
                        if !self.first_read_logged {
                            self.first_read_logged = true;
                            tracing::info!(
                                "startup: first direct-stream bytes ready after {:?} (piece={}, source=local-cache)",
                                self.created_at.elapsed(),
                                piece
                            );
                        }

                        if pos % (1024 * 1024) == 0 || pos < 4096 {
                            tracing::debug!(
                                "poll_read: Served {} bytes from MEMORY cache (piece {}, offset_in_cached={})",
                                to_read,
                                piece,
                                offset_in_cached
                            );
                        }
                    }
                    self.requested_piece_via_api.remove(&piece);
                    return std::task::Poll::Ready(Ok(()));
                }
            }

            // Try to get from the hybrid cache (hot tier then warm tier).
            // Both lookups are sync and do not park the tokio worker thread.
            // PLAN-002 Fix 1: previously used `futures::executor::block_on` which
            // parked the worker thread; now fully sync via moka's sync Cache.
            if let Some(piece_data) = self.piece_cache.get_sync(&self.info_hash, piece) {
                self.requested_piece_via_api.remove(&piece);
                let offset_in_cached = ((self.file_offset + pos) % self.piece_length) as usize;

                let available = piece_data.len().saturating_sub(offset_in_cached);
                let to_read = buf.remaining().min(available);

                if to_read > 0 {
                    buf.put_slice(&piece_data[offset_in_cached..offset_in_cached + to_read]);
                    self.current_pos += to_read as u64;
                    if !self.first_read_logged {
                        self.first_read_logged = true;
                        tracing::info!(
                            "startup: first direct-stream bytes ready after {:?} (piece={}, source=hybrid-cache)",
                            self.created_at.elapsed(),
                            piece
                        );
                    }

                    tracing::debug!(
                        "poll_read: Served {} bytes from hybrid cache (piece {}, offset_in_cached={})",
                        to_read,
                        piece,
                        offset_in_cached
                    );
                }

                // Store in L0 cache for sequential reads within the same piece.
                self.cached_piece_data = Some((piece, piece_data, 0));

                // === READ-AHEAD PREFETCH ===
                if piece != self.last_prefetch_piece {
                    self.last_prefetch_piece = piece;
                    let prefetch_cache = self.piece_cache.clone();
                    let prefetch_info_hash = self.info_hash.clone();
                    let prefetch_handle = self.handle.clone();
                    let last_piece = self.last_piece;

                    // ADAPTIVE PREFETCH COUNT
                    let prefetch_count: i32 = if self.download_speed_ema > 10_000_000.0 {
                        8
                    } else if self.download_speed_ema > 5_000_000.0 {
                        5
                    } else if self.download_speed_ema > 1_000_000.0 {
                        3
                    } else {
                        2
                    };

                    // Spawn background prefetch task: reads pieces from C++ memory storage
                    // and inserts them into the hot tier so they're ready before poll_read
                    // needs them.
                    tokio::spawn(async move {
                        for i in 1..=prefetch_count {
                            let next_piece = piece + i;
                            if next_piece > last_piece {
                                break;
                            }
                            if prefetch_cache.has_piece_sync(&prefetch_info_hash, next_piece) {
                                continue;
                            }
                            if !prefetch_handle.have_piece(next_piece) {
                                continue;
                            }
                            // Read directly from C++ memory storage.
                            let data = libtorrent_sys::memory_read_piece_for_hash(
                                &prefetch_info_hash,
                                next_piece,
                            );
                            if !data.is_empty() {
                                prefetch_cache.put(
                                    &prefetch_info_hash,
                                    next_piece,
                                    bytes::Bytes::from(data),
                                );
                                tracing::debug!(
                                    "Read-ahead: cached piece {} in hybrid hot tier",
                                    next_piece
                                );
                            }
                        }
                    });
                }

                return std::task::Poll::Ready(Ok(()));
            }
        }

        // Not in cache - check if piece is available in libtorrent
        if piece >= 0 && !self.handle.have_piece(piece) {
            // NOTIFICATION-BASED WAITING
            self.piece_waiter
                .register(&self.info_hash, piece, cx.waker().clone());

            // Re-assert urgency every wakeup so libtorrent never deprioritizes
            // this piece after its original deadline expires. The deadline is
            // per-stream (`wait_deadline_ms`) so concurrent initial-playback
            // and container-metadata streams don't all collapse to deadline=0
            // and force libtorrent into sequential picking by piece index.
            let wait_deadline = self.wait_deadline_ms;
            self.handle.set_piece_priority(piece, 7);
            self.handle.set_piece_deadline(piece, wait_deadline);

            let waker = cx.waker().clone();
            tokio::spawn(async move {
                // Safety-net fallback: if piece_waiter notification is missed (e.g.
                // due to a race between have_piece() and register()), wake again
                // after 500ms so we don't stall forever.  The piece_waiter handles
                // the fast path (wakes immediately on piece_finished_alert).
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                waker.wake();
            });

            // Log on first wait regardless of position, and then every 2 seconds.
            // Previously only fired when pos%1MB==0 or pos==0, which meant
            // ContainerMetadata waits (e.g. MKV Cues at pos=244996) were silent
            // for their entire duration.
            let now = std::time::Instant::now();
            let should_log = !self.first_wait_logged
                || self.last_wait_log.map_or(true, |t| now.duration_since(t).as_secs() >= 2);
            if should_log {
                let status = self.handle.status();
                // Check neighbors for hole detection: have pieces N+1..N+3 been
                // downloaded while N itself is missing?  That's a piece hole.
                let next_have: Vec<bool> = (1..=3)
                    .map(|d| self.handle.have_piece(piece + d))
                    .collect();
                let is_hole = next_have.iter().any(|&h| h);
                if !self.first_wait_logged {
                    self.first_wait_logged = true;
                    tracing::info!(
                        "poll_read: FIRST WAIT for piece {} ({:?}, pos={}, peers={}, speed={:.1}MB/s, paused={}, wait_deadline={}ms, next_have={:?})",
                        piece,
                        self.seek_type,
                        pos,
                        status.num_peers,
                        status.download_rate as f64 / 1_000_000.0,
                        status.is_paused,
                        self.wait_deadline_ms,
                        next_have,
                    );
                } else {
                    tracing::info!(
                        "poll_read: STILL WAITING for piece {} ({:?}, elapsed={:.1}s, pos={}, peers={}, speed={:.1}MB/s, paused={}, hole={}, next_have={:?})",
                        piece,
                        self.seek_type,
                        self.created_at.elapsed().as_secs_f64(),
                        pos,
                        status.num_peers,
                        status.download_rate as f64 / 1_000_000.0,
                        status.is_paused,
                        is_hole,
                        next_have,
                    );
                }
                self.last_wait_log = Some(now);
            }

            // STUCK-PIECE NUDGE: after 10s on same piece, re-assert priority/deadline.
            // NOTE: We intentionally do NOT call reset_piece_deadline here — that
            // temporarily removes the piece from libtorrent's deadline queue and was
            // measured to drop download speed from 8 MB/s to 0.2 MB/s.  libtorrent
            // already handles slow-peer re-requests via request_timeout (configured
            // at session creation), so the only safe nudge is to keep asserting the
            // highest priority so the piece never drifts to a lower urgency bucket.
            let elapsed_secs = self.created_at.elapsed().as_secs();
            let needs_nudge = elapsed_secs >= 10
                && self.last_deadline_reset_at
                    .map_or(true, |t| now.duration_since(t).as_secs() >= 10);
            if needs_nudge {
                self.handle.set_piece_priority(piece, 7);
                self.handle.set_piece_deadline(piece, 0);
                self.last_deadline_reset_at = Some(now);
                tracing::info!(
                    "poll_read: NUDGE re-asserted priority/deadline for stuck piece {} (elapsed={:.1}s)",
                    piece,
                    elapsed_secs as f64,
                );
            }

            return std::task::Poll::Pending;
        }

        // Piece is downloaded but not in cache — read directly from C++ memory storage.
        // This path is taken when:
        //   (a) The alert pump hasn't processed piece_finished_alert yet (race window), OR
        //   (b) The piece was evicted from C++ (returns empty) but is in the warm tier.
        if piece >= 0 && !self.requested_piece_via_api.contains_key(&piece) {
            let piece_data = libtorrent_sys::memory_read_piece_for_hash(&self.info_hash, piece);
            if !piece_data.is_empty() {
                tracing::debug!(
                    "poll_read: Direct read piece {} from C++ memory storage ({} bytes)",
                    piece,
                    piece_data.len()
                );

                // Wrap in Bytes (takes ownership, no copy).
                let piece_bytes = bytes::Bytes::from(piece_data);

                // Serve bytes immediately — no async round-trip needed.
                let offset_in_cached =
                    ((self.file_offset + pos) % self.piece_length) as usize;
                let available = piece_bytes.len().saturating_sub(offset_in_cached);
                let to_read = buf.remaining().min(available);
                if to_read > 0 {
                    buf.put_slice(
                        &piece_bytes[offset_in_cached..offset_in_cached + to_read],
                    );
                    self.current_pos += to_read as u64;
                    if !self.first_read_logged {
                        self.first_read_logged = true;
                        tracing::info!(
                            "startup: first direct-stream bytes ready after {:?} (piece={}, source=cpp-memory)",
                            self.created_at.elapsed(),
                            piece
                        );
                    }
                }

                self.requested_piece_via_api.remove(&piece);
                // Put into the hybrid hot tier (sync, O(1)). This ensures the piece
                // is tracked for size-based eviction to the warm disk tier.
                self.piece_cache.put(&self.info_hash, piece, piece_bytes.clone());
                // Store in L0 per-stream cache for the next sequential read.
                self.cached_piece_data = Some((piece, piece_bytes, 0));

                return std::task::Poll::Ready(Ok(()));
            } else {
                // C++ storage returned empty: the piece was evicted from C++ by the
                // drain task after being persisted to the warm tier. The warm tier
                // should have it — try the hybrid cache again before waiting.
                if let Some(warm_data) = self.piece_cache.get_sync(&self.info_hash, piece) {
                    tracing::debug!(
                        "poll_read: piece {} served from warm tier after C++ eviction",
                        piece
                    );
                    let offset_in_cached =
                        ((self.file_offset + pos) % self.piece_length) as usize;
                    let available = warm_data.len().saturating_sub(offset_in_cached);
                    let to_read = buf.remaining().min(available);
                    if to_read > 0 {
                        buf.put_slice(&warm_data[offset_in_cached..offset_in_cached + to_read]);
                        self.current_pos += to_read as u64;
                        if !self.first_read_logged {
                            self.first_read_logged = true;
                            tracing::info!(
                                "startup: first direct-stream bytes ready after {:?} (piece={}, source=warm-tier)",
                                self.created_at.elapsed(),
                                piece
                            );
                        }
                    }
                    self.requested_piece_via_api.remove(&piece);
                    self.cached_piece_data = Some((piece, warm_data, 0));
                    return std::task::Poll::Ready(Ok(()));
                }

                // Piece is in the eviction channel but warm write not yet committed.
                // Wake in 10 ms to retry.
                tracing::debug!(
                    "poll_read: piece {} not yet in warm tier — waiting 10ms",
                    piece,
                );
                self.piece_waiter
                    .register(&self.info_hash, piece, cx.waker().clone());

                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    waker.wake();
                });
                return std::task::Poll::Pending;
            }
        }

        // Should not reach here
        tracing::error!("poll_read: Unexpected state - piece={}, pos={}", piece, pos);
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Memory-only streaming: unexpected state in poll_read",
        )))
    }
}

impl tokio::io::AsyncSeek for LibtorrentFileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        // Calculate target position
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos,
            std::io::SeekFrom::Current(delta) => (self.current_pos as i64 + delta).max(0) as u64,
            std::io::SeekFrom::End(delta) => (self.file_size as i64 + delta).max(0) as u64,
        };

        // DETERMINISTIC: Detect seek type based on target position
        let end_threshold = container_metadata_start(self.file_size);

        self.seek_type = if new_pos >= end_threshold {
            SeekType::ContainerMetadata
        } else {
            SeekType::UserScrub
        };

        tracing::debug!(
            "start_seek: {} -> {} ({:?})",
            self.current_pos,
            new_pos,
            self.seek_type
        );

        // Memory-only mode: just update position, no file handle to seek
        self.current_pos = new_pos;
        // Invalidate local cached piece data since position changed
        self.cached_piece_data = None;
        Ok(())
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        // Memory-only mode: position is already set in start_seek
        let pos = self.current_pos;

        let piece_idx = if self.piece_length > 0 {
            ((self.file_offset + pos) / self.piece_length) as i32
        } else {
            -1
        };

        if piece_idx != self.last_priorities_piece {
            self.set_priorities(pos);
        }

        std::task::Poll::Ready(Ok(pos))
    }
}
