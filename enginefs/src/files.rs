use crate::backend::{FileStreamTrait, TorrentHandle};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek};

pub struct FileHandle<H: TorrentHandle> {
    pub size: u64,
    pub name: String,
    pub stream: Box<dyn FileStreamTrait>,
    pub engine: Arc<crate::engine::Engine<H>>,
}

impl<H: TorrentHandle> FileHandle<H> {
    pub fn new(
        size: u64,
        name: String,
        stream: Box<dyn FileStreamTrait>,
        engine: Arc<crate::engine::Engine<H>>,
    ) -> Self {
        Self {
            size,
            name,
            stream,
            engine,
        }
    }
}

impl<H: TorrentHandle> AsyncRead for FileHandle<H> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<H: TorrentHandle> Drop for FileHandle<H> {
    fn drop(&mut self) {
        // Decrement active_streams and capture the value that was there before the decrement.
        let prev = self.engine.active_streams.fetch_sub(1, Ordering::SeqCst);

        // If prev == 1 we just decremented to 0: this was the last active stream.
        // Record the timestamp so the grace-period background task knows when
        // inactivity began for this torrent.
        if prev == 1 {
            let ts = crate::elapsed_secs();
            // Only update if the engine hasn't already been re-activated (sentinel = -1).
            // Use a compare-exchange: if inactive_since is currently -1 (active) or 0
            // (never had streams), set it to the current timestamp.
            // SeqCst: we need this store to be visible to the grace-period task promptly.
            self.engine.inactive_since.store(ts, Ordering::SeqCst);
            tracing::debug!(
                "FileHandle::drop: last stream closed for {} — inactive_since={}",
                self.engine.info_hash,
                ts
            );
        }
    }
}

impl<H: TorrentHandle> AsyncSeek for FileHandle<H> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.stream).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.stream).poll_complete(cx)
    }
}
