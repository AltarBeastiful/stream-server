## Implementation plans — three high-priority items

---

### 1. Piece pinning during cache reads

**Problem**

The hot-tier eviction path runs on a background channel. A piece can be nominated for eviction at any point after it enters the hot tier. If a streaming read has already started copying that piece's bytes out of the cache — but the eviction fires and frees the allocation before the copy finishes — the reader gets a use-after-free or a partial buffer. In the JS code `lockedPieces` is a simple integer array; before any store read begins the piece index is pushed in, and the eviction path checks the array before freeing. Your Rust cache does not have an equivalent guard.

**What to build**

Add a pin-count map alongside the hot tier. It maps piece index to an atomic or mutex-guarded integer. The streaming reader increments the pin count before beginning a read and decrements it in the read's completion handler (whether success or error). The eviction path, before writing a piece to the warm tier and freeing the hot-tier allocation, checks the pin count. If it is non-zero, the piece is skipped for that eviction cycle and re-queued.

The pin-count map does not need to be a separate structure — it can live as an extra field on the hot-tier entry. The key invariant is: **a piece with a non-zero pin count must never be removed from the hot tier until the count returns to zero.**

A piece can remain pinned across multiple reads if two concurrent streams happen to need the same piece at the same time. Both increment before reading, both decrement on finish; only when the last decrement brings the count to zero is the piece eligible for eviction. This means eviction of a piece that is in high demand may be deferred several cycles, which is the correct behavior — a piece that is actively being read should stay in the fastest tier.

One edge case worth handling: if a piece stays pinned so long that the hot tier exceeds its size bound, the eviction loop should log a warning and skip it rather than blocking. The bound is a soft target; a brief overshoot is acceptable and far better than corrupting a streaming read.

**Acceptance criteria**

Under a test with two concurrent streams seeking to the same piece simultaneously, the hot-tier size never goes negative, no reads return a short or zero buffer, and pieces with pin count > 0 never appear in the warm tier.

---

### 2. Engine teardown grace period

**Problem**

When the last HTTP connection for a given torrent closes, your engine teardown path likely removes the torrent and releases all state immediately. Video players do not maintain a single persistent connection — they reconnect constantly. A seek fires a new connection to a new byte offset. A stall causes the player to drop the connection and retry. Some players open several connections simultaneously and close the ones they don't need. If teardown happens the moment the last connection closes, a player that reconnects 200ms later will find no engine, have to re-add the torrent, wait for peer re-discovery, and potentially re-download metadata. The user experiences this as a multi-second freeze when seeking or resuming after a brief pause.

**What to build**

Introduce a per-torrent `inactive_since` timestamp. When the connection count for a torrent drops to zero, record the timestamp but do not tear down. A background task (your slow monitor, or a dedicated one) checks every few seconds whether any torrent has been inactive for longer than a configurable grace window — 30 seconds is the value server.js uses for stream-level inactivity and 60 seconds for the engine. If a new connection arrives for the torrent before the window expires, clear the `inactive_since` timestamp and continue normally. If the window expires with no new connection, proceed with teardown.

The grace period applies to both peer connections and libtorrent session state. Specifically, during the grace window the torrent should remain in the session with all piece priorities intact, the warm cache tier should not be flushed, and peer connections should stay open. Tearing down connections early just to rebuild them 30 seconds later wastes the peer discovery and connection establishment time that was already paid.

You may want two separate windows: a shorter one (30s) that pauses piece downloading to stop consuming bandwidth, and a longer one (60s) after which the torrent is fully removed. The intermediate state — torrent is present in the session, no active downloads, warm cache is intact — is cheap to maintain and covers the reconnect case at essentially zero cost.

**Configuration**

Expose both windows as settings. Some users run stream-server on a machine with limited RAM and want fast teardown. Others want seamless resume after arbitrarily long pauses. The defaults of 30s / 60s are reasonable starting points.

**Acceptance criteria**

A player that closes all connections and reopens them within the grace window should resume streaming without re-fetching torrent metadata or re-discovering peers. The reconnect latency should be measured in milliseconds, not seconds.

---

### 3. `enginefs-prio` request priority header

**Problem**

Stremio's UI makes two fetch requests to compute the OpenSubtitles movie hash: one for 64KB at byte offset 0, one for 64KB at the end of the file (`fileSize - 65536`). These are sent with the custom header `enginefs-prio: 10`. They are metadata probes, not playback, but they hit the same `/stream` endpoint and therefore go through your seek classifier. Your classifier sees a request to the final bytes of the file and correctly labels it `ContainerMetadata`, giving it the highest urgency with a 0ms deadline. That's the right treatment for a player trying to parse the container index — but wrong for a hash probe, which has no latency requirement. The effect is that the hash probe competes at the highest priority level against the actual container metadata fetch, and may cause the piece picker to serve hash pieces instead of the real index pieces the player needs to start decoding.

**What to build**

Parse the `enginefs-prio` header on every incoming request before running seek classification. It carries an integer where higher means more important. When the header is present, use it to override the classification's deadline assignment rather than skipping classification entirely — you still want to know *what kind* of read this is for logging and buffer management, but the priority that goes to libtorrent should come from the header.

The mapping from header value to libtorrent deadline is a simple linear scale. The JS uses `priority: 10` for hash probes. A reasonable mapping: treat the absence of the header as priority 1 (normal), values 1–4 as background (no tight deadline), values 5–7 as normal streaming, values 8–9 as elevated, and value 10 as urgent. For hash probes specifically, `prio: 10` means "high priority for the HTTP fetch layer" but the fetch is for small chunks that don't need piece-level urgency — you want the HTTP response to be fast, not the torrent download to be reprioritized. In practice, hash probes almost always hit already-downloaded pieces (the first and last chunks of a file are typically downloaded early) so you may only need to ensure the probe reads from the warm or hot cache without blocking.

In addition to deadline control, the header value can feed into your per-stream slot decision. A prio-10 probe should skip the per-stream slot's last-piece-read optimization and go directly to the cache tiers, since it reads non-sequential offsets that won't benefit from the sequential shortcut.

**Acceptance criteria**

With a player that computes the OpenSubtitles hash on open, the container metadata pieces (the real end-of-file index) should reach the player before or simultaneously with the hash probe chunks, not after. The existing seek classification behavior for non-prio requests should be unchanged. Requests bearing `enginefs-prio: 10` should appear in logs with their header value visible.