Now I have a thorough picture of the whole codebase. Let me write up the analysis.Now I have everything I need. Let me write the full analysis.Now I have a complete picture. Here's the full analysis:

---

## server.js deep analysis — takeaways for your Rust implementation

The JS file is a webpack bundle of `torrent-stream` + a thin Express/connect HTTP layer. The interesting code lives in three areas: the **engine** (piece scheduling, wire management), the **FileStream** (the per-request reader), and the **EngineFS layer** (lifecycle, routing, bandwidth policy). I'll go through each with concrete observations.

---

### 1. The FileStream._read() loop — self-healing priority

The most important streaming primitive. On every `_read()` call it:

1. Checks `bitfield.get(piece)` — if the piece is already downloaded, reads immediately via a `bagpipe(2)` (max 2 in-flight store reads per stream).
2. If not downloaded, it checks whether *any existing selection* covers the current piece. If none does, it creates a **new high-priority selection** from the current piece to `selection.to`. This is the self-healing behavior.
3. Marks `_critical` pieces ahead via `engine.critical(piece, width)`, where `width = min(1MB / pieceLength, 4)`. So on a torrent with 256KB pieces it marks 4 ahead; on a 1MB piece torrent just 1. This scales urgency with piece granularity.
4. Calls `engine.refresh()`, then waits for a `notify` event.

**Rust takeaway:** Your `re-assert urgency` in the alert pump path is the right instinct, but check that the number of pieces you mark with a tight deadline also scales with piece size. `min(1MB / piece_len, 4)` is a simple and effective formula. The self-healing selection (re-register if somehow no selection covers the current piece) is a defensive move worth porting as a safety net, especially during concurrent seeks.

---

### 2. The rolling buffer window (`bufferPieces` / `selectTo` / `readFrom`)

When `engine.buffer` is set (enabled in no-cache mode: 15MB), the buffer size is converted to `bufferPieces = buffer / pieceLength`. On every read:

```
selection.selectTo = min(endPiece, currentPiece + bufferPieces)
selection.readFrom = currentPiece
```

The piece scheduler's inner loop iterates `j` from `next.from + next.offset` to `next.selectTo || next.to`. So only pieces within `readFrom..selectTo` are requested, not the full file. Meanwhile, `isPieceSelected(p)` in the circular buffer storage checks `p >= sel.readFrom && p <= sel.selectTo` — so eviction can also tell what's "active" in the current buffer window.

**Rust takeaway:** Your "priority window" (urgent + proactive lookahead) is architecturally equivalent. One thing server.js makes explicit that may be worth checking: the rolling window is updated *on each read call*, not on a timer. This means it tracks playback head position exactly. If your lookahead window is recalculated less frequently, you may over-request or under-request briefly after a seek.

---

### 3. `shufflePriority` — round-robin fairness across concurrent streams

When a wire's request queue fills up while serving a *priority* selection, `shufflePriority(i)` is called. It moves that selection to just after the last other priority selection, implementing round-robin ordering:

```js
for (var last = i, j = i; j < engine.selection.length && engine.selection[j].priority; j++) last = j;
engine.selection.splice(last, 0, engine.selection.splice(i, 1)[0]);
```

This is different from your per-stream jitter approach. The JS approach reshuffles the actual selection array every time a wire saturates on a priority selection. The effect is that if two streams both have high-priority pieces, neither starves.

**Rust takeaway:** Your jitter-based approach is cleaner and doesn't require a mutable global sort. But if you ever see one stream starving another at the libtorrent piece picker level (jitter isn't enough), this round-robin hint is a good fallback to be aware of.

---

### 4. `rank()` — skip requesting from slow wires

For wires below `SPEED_THRESHOLD = 3 * BLOCK_SIZE`, the JS computes a `rank` function per wire:

> "Can faster peer wires collectively deliver this piece before this slow wire would finish it?" If yes, return `false` (skip requesting).

The heuristic estimates how many bytes faster wires could deliver in the time it would take this slow wire to finish the piece. If the answer is yes, the slow wire's request queue is not used for that piece.

**Rust takeaway:** libtorrent's piece picker has its own version of this, so you likely get it for free with the `libtorrent` backend. With `librqbit` you'd need to implement it. The key insight is: **don't penalize slow peers by over-assigning them; just skip them for congested pieces.**

---

### 5. The Growler — two-phase download speed management

`engine.setFloodedPulse(flood, pulse)` configures:
- `flood`: download freely until `flood` bytes total have been downloaded (initial burst)
- `pulse`: once flood is crossed, throttle download speed to `pulse` bps

In defaults: `flood = 0` (no burst phase), `pulse = ~2.5MB/s` (soft), hard limit `~3.5MB/s`. The update loop checks `swarm.downloaded >= _flood && downloadSpeed() > pulse` and if both are true, delays the next scheduler tick by 500ms (`_.debounce`).

**Rust takeaway:** You have `swarmCap` (pause swarm when speed > limit). The JS goes further with the two-phase approach: flood lets you establish connections and fill the pipeline without throttling early, then the pulse kicks in once streaming is stable. Consider whether a brief unthrottled burst window on stream open helps peer establishment in your implementation.

---

### 6. SwarmCap with buffer fill threshold

In no-cache / circular-buffer mode, the swarm cap switches from a speed check to a **buffer fill ratio check**:

```js
// Buffer fill = average progress across active selections
buf = (sel.from + sel.offset - sel.readFrom) / (sel.selectTo - sel.readFrom);
// Pause when buf > 0.75 and unchoked > minPeers
```

The fill ratio is averaged across all active selections. Pause happens at 75% fill.

**Rust takeaway:** Your hot tier has a size bound. But you don't appear to have a feedback loop that throttles *download speed* when the hot tier is nearly full. If the hot tier fills up and eviction to the warm tier is slower than incoming pieces, you could hit back-pressure. A "pause the swarm when hot tier > X% and warm tier I/O is saturated" check is worth adding.

---

### 7. Dynamic request queue size

```js
function getRequestsNumber() {
  var unchoked = wires.filter(p => !p.peerChoking).length;
  var normalRange = 1 - Math.max(0, Math.min(1, (unchoked - 1) / 29));
  return Math.round(45 * Math.pow(normalRange, 4) + 5);
}
```

This scales from 50 requests/wire (1 unchoked peer) down to 5 requests/wire (30+ unchoked peers). The quartic curve means it stays high until you have many peers, then drops sharply.

**Rust takeaway:** libtorrent manages request queue depth internally, but if you're controlling it via `set_piece_deadline` call density, a similar adaptive approach (fewer tight deadlines per wire when you have many peers) may help avoid deadline collisions.

---

### 8. `lockedPieces` — pins during store reads

Before a store read begins, the piece index is pushed to `engine.lockedPieces`. After the read callback fires (success or error), it's removed. Other code checks `lockedPieces` before evicting or resetting a piece.

**Rust takeaway:** Your hot tier is evicted by a background channel. Make sure pieces that are actively being copied out of the cache into a streaming response are pinned and excluded from the eviction candidate set. A missed-eviction stall is a rare bug but subtle to diagnose.

---

### 9. `prewarmStream` on open-ended Range header

```js
if (range && range.endsWith("-")) {
  EngineFS.getDefaults(e.infoHash).circularBuffer || prewarmStream(e.infoHash, fileIdx);
}
```

When the `Range` header ends with `-` (e.g. `Range: bytes=0-`), which is what many players send for progressive streaming (not seeking), the server calls `file.select()` to start downloading the whole file — rather than waiting for the first piece request to arrive.

**Rust takeaway:** You already classify initial playback from the offset. But the open-ended Range signal is a stronger hint: the client is not seeking, it wants a full progressive download. Worth special-casing this to pre-select all pieces at background priority so the download starts warming up before the first bytes are flushed.

---

### 10. `enginefs-prio` custom header

```js
if (req.headers["enginefs-prio"])
  opts.priority = parseInt(req.headers["enginefs-prio"]) || 1;
```

The Stremio UI sends this header (with value `10`) when fetching byte ranges for the OpenSubtitles movie hash calculation — two specific 64KB chunks at offset 0 and at `fileSize - 65536`. These are metadata fetches, not playback, and must not downgrade playback priority.

**Rust takeaway:** Your seek classification catches some of this, but an explicit priority override header gives the client direct control. Stremio's internal code specifically uses `enginefs-prio: 10` for the hash probe fetches. Support this header so that hash calculation doesn't interfere with your seek classification logic.

---

### 11. Stream lifecycle with debounced timeouts

`Counter` tracks `stream-open` / `stream-close` events per `(infoHash, fileIndex)` and per `infoHash`. It doesn't fire `stream-inactive` / `engine-inactive` immediately when count hits 0 — it waits `STREAM_TIMEOUT = 30s` and `ENGINE_TIMEOUT = 60s`. If a new open arrives within the window, the timer cancels.

This means a player reconnecting after a brief stall or seek doesn't tear down and rebuild the engine.

**Rust takeaway:** Architecture.md mentions the slow monitor runs every 2s but deliberately doesn't touch piece priorities. Check that your engine teardown / torrent removal path has a similar grace window. If you remove a torrent immediately when the last HTTP connection closes, players that reconnect after a brief pause will have to re-add the torrent and re-establish peers from scratch.

---

### 12. `virtual` piece subdivision

If `torrent.pieceLength > 524288 && pieceLength % 524288 == 0`, the engine subdivides into 512KB virtual pieces:

```js
var pieceLength = torrent.pieceLength > 524288 && torrent.pieceLength % 524288 == 0
  ? 524288 : torrent.pieceLength;
```

Virtual pieces map back to real pieces via `mapPiece(index) = floor(index * pieceLength / verificationLen)`. This lets the piece scheduler operate at finer granularity for prioritization and buffering, even on torrents with large (2MB+) native pieces.

**Rust takeaway:** This is the most structurally invasive thing server.js does that your architecture doesn't mention. libtorrent works natively at torrent-piece granularity. For movies with 4MB pieces, your urgent window of "15 pieces" is actually 60MB of lookahead, which may be overkill. If you find priority granularity to be coarse on large-piece torrents, this subdivision trick is the JS answer to that problem.

---

### Summary table

| Behavior | server.js approach | Your Rust status (from architecture.md) |
|---|---|---|
| Critical piece marking | `min(1MB/pieceLen, 4)` pieces ahead | 15-piece / 15-second urgent window |
| Multi-stream fairness | `shufflePriority` round-robin | Per-stream deadline jitter |
| Slow peer skipping | `rank()` per-wire estimation | Delegated to libtorrent |
| Download throttling | Growler (flood + pulse) + SwarmCap | SwarmCap equivalent only |
| Buffer fill throttle | Pause at 75% in memory mode | Not mentioned |
| Read concurrency | `bagpipe(2)` per FileStream | Async but no explicit cap mentioned |
| Piece pinning during read | `lockedPieces` | Worth verifying |
| Open-ended Range hint | `prewarmStream` on `Range: N-` | Seek classification covers start/end; open-ended is a separate signal |
| External priority override | `enginefs-prio` header | Not mentioned |
| Engine teardown grace | 30s / 60s debounce | Not mentioned |
| Piece subdivision | 512KB virtual pieces on large torrents | Not implemented (libtorrent-native pieces) |

The three highest-value items to verify or add to your Rust implementation are: **piece pinning during cache reads** (correctness risk), **engine teardown grace period** (user experience regression on brief reconnects), and **the `enginefs-prio` header** (needed for Stremio's subtitle hash calculation to not interfere with playback). The others are optimization opportunities with measurable but less critical impact.