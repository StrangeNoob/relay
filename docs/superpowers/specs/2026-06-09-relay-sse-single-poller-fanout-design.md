# Relay SSE Single-Poller Fan-Out — Design

**Date:** 2026-06-09
**Status:** Approved (brainstorming)
**Component:** `internal/api` (SSE stream)

## Problem

The dashboard stream at `GET /api/stream` (`internal/api/stream.go`) is **per-connection**.
Every open dashboard runs its own server-side goroutine with a 1 s ticker, and on each tick
independently calls `broker.Queues` (a Redis `SCAN`), then `broker.Stats` (≈4 round-trips) and
`broker.Counters` (2 `GET`s) **per queue**. Redis load therefore scales as
**O(connections × queues) per second**, and every connection recomputes the *identical* global
snapshot. With many dashboards open, Redis — single-threaded for command execution — saturates
long before Go's HTTP layer feels any strain. The `SCAN`-per-connection-per-second is the worst
offender.

CLAUDE.md already documents this as an intentional demo-grade limitation:

> "SSE is per-connection. Each open dashboard tab runs its own server-side ticker goroutine
> reading Redis every ~1 s. This is fine for a demo; a production deployment would fan-out from a
> single poller."

This change implements that fan-out.

## Goal

Replace per-connection polling with a single in-process **hub**: one background goroutine polls
Redis once per interval, builds the snapshot, and broadcasts it to every connected subscriber.
Redis load becomes **O(queues) per second, independent of connection count**.

## Non-Goals

- **No cross-replica fan-out (Redis Pub/Sub).** An in-process hub per server replica already polls
  only O(queues)/sec, which is cheap. Pub/Sub is deferred future work, not needed at this scale.
- **No wire-format change.** The emitted SSE event stays byte-identical, so the committed
  `web/dist` client needs no change and no rebuild.
- **No new HTTP endpoints, flags, or dependencies.**

## Architecture

A new `hub` type in `internal/api/hub.go` owns all Redis polling and broadcast. It depends on a
small interface (not the concrete broker) so it can be unit-tested without Redis.

```go
// snapshotSource is the slice of the broker the hub needs.
// Satisfied by *broker.Broker.
type snapshotSource interface {
    Queues(ctx context.Context) ([]string, error)
    Stats(ctx context.Context, queue string) (broker.Stats, error)
    Counters(ctx context.Context, queue string) (broker.Counters, error)
}

type subscriber struct {
    ch chan []byte // buffered, cap 1; latest-wins
}

type hub struct {
    src      snapshotSource
    logger   *slog.Logger
    interval time.Duration // = streamInterval; injectable for tests

    mu     sync.Mutex
    subs   map[*subscriber]struct{}
    last   []byte             // most-recent marshalled snapshot, for instant populate
    cancel context.CancelFunc // non-nil iff the poller goroutine is running
}
```

`API` (in `internal/api/api.go`) gains a `hub *hub` field, constructed in `New`. **`api.New`'s
signature is unchanged** (`New(b *broker.Broker, logger *slog.Logger) http.Handler`); it builds the
hub from `b` and the logger.

### Lazy lifecycle (poll only while ≥1 subscriber)

- **`newHub(src snapshotSource, logger *slog.Logger, interval time.Duration) *hub`** — constructs
  an idle hub with an empty `subs` map. Does **not** start polling.
- **`subscribe() *subscriber`** (under `mu`): if `subs` is empty, start the poller —
  `ctx, cancel := context.WithCancel(context.Background())`, store `cancel`, `go h.run(ctx)`.
  Create the subscriber (buffered channel cap 1), register it. Capture `last` while still holding
  the lock. After unlocking, if the captured `last != nil`, do a non-blocking send of it to the
  subscriber's channel so a late joiner's UI populates instantly. Return the subscriber.
- **`unsubscribe(s *subscriber)`** (under `mu`): delete `s` from `subs`; if `subs` is now empty and
  `cancel != nil`, call `cancel()` and set it to `nil`. The poller goroutine then returns on its
  next `select`. Idle Redis load is zero.
- **`run(ctx context.Context)`**: poll **once immediately** (preserving today's "immediate first
  snapshot"), then loop on a `time.Ticker(h.interval)`:
  ```
  for {
      select {
      case <-ctx.Done():
          return
      case <-ticker.C:
          h.pollAndBroadcast(ctx)
      }
  }
  ```
- **`pollAndBroadcast(ctx)`**: build the snapshot (see Data flow); on success store it in `last`
  (under `mu`) and `broadcast` it; on error, log and skip (keep `last`, keep polling).
- **`broadcast(buf []byte)`** (under `mu`): for each subscriber, non-blocking latest-wins send —
  ```
  select {
  case s.ch <- buf:
  default: // channel full: drop the stale snapshot, enqueue the newest
      select { case <-s.ch: default: }
      select { case s.ch <- buf: default: }
  }
  ```
  The poller never blocks on a slow client.

### Data flow (snapshot building — moved from `stream.go`)

`pollAndBroadcast` reuses today's logic verbatim, just relocated into the hub:

1. `queues, err := src.Queues(ctx)` — on error, log + skip the tick.
2. For each queue `q`: `src.Stats(ctx, q)` and `src.Counters(ctx, q)`; on a per-queue error, log
   and `continue` (omit that queue from this snapshot).
3. Assemble `[]queueSnapshot` (`queue, ready, inflight, delayed, dlq, processed_total, dead_total`)
   — the existing struct in `stream.go`, unchanged.
4. `json.Marshal`; on error, log + skip.
5. Store as `last`, then `broadcast`.

### The simplified `stream` handler

`stream.go` keeps the `streamInterval` constant and the `queueSnapshot` type, and reduces the
handler to:

```go
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        a.writeError(w, http.StatusInternalServerError, "streaming unsupported")
        return
    }
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    sub := a.hub.subscribe()
    defer a.hub.unsubscribe(sub)

    ctx := r.Context()
    for {
        select {
        case <-ctx.Done():
            return
        case buf := <-sub.ch:
            if _, err := fmt.Fprintf(w, "data: %s\n\n", buf); err != nil {
                return // client disconnected
            }
            flusher.Flush()
        }
    }
}
```

## Concurrency & safety

- `mu` guards `subs`, `last`, and `cancel`. `subscribe`, `unsubscribe`, `broadcast`, and the
  `last` update in `pollAndBroadcast` all take it.
- The immediate-populate send in `subscribe` happens **after** unlocking (it targets the new
  subscriber's own channel, which no one else holds yet), avoiding holding `mu` across a channel op
  that interacts with broadcast.
- **Benign restart overlap:** a `subscribe` arriving in the instant after the last `unsubscribe`
  cancels the poller starts a fresh poller while the old goroutine may not have returned yet. The
  old goroutine's `ctx` is already cancelled, so its next `select` returns; it touches no shared
  state on the way out (it does not modify `cancel`). At worst it performs one final harmless
  broadcast of fresh data. This is documented and acceptable; no generation counter is needed.
- All tests run under `-race`.

## Wire-format compatibility

The event remains `data: <JSON array of queueSnapshot>\n\n`, all queues per snapshot, same field
names and types. The deployed `web/dist` dashboard is unaffected — **no frontend change, no dist
rebuild**. This is a pure server-side internal refactor.

## Error handling

| Condition | Behavior |
|---|---|
| `Queues` Redis error | Log, skip the tick, keep poller alive and `last` cached (matches current "skip this tick"). |
| Per-queue `Stats`/`Counters` error | Log, omit that queue from this snapshot, continue others. |
| `json.Marshal` error | Log, skip the tick. |
| Slow/non-reading subscriber | Latest-wins drop (silent — per-drop logging would be noisy); poller never blocks. |
| Client disconnect | `r.Context().Done()` or a write error returns from the handler; `defer` unsubscribes. |
| Server shutdown | `srv.Shutdown` closes connections → handlers return → unsubscribe → last unsubscribe stops the poller. No explicit hub `Close` needed. |

## Testing

### Unit — `internal/api/hub_test.go` (no Redis)

A fake `snapshotSource` records call counts and signals on a channel each time `Queues` is called,
so tests synchronize deterministically (no `time.Sleep`, `-race`-clean). The fake returns a fixed
queue list and canned `Stats`/`Counters`, and can be switched to return an error.

1. **Lazy start** — a freshly constructed hub performs no polls until the first `subscribe`; after
   `subscribe`, at least one poll occurs.
2. **Lazy stop** — after the last `unsubscribe`, polling ceases (no further `Queues` calls within a
   few intervals).
3. **Fan-out** — with N subscribers, a single poll cycle delivers the same snapshot to all N; the
   source is called **once per cycle**, not N times.
4. **Latest-wins / slow consumer** — a subscriber that never reads does not block the poller; other
   subscribers keep receiving; the slow subscriber's channel only ever holds the newest snapshot.
5. **Late joiner** — subscribing after `last` is populated delivers the cached snapshot
   immediately (before the next tick).
6. **Redis-error tick** — when the fake returns an error, the poller logs, skips, and stays alive
   (subsequent successful ticks resume broadcasting).

Tests pass a small `interval` (e.g., a few ms) and a discard logger.

### Integration — `internal/api` against real Redis (DB 12)

Keep/adapt the existing `/api/stream` test: start the API over a real broker, connect, enqueue a
job, read one SSE snapshot, and assert the queue's fields (now served through the hub). Confirms
end-to-end behavior is unchanged from the client's perspective.

## Files

- **Create** `internal/api/hub.go` — `hub`, `subscriber`, `snapshotSource`, `newHub`, `subscribe`,
  `unsubscribe`, `run`, `pollAndBroadcast`, `broadcast`.
- **Create** `internal/api/hub_test.go` — the six unit tests above with a fake source.
- **Modify** `internal/api/api.go` — add `hub *hub` field to `API`; construct it in `New`.
- **Modify** `internal/api/stream.go` — keep `streamInterval` + `queueSnapshot`; replace the
  handler body with subscribe/relay/unsubscribe; remove `writeSnapshot` (logic moves to the hub).
- **Modify** existing `internal/api` stream integration test if needed to remain green.
- **Modify** `CLAUDE.md` — update the "SSE is per-connection" limitation entry to describe the
  single-poller fan-out: one background poller while ≥1 dashboard is connected, broadcasting to all
  subscribers; Redis load is now O(queues)/sec rather than O(connections × queues)/sec; still
  per-process (each server replica runs its own poller).

## Success criteria

- Redis is polled at most once per `interval` per server process while ≥1 dashboard is connected,
  regardless of how many are connected; zero polling when none are connected.
- The SSE wire format is unchanged; the existing dashboard works without modification.
- All new unit tests and the adapted integration test pass under `-race`; `golangci-lint` clean.
