# Relay — Phase 2: Prometheus Metrics

**Status:** Approved design · **Date:** 2026-06-08
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 2 (depth) — final sub-project (follows 2a delayed/backoff, 2b priority, 2c idempotency, 2d rate limiting)

## Purpose

Make the engine observable. The parent spec calls for "Prometheus counters/gauges (enqueued,
processed, retried, dead, in-flight, latency)." This slice instruments every job-state transition
the broker performs and exposes them in Prometheus exposition format, plus live queue-depth gauges
read straight from Redis at scrape time. It is the last remaining Phase 2 feature; completing it
closes Phase 2.

## Scope

In scope:

- A `Metrics` interface in `broker` (consumer-side contract) with a no-op default, set via a new
  `WithMetrics` option. Opt-in, additive, zero behavior change when unused.
- Inline counter/histogram recording at each broker state transition (enqueue, dedup, claim, ack,
  retry, dead, reap, promote, latency).
- A pull-based `prometheus.Collector` in `internal/metrics` that reports per-queue depth gauges
  (`ready`/`inflight`/`delayed`/`dlq`) by running `ZCARD`/`LLEN` at scrape time.
- A `--metrics-addr` flag on `cmd/worker` that, when set, serves `/metrics`.

Out of scope: the Phase 3 API/dashboard server (`cmd/server`); a metrics endpoint on any process
other than `cmd/worker`; tracing; alerting rules; Grafana dashboards. Phase 2 has no other
remaining features — this slice completes it.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Instrumentation wiring | **Injected `Metrics` interface, no-op default** | Keeps the broker free of a hard prometheus dependency, allows isolated assertion with a fake recorder, and makes metrics purely additive (existing tests/usage untouched). |
| Push vs pull | **Both: counters/histogram pushed inline; depth gauges pulled at scrape** | Counters are event-driven; queue depths are point-in-time facts that should never go stale, so they are read from Redis when Prometheus scrapes. |
| Metric library | **`github.com/prometheus/client_golang`** | Correct exposition/histogram/escaping and a clean custom-collector API. It is an instrumentation library, not a queue library — the "build the queue from scratch" rule is untouched. |
| Latency definition | **End-to-end: `now − created_at`, observed in `Ack`** | Captures total time in system (queue wait + processing). Derivable entirely from the job hash; no new field or claim-time plumbing. |
| Labels | **`queue` on everything; `state` additionally on the depth gauge** | Bounded cardinality (number of queues), and the dimensions an operator actually slices by. |
| Endpoint | **`cmd/worker --metrics-addr`, empty = off** | No metrics process exists until Phase 3; the worker is the natural host. Off by default keeps current behavior byte-identical and avoids binding a port in CI. |
| Registry | **Dedicated `prometheus.Registry` per recorder** | No global mutable state; matches the project's emphasis on isolated, testable units. |

## Data model

No new Redis keys and no new job-hash fields. Depth gauges are computed from the existing per-queue
keys at scrape time:

| Gauge sample | Source |
|---|---|
| `relay_queue_depth{queue,state="ready"}` | `ZCARD q:{name}:ready` |
| `relay_queue_depth{queue,state="inflight"}` | `ZCARD q:{name}:inflight` |
| `relay_queue_depth{queue,state="delayed"}` | `ZCARD q:{name}:delayed` |
| `relay_queue_depth{queue,state="dlq"}` | `LLEN q:{name}:dlq` |

## Components & changes

### `internal/broker`

- **`Metrics` interface** (new file `metrics.go`):

  ```go
  type Metrics interface {
      IncEnqueued(queue string)
      IncDeduplicated(queue string)
      IncClaimed(queue string)
      IncProcessed(queue string)
      IncRetried(queue string)
      IncDead(queue string)
      AddReaped(queue string, n int)
      AddPromoted(queue string, n int)
      ObserveLatency(queue string, d time.Duration)
  }
  ```

- **`noopMetrics`** — unexported zero-cost implementation; every method empty. `Broker` gains a
  `metrics Metrics` field initialised to `noopMetrics{}` in `New`.
- **`WithMetrics(m Metrics) Option`** — overrides the recorder. A `nil` argument is ignored (keeps
  the no-op) so callers can't accidentally install a nil and panic.
- **Instrumentation points** (no Lua changes; all signals already available in Go):
  - `Enqueue`: on success → `IncEnqueued(j.Queue)`; on `ErrDuplicate` → `IncDeduplicated(j.Queue)`
    (recorded before the sentinel is returned).
  - `Claim`: when a job is returned (`ok == true`) → `IncClaimed(queue)`. Empty/rate-limited claims
    record nothing.
  - `Ack`: → `IncProcessed(j.Queue)` and `ObserveLatency(j.Queue, time.Since(j.CreatedAt))`.
  - `Nack`: capture `nack.lua`'s existing `"retry"`/`"dead"` return (currently discarded via
    `.Err()`) → `IncRetried` / `IncDead`. Still returns its current error contract.
  - `Reap`: when `n > 0` → `AddReaped(queue, n)`.
  - `Promote`: when `n > 0` → `AddPromoted(queue, n)`.

  Metrics are recorded only after the underlying Redis operation succeeds, so a failed op never
  inflates a counter.

### `internal/metrics` (new package)

Depends only on `prometheus/client_golang` and `redis/go-redis` (for the collector) — **not** on
`broker`. It satisfies `broker.Metrics` structurally.

- **`Recorder`** — implements `broker.Metrics` over a private `*prometheus.Registry`. Holds the
  counter vecs and histogram vec (all `*prometheus.CounterVec` / `*prometheus.HistogramVec` with a
  `queue` label). Constructor `NewRecorder() *Recorder` registers them on a fresh registry.
  `(*Recorder).Registry() *prometheus.Registry` exposes it for the HTTP handler.
- Metric definitions (namespace `relay`):

  | Metric | Type | Labels |
  |---|---|---|
  | `relay_jobs_enqueued_total` | counter | `queue` |
  | `relay_jobs_deduplicated_total` | counter | `queue` |
  | `relay_jobs_claimed_total` | counter | `queue` |
  | `relay_jobs_processed_total` | counter | `queue` |
  | `relay_jobs_retried_total` | counter | `queue` |
  | `relay_jobs_dead_total` | counter | `queue` |
  | `relay_jobs_reaped_total` | counter | `queue` |
  | `relay_jobs_promoted_total` | counter | `queue` |
  | `relay_job_latency_seconds` | histogram | `queue` |
  | `relay_queue_depth` | gauge (via collector) | `queue`, `state` |

  The latency histogram uses buckets spanning roughly 1ms→5min (end-to-end time is much wider than
  prometheus' default 5ms→10s), e.g. `prometheus.ExponentialBuckets(0.001, 3, 13)`.

- **`DepthCollector`** — implements `prometheus.Collector`. Constructed with the redis client and
  the queue name(s) to watch (`NewDepthCollector(rdb, queues...)`). `Describe` emits the
  `relay_queue_depth` descriptor; `Collect` runs `ZCARD`/`LLEN` per queue under a short
  scrape-scoped `context.Context` (a few seconds) and emits one gauge sample per (queue, state). On
  a Redis error for a given query it skips that sample rather than emitting a stale or zero value.
  Registered on the `Recorder`'s registry by the caller.

### `cmd/worker`

- New `--metrics-addr` flag (string, default `""`).
- When empty: unchanged — broker built without `WithMetrics`, no HTTP server.
- When set: build a `metrics.NewRecorder()`, pass `broker.WithMetrics(rec)`, register a
  `metrics.NewDepthCollector(rdb, *queue)` on `rec.Registry()`, and start an `http.Server` serving
  `promhttp.HandlerFor(rec.Registry(), promhttp.HandlerOpts{})` at `/metrics`. The server is shut
  down (graceful `Shutdown`) alongside the worker pool / reaper / promoter on SIGINT/SIGTERM.

## Testing

Real Redis where Redis is needed; skip (not fail) when unreachable. To keep `go test ./...`
parallel-safe, the metrics package uses **Redis DB 13** (broker tests use DB 15, worker tests DB
14, so no `FlushDB` collisions).

### `internal/broker`

A fake `Metrics` recorder (records calls/counts in memory) injected via `WithMetrics`, asserted
against real Redis ops:

- Enqueue (no key) → exactly one `IncEnqueued`, queue correct.
- Enqueue duplicate (same idempotency key within TTL) → one `IncEnqueued` then one
  `IncDeduplicated`; ready ZCard reflects only the first.
- Claim of a ready job → one `IncClaimed`; claim of an empty queue → no call.
- Ack → one `IncProcessed` and one `ObserveLatency` with a non-negative duration.
- Nack with retries left → one `IncRetried`, none `IncDead`; Nack with budget spent → one
  `IncDead`, none `IncRetried`.
- Reap of N expired → one `AddReaped(queue, N)`; Promote of N due → one `AddPromoted(queue, N)`.
- Default broker (no `WithMetrics`) and `WithMetrics(nil)` → no panic across a full
  enqueue/claim/ack cycle (noop path).

### `internal/metrics`

- Compile-time assertion `var _ broker.Metrics = (*Recorder)(nil)`.
- Using `prometheus/testutil`: after recording, assert counter/histogram sample values
  (`testutil.ToFloat64`, `CollectAndCount`) for representative metrics and the `queue` label.
- `DepthCollector` against real Redis (DB 13): seed known members into `ready`/`inflight`/`delayed`
  and items into `dlq`, then assert `relay_queue_depth{state=…}` matches via
  `testutil.CollectAndCompare` / `GatherAndCompare`.
- Empty/unknown queue → depth samples are `0` (or absent on Redis error); no panic.

### `cmd/worker`

Build/vet only (consistent with prior slices); no integration test for the HTTP server.

## Known limitations

- **Label cardinality is per queue.** One time series per metric per queue. Fine for the handful of
  queues this project targets; a deployment with unbounded queue names would need care.
- **Depth gauges cost a scrape-time round-trip.** Each scrape issues one Redis call per (queue,
  state). Cheap at this scale; a very high scrape frequency against many queues would add load.
- **Metrics are per-process.** Each worker process exposes its own counters; cluster-wide totals
  come from Prometheus aggregating across scraped targets, as usual. Depth gauges read shared Redis,
  so every worker reports the same depths — operators should aggregate with `max`/`avg`, not `sum`.
- **Endpoint only on `cmd/worker`.** The standalone API/dashboard server is Phase 3.

## Invariants preserved

- At-least-once delivery — metrics are read-only observation; no job movement changes.
- The atomic claim is sacred — no Lua script is modified; instrumentation is Go-side, after the
  atomic op succeeds.
- Crash safety via the reaper is untouched.
- Build the queue from scratch on Redis primitives — `prometheus/client_golang` instruments; it
  does not implement any queue mechanics.
