# Relay — Phase 3a: HTTP API + Server Foundation

**Status:** Approved design · **Date:** 2026-06-08
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 3 (polish) — first sub-project. Phase 3 decomposes into 3a (this), 3b live dashboard,
3c producer SDK, 3d packaging/deploy/README.

## Purpose

Give Relay an HTTP control surface and a long-running server process. Until now the only ways to
drive the queue are the Go broker API and the `cmd/worker`/`cmd/demo` binaries. This sub-project
adds a JSON REST API (`internal/api`) and an always-on server (`cmd/server`) that can enqueue jobs,
report per-queue stats, inspect the dead-letter queue, and requeue dead-lettered jobs — plus the
broker methods those need. It is the foundation the dashboard (3b) and producer SDK (3c) consume.

## Scope

In scope:

- New broker read/admin methods: `Stats`, `ListDLQ`, `RequeueDLQ` (with a new atomic `requeue.lua`),
  and `Queues` (discovery via `SCAN`).
- `internal/api`: a stdlib `net/http` handler exposing the REST endpoints below.
- `cmd/server`: wires Redis + broker + the API handler + `/metrics` + `/healthz`, with graceful
  shutdown. API enqueues are counted via a Phase 2 `metrics.Recorder`.

Out of scope: authentication/authorization (demo-grade); binary payloads (UTF-8 string payloads
only; base64 is a documented future option); cursor pagination (simple offset/limit over the DLQ
list); a rate-limit configuration API; the dashboard UI (3b); the producer SDK (3c); docker-compose
and deploy (3d).

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| HTTP stack | **stdlib `net/http` with 1.22 `ServeMux` pattern routing** | No new dependency (project ships only go-redis + prometheus); method+path patterns (`POST /api/queues/{queue}/jobs`) cover routing; fully testable with `httptest`. |
| API style | **JSON REST** | Simple, dashboard- and SDK-friendly, language-agnostic. |
| Requeue attempts | **Reset to 0** | A DLQ job exhausted its retry budget; a manual requeue is a deliberate "try again from scratch", so it gets a fresh budget. |
| Requeue atomicity | **New `requeue.lua`** (LREM + reset + ZADD ready) | The move out of the DLQ and back into ready must be one atomic step, consistent with the "every state transition is atomic Lua" invariant. |
| Ready score on requeue | **Recomputed in Lua** from the hash's priority + `now` + `priorityScale` | Matches how `promote.lua`/`reaper.lua` already rebuild ready scores; keeps priority ordering correct. |
| Queue discovery | **`Queues(ctx)` via `SCAN`** | The dashboard needs queue names without hardcoding; `SCAN` is non-blocking. |
| Metrics on server | **Mount `/metrics`; install a `Recorder` on the server's broker** | API enqueues get counted (`relay_jobs_enqueued_total`); a `DepthCollector` for the configured queues exposes gauges from the always-on server. Reuses Phase 2. |
| Payload encoding | **JSON string (UTF-8 → bytes)** | Friendly for the common case; binary via base64 is deferred. |

## Components & changes

### `internal/broker`

- `type Stats struct { Ready, Inflight, Delayed, DLQ int64 }`.
- `Stats(ctx context.Context, queue string) (Stats, error)` — one pipeline issuing `ZCARD` on
  ready/inflight/delayed and `LLEN` on dlq; maps results into `Stats`.
- `ListDLQ(ctx context.Context, queue string, limit, offset int64) ([]job.Job, error)` — `LRANGE`
  `q:{queue}:dlq` over `[offset, offset+limit)` to get ids, then `HGETALL` each into a `job.Job`
  via the existing `FromHash`. `limit <= 0` uses a default (e.g. 50); a hard max (e.g. 1000) caps
  it. An id whose hash is missing is skipped (it was already cleaned up).
- `RequeueDLQ(ctx context.Context, queue, id string) (bool, error)` — runs `requeue.lua`. Returns
  `(false, nil)` when the id was not present in the DLQ (so the API can answer `404`).
- `Queues(ctx context.Context) ([]string, error)` — `SCAN` (cursor loop, `MATCH q:*:*`,
  reasonable `COUNT`) collecting distinct queue names by stripping the `q:` prefix and the
  `:{suffix}` (ready/inflight/delayed/dlq/delayed/ratelimit/dedup:*) — parse the name as the segment
  between the first `q:` and the next `:`. Deduplicated, sorted for stable output.

### `internal/broker/scripts/requeue.lua` (new, `go:embed`)

```
-- KEYS[1] = dlq list   q:{name}:dlq
-- KEYS[2] = ready set   q:{name}:ready
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = now (unix ms)
-- ARGV[4] = priority scale (for the ready score)
-- Returns 1 if the job was requeued, 0 if it was not in the DLQ.

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
  return 0
end
local job_key = ARGV[2] .. ARGV[1]
local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
redis.call('HSET', job_key, 'state', 'ready', 'attempts', 0)
local score = priority * tonumber(ARGV[4]) - tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], score, ARGV[1])
return 1
```

### `internal/api`

`New(b *broker.Broker) http.Handler` returns a configured `*http.ServeMux`. Routes:

| Method + pattern | Handler behavior |
|---|---|
| `POST /api/queues/{queue}/jobs` | Decode `{payload string, delay_ms?: int, priority?: int, idempotency_key?: string}`. Build a `job.New(queue, []byte(payload))`; apply `WithDelay`/`WithPriority`/`WithIdempotencyKey` from the present fields; `Enqueue`. `201 {id, state}`; `broker.ErrDuplicate` → `409`. |
| `GET /api/queues/{queue}/stats` | `Stats` → `200 {ready, inflight, delayed, dlq}`. |
| `GET /api/queues/{queue}/dlq?limit=&offset=` | Parse/clamp `limit`/`offset`; `ListDLQ` → `200 [job…]`. |
| `POST /api/queues/{queue}/dlq/{id}/requeue` | `RequeueDLQ` → `200 {requeued:true}`; not found → `404`. |
| `GET /api/queues` | `Queues` → `200 [name…]`. |

Cross-cutting: a `writeJSON(w, status, v)` helper and a `writeError(w, status, msg)` helper that
emits `{ "error": "..." }`. Bad JSON or non-integer query params → `400`. Broker errors → `500`
(logged via the handler's `*slog.Logger`, injected so tests can silence it). A wrong method on a
known path yields `405` from `ServeMux` automatically. The job JSON shape exposes
`id, queue, payload, state, attempts, max_retries, priority, created_at, idempotency_key` (payload
rendered as a string).

### `cmd/server`

Flags: `-addr` (default `:8080`), `-redis` (default from `REDIS_ADDR` or `localhost:6379`),
`-queues` (comma-separated list for the depth collector; empty = none). Builds
`rec := metrics.NewRecorder()`, `b := broker.New(rdb, broker.WithMetrics(rec))`, registers
`metrics.NewDepthCollector(rdb, queues...)` on `rec.Registry()`. Mounts a top-level mux:
`/api/` → `api.New(b)`, `/metrics` → `promhttp.HandlerFor(rec.Registry(), …)`, `/healthz` → `200`.
Graceful shutdown on SIGINT/SIGTERM (`http.Server.Shutdown` with a timeout).

## Error handling

- `400` — malformed JSON body, missing/empty required fields, non-integer query params.
- `404` — requeue of an id not present in the queue's DLQ.
- `405` — wrong method on a defined path (stdlib `ServeMux`).
- `409` — enqueue rejected as an idempotency-key duplicate (`ErrDuplicate`).
- `500` — broker/Redis errors; logged, generic `{ "error": "internal error" }` to the client.
- All errors share the `{ "error": "..." }` envelope.

## Testing

Real Redis where needed; skip (not fail) when unreachable.

### `internal/broker` (DB 15)

- `Stats`: enqueue N to ready, claim some (inflight), delay some, nack some to DLQ; assert each count.
- `ListDLQ`: drive K jobs to the DLQ; assert ids/fields, ordering, and that `limit`/`offset` page
  correctly; assert a missing-hash id is skipped.
- `RequeueDLQ`: dead-letter a job; requeue it; assert it left the DLQ, is back in ready, `state=ready`,
  `attempts=0`; requeue of an unknown id returns `(false, nil)`. Atomicity exercised by the single
  script.
- `Queues`: seed several `q:*:*` keys; assert distinct, sorted names; empty Redis → empty slice.

### `internal/api` (dedicated test **DB 12** — broker uses 15, worker 14, metrics 13)

`httptest` against a real broker. End-to-end flows:

- `POST …/jobs` → `201`, body has an id; a follow-up `GET …/stats` shows `ready == 1`.
- `POST …/jobs` twice with the same `idempotency_key` → second is `409`.
- Drive a job to the DLQ (enqueue→claim→nack with max retries 0), `GET …/dlq` lists it, then
  `POST …/dlq/{id}/requeue` → `200`, and `GET …/stats` shows it back in `ready`, dlq `0`.
- `POST …/dlq/{unknown}/requeue` → `404`.
- `GET /api/queues` lists the queue used.
- Malformed JSON body → `400`.

### `cmd/server`

Build/vet only (consistent with prior phases).

## Known limitations

- **No authentication.** The API is open; intended for the demo/dashboard, not a hostile network.
  A future hardening step would add an auth middleware.
- **UTF-8 string payloads.** The JSON `payload` is treated as a UTF-8 string; binary payloads would
  need a base64 field (deferred).
- **Offset/limit DLQ paging over a Redis list.** Simple and fine at demo scale; very large DLQs
  would want cursoring. `LRANGE` is O(offset+limit).
- **`Queues` uses `SCAN`.** Eventually-consistent and unordered at the Redis level; results are
  deduped and sorted in Go. On a huge keyspace `SCAN` still iterates everything (bounded work per
  call, multiple round-trips).
- **Server depth gauges use the `-queues` flag.** The `/metrics` depth collector reports only the
  queues passed at startup; it does not auto-discover. (Discovery exists for the API but is not wired
  into the collector in 3a.)

## Invariants preserved

- At-least-once delivery — the API is a control surface; `RequeueDLQ` is an explicit operator action
  that moves a job dlq→ready atomically, consistent with the existing transition model.
- The atomic claim is sacred — unchanged; the new `requeue.lua` follows the same one-script-per-
  transition rule.
- Crash safety via the reaper — untouched.
- Build the queue from scratch on Redis primitives — the API adds no queue library; routing is
  stdlib `net/http`.
