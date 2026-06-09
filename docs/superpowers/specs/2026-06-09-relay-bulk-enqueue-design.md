# Relay — Bulk Enqueue (visible traffic)

**Status:** Approved design · **Date:** 2026-06-09
**Parent:** Phase 3a HTTP API + 3b dashboard + 3c SDK specs
**Scope:** A cross-cutting enhancement: broker `EnqueueBulk`, a bulk HTTP endpoint, an SDK method, and a dashboard count control. Frontend deploys live via Railway `main` auto-deploy; backend is just more queue plumbing on the existing engine.

## Purpose

Single-job enqueues (one HTTP request per job) don't move the dashboard's depth/throughput charts
enough to see the system working. Add a **bulk enqueue** — one request creates many jobs — and a
**count field** on the dashboard's enqueue form, so a click produces a visible burst of traffic.

## Scope

In scope:

- `broker.EnqueueBulk` — pipeline N jobs onto a queue in one Redis round-trip.
- `POST /api/queues/{queue}/jobs/bulk` — `{count, payload, priority?, delay_ms?}` → `{enqueued, state}`.
- `client.EnqueueBulk` — SDK method for the bulk endpoint.
- Dashboard enqueue form: a **Count** field (default 1; >1 → bulk).

Out of scope: changing `cmd/demo` (stays single-enqueue; may adopt bulk later); a heterogeneous
array batch (`{jobs:[...]}`); per-item idempotency in bulk; payload templating/index interpolation.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Shape | **count + payload template** | Goal is visible load; a count maps to a single number field on the form and a `count` arg in the SDK. |
| Payload | **identical for all N (distinct by job id)** | `job.New` assigns a unique id per job, so jobs are distinct without mutating the payload — which keeps JSON/structured payloads valid. |
| Idempotency | **none in bulk** | Bulk is for volume; a shared idempotency key would dedup all N to one. The single endpoint keeps idempotency. |
| Broker impl | **Redis pipeline of `enqueue.lua`** | One round-trip for N jobs; reuses the existing atomic per-job script unchanged. |
| Cap | **1 ≤ count ≤ 10000** | Prevents accidental/abusive huge requests; plenty for a visible burst. |
| Trigger | **dashboard Count field + SDK** | One click on the form produces the burst; SDK exposes it programmatically. `cmd/demo` unchanged. |

## Components & changes

### `internal/broker` — `EnqueueBulk`

```go
// EnqueueBulk enqueues many jobs onto their queue in a single Redis pipeline and
// returns the number enqueued. It applies the shared delay/priority options to
// every job; it does NOT apply idempotency/dedup (bulk is for volume — jobs are
// distinct by id). All jobs are assumed to share one queue.
func (b *Broker) EnqueueBulk(ctx context.Context, jobs []job.Job, opts ...EnqueueOption) (int, error)
```

- Resolve the `enqueueConfig` from `opts` once (delay → ready-at score, priority → score), exactly as
  single `Enqueue` does, but with no dedup key.
- Open a `b.rdb.Pipeline()`, and for each job queue the same `enqueueScript.Run(...)` call that single
  `Enqueue` uses (same KEYS/ARGV minus the dedup path), writing the job hash + adding to ready/delayed.
- `pipe.Exec(ctx)`; on error wrap `%w`. Return `len(jobs)` on success.
- Call `b.metrics.IncEnqueued(queue)` once per job so `/metrics` reflects the true count.
- Empty `jobs` slice → return `(0, nil)` without touching Redis.

(The exact KEYS/ARGV mirror `Enqueue`; the implementation plan will copy them verbatim from the
current `Enqueue` so the pipelined calls are identical to the single path.)

### `internal/api` — bulk endpoint

- Route: `mux.HandleFunc("POST /api/queues/{queue}/jobs/bulk", a.enqueueBulk)`.
- Request: `type bulkEnqueueRequest struct { Count int; Payload string; Priority *int; DelayMs int64 }`
  (JSON `count`, `payload`, `priority`, `delay_ms`).
- Validate: `count >= 1 && count <= maxBulkCount` (`maxBulkCount = 10000`); else `400` with a clear
  message (`"count must be between 1 and 10000"`).
- Build `count` jobs: `j := job.New(queue, []byte(req.Payload))` in a loop; assemble `opts` from
  `DelayMs`/`Priority` (same mapping as single enqueue). Call `a.broker.EnqueueBulk(ctx, jobs, opts...)`.
- Response: `201 {"enqueued": <n>, "state": "ready"|"delayed"}` (`delayed` when `delay_ms>0`).
- Errors: broker error → log + `500`. (No `ErrDuplicate` path — bulk has no idempotency.)
- The existing `POST .../jobs` (single) is unchanged.

### `internal/client` — `EnqueueBulk`

```go
type BulkResult struct {
    Enqueued int    `json:"enqueued"`
    State    string `json:"state"`
}

// EnqueueBulk enqueues count copies of payload onto a queue in one request.
// WithDelay/WithPriority apply to all; WithIdempotencyKey is ignored (bulk has no dedup).
func (c *Client) EnqueueBulk(ctx context.Context, queue string, payload []byte, count int, opts ...EnqueueOption) (BulkResult, error)
```

- Builds the JSON body `{count, payload, delay_ms?, priority?}` from `count` + the existing
  `enqueueBody` option application (reusing `WithDelay`/`WithPriority`; ignore any idempotency key).
- `POST /api/queues/{queue}/jobs/bulk`; decode `BulkResult`. Non-2xx → `*APIError` (same `do` helper).

### Dashboard — Count field on the enqueue form

- `web/src/api.ts`: add `enqueueBulk(queue, body)` where body is `{count, payload, priority?, delay_ms?}`
  → POSTs to the bulk endpoint, returns `{enqueued, state}`.
- `web/src/lib/count.ts` (new, pure, tested): `clampCount(raw: string, max = 10000): number` — parses
  the field, floors to an integer, clamps to `[1, max]`, defaults `1` on non-numeric/empty.
- `web/src/components/EnqueueForm.tsx`: add a **Count** number input (default `1`). On submit:
  `clampCount(count)`; if `== 1` → existing `enqueue(...)` (single); if `> 1` → `enqueueBulk(...)`.
  Show the result (e.g. "enqueued N") and close, refreshing the dashboard as today.
- Rebuild + commit `web/dist`.

## Testing

- **broker (DB 15):** `EnqueueBulk` of N → `ready` ZCARD == N; with `WithDelay` → `delayed` ZCARD == N,
  ready 0; empty slice → 0, no writes; a metrics recorder sees N enqueued increments.
- **api (DB 12):** `POST .../jobs/bulk {count:50,payload}` → 201 `{enqueued:50,state:"ready"}` and
  `Stats.Ready == 50`; `count:0` and `count:10001` → 400; `delay_ms>0` → `state:"delayed"`.
- **client (DB 11):** hermetic httptest — request body carries `count` and decodes `BulkResult`;
  plus the wire-compat round-trip (`client.EnqueueBulk` → real api/broker → `Stats` shows N).
- **web:** `count.test.ts` for `clampCount` (parses, floors, clamps low/high, defaults on junk);
  `tsc`/`vitest`/`vite build` and committed-dist-in-sync all pass.
- No change to single enqueue, claim/ack/nack, reaper/promoter — the existing suite stays green.

## Invariants preserved

- At-least-once and the atomic claim are untouched. `enqueue.lua` is reused unchanged; bulk just
  pipelines it. No new Go dependency. The metric stays per-process and accurate (incremented per job).

## Known limitations

- **No per-job results from bulk.** The response is a count + state, not N ids (N can be large).
- **No idempotency in bulk.** Re-sending a bulk request enqueues another N (by design).
- **Count cap 10000 per request.** Larger bursts need multiple requests.
- **Bulk is one queue per request.** The endpoint is per-queue (path param), matching the single API.
