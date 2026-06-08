# Relay — Phase 3c: Producer SDK (`internal/client`)

**Status:** Approved design · **Date:** 2026-06-09
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Depends on:** [`2026-06-08-relay-phase3a-http-api-design.md`](2026-06-08-relay-phase3a-http-api-design.md)
**Phase:** 3 (polish) — third sub-project (3a HTTP API ✅, 3b dashboard ✅, 3c this, 3d packaging/deploy/README).

## Purpose

Give producers a clean Go way to drive Relay without touching Redis: a small HTTP client for the
3a API. It wraps enqueue (with delay/priority/idempotency) plus the read/admin operations (stats,
DLQ inspect, requeue, queue discovery), and `cmd/demo` is refactored to use it — proving the SDK
end-to-end and establishing the correct producer topology (producer → HTTP API → broker) for the
3d docker-compose demo.

## Scope

In scope:

- `internal/client`: a self-contained HTTP client for the API (`Enqueue`, `Stats`, `ListDLQ`,
  `Requeue`, `Queues`) with functional options, typed errors, and its own DTOs.
- Refactor `cmd/demo` to enqueue through the SDK over HTTP instead of the broker over Redis.

Out of scope: client-side retries/backoff (one request per call; callers retry); authentication;
SSE/stream consumption; publishing the SDK as an external (non-`internal/`) module; a connection
pool beyond what `net/http` provides.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Surface | **Full thin API client** (enqueue + stats + dlq + requeue + queues) | The read/admin ops are trivial HTTP wrappers; a complete, reusable Go SDK costs little more and is a stronger artifact. Enqueue is the headline. |
| Coupling | **Self-contained DTOs; no `broker`/Redis import** | A producer importing the SDK must not transitively pull in the queue internals or go-redis. The client decodes the API's `jobView`/stats JSON into its own types. |
| Transport | **stdlib `net/http`** | No new dependency; the API is plain JSON REST. |
| Errors | **Sentinels `ErrDuplicate` (409) / `ErrNotFound` (404); else `*APIError{Status,Message}`** | Lets callers branch on the common cases with `errors.Is`, and surfaces the server's `{"error":…}` envelope for the rest. |
| Demo wiring | **`cmd/demo` becomes an SDK consumer over HTTP** | Proves the SDK and matches the real producer topology; the demo needs `cmd/server` running (accepted tradeoff). |
| Config | **`New(baseURL, ...Option)` with `WithHTTPClient`/`WithTimeout`** | Sane default `*http.Client` (~10s timeout); injectable for tests and tuning. |

## Components & changes

### `internal/client` (new package)

Imports only the stdlib (`context`, `net/http`, `encoding/json`, `errors`, `fmt`, `time`,
`net/url`, `strconv`). No `internal/broker`, no `internal/job`, no go-redis.

- **`Client`** + `New(baseURL string, opts ...Option) *Client`. Stores the trimmed base URL and an
  `*http.Client`. `Option`s: `WithHTTPClient(*http.Client)`, `WithTimeout(time.Duration)`. Default
  client has a ~10s timeout.
- **DTOs** (JSON tags match the server exactly):
  - `EnqueueResult{ ID string \`json:"id"\`; State string \`json:"state"\` }`
  - `Stats{ Ready, Inflight, Delayed, DLQ int64 }` (tags `ready/inflight/delayed/dlq`)
  - `Job{ ID, Queue, Payload, State string; Attempts, MaxRetries, Priority int; CreatedAt string;
    IdempotencyKey string }` (tags matching the API `jobView`: `id/queue/payload/state/attempts/
    max_retries/priority/created_at/idempotency_key`). `Payload`/`CreatedAt` are strings (the API
    renders them so).
- **Methods** (all take `ctx` first):
  - `Enqueue(ctx, queue string, payload []byte, opts ...EnqueueOption) (EnqueueResult, error)` —
    builds the JSON body `{payload, delay_ms?, priority?, idempotency_key?}` from the options;
    `POST {base}/api/queues/{queue}/jobs`; `201` → decode `EnqueueResult`; `409` → `ErrDuplicate`.
    `EnqueueOption`s: `WithDelay(time.Duration)` (→ `delay_ms`), `WithPriority(int)`,
    `WithIdempotencyKey(string)`. Only set fields are sent (priority is sent only when the option is
    given, so a deliberate priority 0 is distinguishable from unset, mirroring the API's `*int`).
  - `Stats(ctx, queue string) (Stats, error)` — `GET {base}/api/queues/{queue}/stats`.
  - `ListDLQ(ctx, queue string, limit, offset int) ([]Job, error)` — `GET …/dlq?limit=&offset=`.
  - `Requeue(ctx, queue, id string) error` — `POST …/dlq/{id}/requeue`; `404` → `ErrNotFound`.
  - `Queues(ctx) ([]string, error)` — `GET {base}/api/queues`.
  - Path segments are escaped with `url.PathEscape`.
- **Errors**: `var ErrDuplicate = errors.New(...)`, `var ErrNotFound = errors.New(...)`; a
  `type APIError struct { Status int; Message string }` implementing `error` (`Error()` →
  `"relay api: <status> <message>"`). A shared `do(...)` helper performs the request, maps `409`/
  `404` to the sentinels on the relevant calls, decodes the `{"error":…}` envelope into `*APIError`
  for other non-2xx, and wraps transport errors with `%w`.

### `cmd/demo` (refactor)

- Flags: drop `-redis`; add `-server` (default `http://localhost:8080`). Keep `-queue`, `-count`,
  `-delay`, `-priority`, `-idempotency-key`.
- Build `c := client.New(*server)`. For each of `count` jobs, call
  `c.Enqueue(ctx, *queue, []byte(payload), opts...)` mapping the flags to `WithDelay`/`WithPriority`/
  `WithIdempotencyKey`; treat `client.ErrDuplicate` as a benign logged drop; any other error exits
  non-zero. No `broker`/`job`/`redis` imports remain.

## Testing

### `internal/client`

- **Hermetic tests (no Redis):** drive each method against an `httptest.NewServer` with canned
  handlers. Assert the request method, path, query, and JSON body the client sends, the decoding of
  responses, and error mapping: `409 → ErrDuplicate` (Enqueue), `404 → ErrNotFound` (Requeue), a
  `500` with `{"error":"boom"}` → `*APIError{Status:500, Message:"boom"}`, and a transport failure
  wrapped. Verify `WithPriority(0)` sends `priority:0` while an omitted priority sends no priority
  field.
- **Wire-compat round-trip (real Redis, new DB 11):** stand up `api.New(broker.New(rdb))` behind an
  `httptest.NewServer`, point a `client.New(srv.URL)` at it, then `Enqueue` a job and assert
  `Stats` shows `ready==1` and `Queues` lists the queue; drive a job to the DLQ (enqueue→the worker
  path isn't available here, so seed the DLQ directly via rdb or via the broker) and assert
  `ListDLQ` + `Requeue` round-trip. This proves the client DTOs match the live server JSON. Uses a
  dedicated **DB 11** (broker 15 / worker 14 / metrics 13 / api 12 / client 11) so `go test ./...`
  stays parallel-safe; skip when Redis is unreachable.

### `cmd/demo`

Build/vet only (consistent with prior phases).

## Invariants preserved

- At-least-once delivery and the atomic claim are untouched — the SDK is a thin HTTP client over the
  existing API; it adds no queue logic.
- Build from scratch on Redis primitives — the SDK adds no Go dependency (stdlib only); the Go
  module still depends only on go-redis + prometheus.

## Known limitations

- **No client-side retries.** Each method makes one HTTP request; transient failures surface to the
  caller, who decides whether to retry. (At-least-once still holds at the queue level.)
- **No auth.** Matches the demo-grade API.
- **`cmd/demo` requires a running server.** It no longer talks to Redis directly; the produce path
  is producer → API → broker.
- **DTOs are hand-mirrored from the API JSON.** The round-trip test guards against drift, but a
  server JSON change still requires a matching client DTO change.
