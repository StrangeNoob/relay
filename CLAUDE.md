# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**Relay** — a portfolio-grade distributed task queue written in **Go**, with queue semantics
built **from scratch** on Redis primitives. It is a backend / distributed-systems showcase
project: the point is to *prove understanding of queue internals*, not to wrap an existing
library. Do not introduce a queue dependency (BullMQ, asynq, Machinery, Celery, etc.) — the
mechanics are the deliverable.

**Status: Phase 1 complete; Phase 2 complete.** The core engine plus delayed jobs, the promoter,
retry backoff, priority, idempotency enforcement, per-queue rate limiting, and Prometheus metrics
are built, tested against a real Redis under `-race`, and CI is green. Phase 3 (API/dashboard) is
next. Repo: <https://github.com/StrangeNoob/relay>. What exists today:

- `internal/job` — the `Job` model + Redis-hash encoding (`ToHash`/`FromHash`).
- `internal/broker` — `Enqueue` (with `WithDelay`/`WithReadyAt`/`WithPriority`/`WithIdempotencyKey` options), atomic `Claim`, `Ack`,
  `Nack` (full-jitter backoff via the delayed set), `Reap`, `Promote`, `Extend` (heartbeat), with
  Lua under `internal/broker/scripts/`: `enqueue.lua`, `claim.lua`, `ack.lua`, `nack.lua`,
  `reaper.lua`, `promote.lua`, `heartbeat.lua`. Broker options: `WithBackoff`, `WithDedupTTL`,
  `WithRateLimit(queue, rate, burst)` (token-bucket per-queue rate limiting via Redis hash),
  `WithMetrics(m)` (installs a `broker.Metrics` implementation; default is a no-op).
- `internal/metrics` — Prometheus `Recorder` (implements `broker.Metrics`; counters
  `relay_jobs_*_total`, histogram `relay_job_latency_seconds`, all labelled by queue) and
  `DepthCollector` (a `prometheus.Collector` reporting `relay_queue_depth{queue,state}` gauges
  by reading ZCARD/LLEN at scrape time).
- `internal/worker` — `Worker` (claim loop, dispatch, heartbeat, graceful shutdown), plus `Reaper`
  and `Promoter` background loops sharing one `runDrainLoop` helper.
- `cmd/worker`, `cmd/demo` — thin runnable entrypoints (worker pool + reaper + promoter; load
  generator with `--delay`). `cmd/worker` accepts `--metrics-addr` (default "" = off); when set,
  serves `/metrics` and registers the depth collector with graceful shutdown.
- `.github/workflows/ci.yml` — Redis service + `go test -race` + `golangci-lint`.

Phase 3 (API/dashboard/`cmd/server`, docker-compose, deploy) is **not** built yet.

## Source of truth

The approved design lives in
[`docs/superpowers/specs/2026-06-07-relay-distributed-task-queue-design.md`](docs/superpowers/specs/2026-06-07-relay-distributed-task-queue-design.md).
**Read it before implementing anything.** It is the authoritative description of architecture,
components, the Redis data model, delivery semantics, and the phased build order. If code and
spec disagree, the spec wins until the spec is deliberately updated.

## Core invariants (do not violate without updating the spec)

- **Delivery is at-least-once, never exactly-once.** Idempotency keys let consumers dedup; do
  not add code or README claims implying exactly-once.
- **The atomic claim is sacred.** Claiming a job (pop from `ready` → add to `inflight` with a
  visibility deadline → bump attempts) MUST be a single atomic Redis operation (Lua script).
  Splitting it into multiple round-trips breaks competing-consumer safety. This is the heart of
  the project.
- **Crash safety comes from the reaper.** A worker dying mid-job is recovered only because the
  visibility deadline expires and the reaper requeues it. Any change to claim/heartbeat/ack must
  preserve this path.
- **Build from scratch on Redis primitives.** Redis is the durable substrate; the queue logic is
  ours.

## Known limitations (intentional, documented)

- **No fencing token on ack/nack/promote.** `Ack`/`Nack`/reap/promote act on a bare job id. If a
  worker stalls past its visibility deadline, gets reaped and re-claimed elsewhere, the original
  worker's late ack/nack can disturb the new claim; likewise a backoff retry trusts the `attempts`
  recorded on the hash, so a reclaim race could miscount. Consistent with at-least-once delivery;
  per-claim fencing tokens are possible future hardening, not a current guarantee.
- **Backoff is full-jitter, computed in Go.** `Nack` computes the retry delay (`random(0,
  min(cap, base·2^(n-1)))`) under a mutex-guarded rand and passes the ready-at into `nack.lua`;
  the script only decides retry-vs-dead and moves the job. Defaults: base 1s, cap 10m
  (`broker.WithBackoff`).
- **Idempotency is enqueue-only, TTL-window.** A keyed duplicate is dropped within the dedup TTL (default 24h, `WithDedupTTL`); the key is not released on completion. Delivery remains at-least-once — consumers needing exactly-once effects still dedup on the key.
- **Rate-limit config is per-worker, not stored in Redis.** All workers on a queue must register the same `WithRateLimit` (they share one Redis bucket and pass rate/burst on every claim); mismatched configs refill inconsistently. A rate-limited claim is indistinguishable from an empty queue to the worker (it polls again).
- **Metrics are per-process and opt-in.** `broker.WithMetrics` installs a Prometheus recorder (default is a no-op); `cmd/worker --metrics-addr` serves `/metrics`. Counters/latency are per worker process — aggregate across workers in Prometheus. Queue-depth gauges read shared Redis at scrape time (one round-trip per queue/state), so every worker reports the same depths (aggregate with max/avg, not sum). Label cardinality is per queue. The endpoint lives on `cmd/worker` until the Phase 3 server exists.

## Redis data model & job lifecycle (the architecture in brief)

A job is one Redis hash; its *position in the queue* is membership in one of several per-queue
ZSETs, keyed by `q:{name}:`. The ZSET **score is the mechanism** in each case — read the scores,
and the whole engine follows:

| Key | Type | Score = | Role |
|---|---|---|---|
| `job:{id}` | hash | — | full job. Fields: `id, queue, payload, state, attempts, max_retries, created_at, idempotency_key`. **No deadline field** — the deadline lives only as the `inflight` ZSET score. |
| `q:{name}:ready` | ZSET | priority | claimable now; claim pops the best score = priority (higher first), oldest-first within a priority |
| `q:{name}:inflight` | ZSET | visibility deadline | claimed-not-acked; **reaper scans this for expiry** |
| `q:{name}:dlq` | list | — | exhausted jobs (inspect/requeue surface is Phase 3) |
| `q:{name}:delayed` | ZSET | ready-at ts | scheduled + backoff jobs; **promoter scans this** and moves due ones (`ready-at ≤ now`) to `ready` |
| `q:{name}:dedup:{key}` | string | — | per-key string with TTL; **enqueue dedup** — a keyed duplicate is dropped with ErrDuplicate |
| `q:{name}:ratelimit` | hash | — | per-queue token bucket (`tokens`, `ts`); claim consumes a token only on a successful pop |

States in use today: `pending` (constructed, not enqueued), `ready`, `inflight`, `delayed`
(scheduled or waiting out a backoff), `dead`.

Lifecycle (each arrow that must stay atomic is a Lua script — see invariants):

```
enqueue            → ready
enqueue(WithDelay) → delayed ──[promoter: ready-at≤now]──→ ready
  → [CLAIM: ready→inflight, deadline=now+vt, attempts++] → process
  ack   → remove from inflight + delete job
  nack  → attempts<maxRetries ? delayed (now + full-jitter backoff) : dlq
  reaper   (bg): inflight where deadline<=now → ready  # at-least-once on crash
  promoter (bg): delayed  where ready-at<=now  → ready  # releases scheduled + backed-off jobs
```

Two background loops (reaper, promoter) plus the worker claim loop are the only things that
move jobs between states. Heartbeat (`broker.Extend`, `ZADD XX`) pushes a job's `inflight`
deadline forward while a long handler runs, so the reaper does not reclaim live work.

## Layout (✅ built · ◻ planned)

```
cmd/worker/main.go                 # ✅ worker pool + reaper + promoter daemon
cmd/demo/main.go                   # ✅ load generator (--delay)
cmd/server/main.go                 # ◻ API+dashboard server (Phase 3)
internal/job/                      # ✅ job model + hash encoding
internal/broker/                   # ✅ enqueue/claim/ack/nack/reap/promote/extend
internal/broker/scripts/*.lua      # ✅ enqueue, claim, ack, nack, reaper, promote, heartbeat (go:embed)
internal/worker/                   # ✅ Worker + Reaper + Promoter runtime
internal/metrics/                  # ✅ Prometheus Recorder + DepthCollector
internal/{client,api}/             # ◻ producer SDK / HTTP API (Phase 3)
web/                               # ◻ embedded dashboard assets (Phase 3)
deployments/docker-compose.yml     # ◻ redis + server + N workers + demo (Phase 3)
.github/workflows/ci.yml           # ✅ Redis service + go test -race + golangci-lint
```

Use `internal/` for everything not meant as a public import surface. `cmd/` holds only thin
`main` wiring — the demo handler in `cmd/worker` is scaffolding, not core logic.

## Build order (do not jump ahead)

1. **Phase 1 — core: ✅ done.** job model; enqueue/claim/ack/nack Lua; reaper; worker runtime; basic DLQ; integration tests; CI. A working, testable queue ships first.
2. **Phase 2 — depth: ✅ done.** delayed jobs + promoter ✅; backoff + jitter ✅; priority ✅; idempotency ✅; rate limiting ✅; Prometheus metrics ✅.
3. **Phase 3 — polish:** dashboard; docker-compose demo; deployed demo; README + diagram.
4. **Future work (NOT now):** Postgres-backed (`SKIP LOCKED`) mode; exactly-once via consumer outbox.

## Conventions

- **Language:** Go. Code is kept well-commented because it doubles as a Go-learning artifact for
  the author — explain non-obvious idioms rather than assuming fluency.
- **Tests:** every `internal/` package is independently testable. Broker/Lua tests run against a
  real Redis (CI service container), not a mock, so atomicity is actually exercised. Run
  `go test -race` for the concurrency suite.
- **Lua scripts** live as `.lua` files under `internal/broker/scripts/` and are embedded with
  `go:embed` — do not inline Lua as Go string literals.
- **Errors:** wrap with `%w`; never silently swallow. Worker shutdown is graceful (stop claiming,
  finish or nack in-flight, exit clean).

## Build & dependencies

- **Module:** `github.com/StrangeNoob/relay`.
- **Toolchain:** `go 1.24` with `toolchain go1.25.11` pinned in `go.mod` (go-redis v9 needs ≥1.24).
  If a `go1.24` toolchain download fails, the pin makes the build use the already-cached 1.25.x.
- **Direct dependencies (two):**
  - `github.com/redis/go-redis/v9` — a Redis *driver*, not a queue library.
  - `github.com/prometheus/client_golang` — a metrics instrumentation library, not a queue library.
  Neither violates the "build the queue from scratch on Redis" rule. The queue logic is ours.
- **Tests need a real Redis** at `localhost:6379` (override with `REDIS_ADDR`). Each Redis-using
  package claims its own logical DB so `go test ./...` runs them in parallel without flushing each
  other (broker → **DB 15**, worker → **DB 14**, metrics → **DB 13**; a new one picks another), with
  `FlushDB` per test, and they **skip** (not fail) when Redis is unreachable — so a green local run
  with no Redis means those suites were skipped. CI provides a Redis service.

```sh
go build ./...
go test -race ./...                         # needs Redis on :6379 (or REDIS_ADDR)
golangci-lint run                            # CI pins v2.12.2; default linters, currently clean

# run it end to end against a local Redis:
go run ./cmd/worker -queue demo -concurrency 4 &   # worker pool + reaper
go run ./cmd/demo   -queue demo -count 100         # enqueue load
```

Keep this section updated as the Makefile / docker-compose take shape.
