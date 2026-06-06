# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**Relay** — a portfolio-grade distributed task queue written in **Go**, with queue semantics
built **from scratch** on Redis primitives. It is a backend / distributed-systems showcase
project: the point is to *prove understanding of queue internals*, not to wrap an existing
library. Do not introduce a queue dependency (BullMQ, asynq, Machinery, Celery, etc.) — the
mechanics are the deliverable.

**Status: Phase 1 complete.** The core engine is built, tested against a real Redis under
`-race`, and CI is green. Repo: <https://github.com/StrangeNoob/relay>. What exists today:

- `internal/job` — the `Job` model + Redis-hash encoding (`ToHash`/`FromHash`).
- `internal/broker` — `Enqueue`, atomic `Claim`, `Ack`, `Nack`, `Reap`, `Extend` (heartbeat),
  with Lua under `internal/broker/scripts/`: `claim.lua`, `ack.lua`, `nack.lua`, `reaper.lua`,
  `heartbeat.lua`.
- `internal/worker` — `Worker` (claim loop, dispatch, heartbeat, graceful shutdown) and `Reaper`
  (background reap loop).
- `cmd/worker`, `cmd/demo` — thin runnable entrypoints (worker pool + reaper; load generator).
- `.github/workflows/ci.yml` — Redis service + `go test -race` + `golangci-lint`.

Phase 2 (delayed jobs/promoter, priority, backoff+jitter, idempotency enforcement, rate limiting,
metrics) and Phase 3 (API/dashboard/`cmd/server`, docker-compose, deploy) are **not** built yet.

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

- **No fencing token on ack/nack.** `Ack`/`Nack`/reap act on a bare job id. If a worker stalls
  past its visibility deadline, gets reaped and re-claimed elsewhere, the original worker's late
  ack/nack can disturb the new claim. This is consistent with at-least-once delivery; per-claim
  fencing tokens are possible future hardening, not a current guarantee.
- **`nack` has no backoff yet** (requeues immediately to `ready`) — see lifecycle below.

## Redis data model & job lifecycle (the architecture in brief)

A job is one Redis hash; its *position in the queue* is membership in one of several per-queue
ZSETs, keyed by `q:{name}:`. The ZSET **score is the mechanism** in each case — read the scores,
and the whole engine follows:

| Key | Type | Score = | Role |
|---|---|---|---|
| `job:{id}` | hash | — | full job. Fields: `id, queue, payload, state, attempts, max_retries, created_at, idempotency_key`. **No deadline field** — the deadline lives only as the `inflight` ZSET score. |
| `q:{name}:ready` | ZSET | priority | claimable now; claim pops the best score (currently score 0 for all — priority is Phase 2) |
| `q:{name}:inflight` | ZSET | visibility deadline | claimed-not-acked; **reaper scans this for expiry** |
| `q:{name}:dlq` | list | — | exhausted jobs (inspect/requeue surface is Phase 3) |
| `q:{name}:delayed` | ZSET | ready-at ts | **planned (Phase 2)** — scheduled + backoff jobs; promoter moves due ones to `ready` |
| `q:{name}:dedup` | set/hash | — | **planned (Phase 2)** — idempotency keys for enqueue dedup |

States in use today: `pending` (constructed, not enqueued), `ready`, `inflight`, `dead`. The
`delayed` state arrives with Phase 2.

Lifecycle (each arrow that must stay atomic is a Lua script — see invariants):

```
enqueue → ready → [CLAIM: ready→inflight, deadline=now+vt, attempts++] → process
  ack   → remove from inflight + delete job
  nack  → attempts<maxRetries ? ready : dlq        # backoff via `delayed` is Phase 2
  reaper (bg): inflight where deadline<=now → ready # at-least-once on crash
```

Current reality vs. spec target: `nack` requeues straight to `ready` with no delay — exponential
backoff + jitter (re-queue via `delayed`) and the **promoter** loop are Phase 2. Today the only
background loop is the reaper; together with the worker claim loop they are the only things that
move jobs between states. Heartbeat (`broker.Extend`, `ZADD XX`) pushes a job's `inflight`
deadline forward while a long handler runs, so the reaper does not reclaim live work.

## Layout (✅ built · ◻ planned)

```
cmd/worker/main.go                 # ✅ worker pool + reaper daemon
cmd/demo/main.go                   # ✅ load generator
cmd/server/main.go                 # ◻ API+dashboard server (Phase 3)
internal/job/                      # ✅ job model + hash encoding
internal/broker/                   # ✅ enqueue/claim/ack/nack/reap/extend  (◻ promoter — Phase 2)
internal/broker/scripts/*.lua      # ✅ claim, ack, nack, reaper, heartbeat (embedded via go:embed)
internal/worker/                   # ✅ Worker + Reaper runtime
internal/{client,api,metrics}/     # ◻ producer SDK / HTTP API / Prometheus (Phase 2–3)
web/                               # ◻ embedded dashboard assets (Phase 3)
deployments/docker-compose.yml     # ◻ redis + server + N workers + demo (Phase 3)
.github/workflows/ci.yml           # ✅ Redis service + go test -race + golangci-lint
```

Use `internal/` for everything not meant as a public import surface. `cmd/` holds only thin
`main` wiring — the demo handler in `cmd/worker` is scaffolding, not core logic.

## Build order (do not jump ahead)

1. **Phase 1 — core: ✅ done.** job model; enqueue/claim/ack/nack Lua; reaper; worker runtime; basic DLQ; integration tests; CI. A working, testable queue ships first.
2. **Phase 2 — depth (next):** delayed jobs + promoter; priority; backoff + jitter; idempotency; per-queue rate limiting; Prometheus metrics.
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
- **Only dependency:** `github.com/redis/go-redis/v9` — a Redis *driver*, not a queue library; it
  does not violate the "no queue dependency" rule. The queue logic is ours.
- **Tests need a real Redis** at `localhost:6379` (override with `REDIS_ADDR`). They use **DB 15**
  and `FlushDB` per test, and **skip** (not fail) when Redis is unreachable — so a green local run
  with no Redis means the broker/worker suites were skipped. CI provides a Redis service.

```sh
go build ./...
go test -race ./...                         # needs Redis on :6379 (or REDIS_ADDR)
golangci-lint run                            # CI pins v2.12.2; default linters, currently clean

# run it end to end against a local Redis:
go run ./cmd/worker -queue demo -concurrency 4 &   # worker pool + reaper
go run ./cmd/demo   -queue demo -count 100         # enqueue load
```

Keep this section updated as the Makefile / docker-compose take shape.
