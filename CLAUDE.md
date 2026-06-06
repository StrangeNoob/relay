# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**Relay** — a portfolio-grade distributed task queue written in **Go**, with queue semantics
built **from scratch** on Redis primitives. It is a backend / distributed-systems showcase
project: the point is to *prove understanding of queue internals*, not to wrap an existing
library. Do not introduce a queue dependency (BullMQ, asynq, Machinery, Celery, etc.) — the
mechanics are the deliverable.

**Status: pre-implementation.** As of this writing the repo contains only the design spec and
this file. No Go code exists yet.

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

## Intended layout (per spec — create as you build)

```
cmd/{server,worker,demo}/main.go   # API+dashboard server, worker daemon, demo load generator
internal/broker/                   # core engine: enqueue/claim/ack/nack/reaper/promoter
internal/broker/scripts/*.lua      # atomic Lua scripts, embedded via go:embed
internal/{job,worker,client,api,metrics}/
web/                               # embedded dashboard assets (plain JS via go:embed, no SPA build)
deployments/docker-compose.yml     # redis + server + N workers + demo
.github/workflows/ci.yml
```

Use `internal/` for everything not meant as a public import surface. `cmd/` holds only thin
`main` wiring.

## Build order (do not jump ahead)

1. **Phase 1 — core:** job model; enqueue/claim/ack/nack Lua; reaper; worker runtime; basic DLQ; integration tests; CI. Ship a working, testable queue before anything else.
2. **Phase 2 — depth:** delayed jobs + promoter; priority; backoff + jitter; idempotency; per-queue rate limiting; Prometheus metrics.
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

## Build commands

No build tooling exists yet. Once `go.mod` is initialized, the expected commands are:

```sh
go build ./...
go test -race ./...
golangci-lint run
docker compose -f deployments/docker-compose.yml up   # full local stack + demo load
```

Keep this section updated as the Makefile / CI take shape.
