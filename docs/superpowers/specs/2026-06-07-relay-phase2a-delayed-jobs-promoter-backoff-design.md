# Relay — Phase 2a: Delayed Jobs, Promoter, and Backoff

**Status:** Approved design · **Date:** 2026-06-07
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 2 (depth) — first sub-project

## Purpose

Add scheduled/delayed jobs and turn `nack` into a real retry-with-backoff path. Today a failed
job is requeued straight to `ready` with no delay; this is the largest gap between the code and
the parent spec's lifecycle. This sub-project introduces the `delayed` holding area and the
**promoter** that releases jobs from it, then routes both scheduled enqueues and backoff retries
through it.

The remaining Phase 2 features (priority, idempotency enforcement, rate limiting, metrics) are
separate sub-projects and out of scope here.

## Scope

In scope:

- Delayed/scheduled enqueue via functional options.
- A `delayed` ZSET (score = ready-at) and a per-queue **promoter** loop that moves due jobs to
  `ready`.
- `nack` reworked to requeue retries to `delayed` with exponential backoff + full jitter; the DLQ
  path on exhaustion is unchanged.
- A new job state `delayed`.

Out of scope (later Phase 2 sub-projects): priority, idempotency enforcement, rate limiting,
Prometheus metrics. The promoter therefore enqueues to `ready` at score `0`, the same as today's
enqueue; priority scoring arrives with the priority sub-project.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Delayed-enqueue API | **Functional options** on `Enqueue` (`WithDelay`, `WithReadyAt`) | One method; priority/idempotency become options later too; backward-compatible variadic. |
| Backoff strategy | **Full jitter**: `delay = random(0, min(cap, base·2^(attempts-1)))` | AWS-recommended; maximally spreads synchronized retries (avoids thundering herd). |
| Where the delay is computed | **In Go** (passed to Lua as a ready-at timestamp) | Jitter needs randomness; keeping it in Go stays deterministic/testable and avoids `math.random` in Lua. |
| Retry-vs-dead decision | **In `nack.lua`**, read from the job hash | Keeps the decision atomic with the move; hash attempts/max are authoritative. |
| Promoter shape | New `worker.Promoter`, **mirroring `Reaper`** | Same tick→drain→sleep lifecycle as the reaper; per-queue, batch-bounded, drain-per-tick. |
| Loop duplication | **Extract a shared loop helper** in `internal/worker` | Reaper and Promoter share an identical lifecycle; each supplies a one-pass drain func. |
| Backoff math location | **Pure `nextBackoff` function** in `internal/broker` | Unit-testable deterministically with an injected rand source; no test-only knobs in the public API. |

## Data model

Adds one key and one state to the existing model:

- `q:{name}:delayed` — ZSET, **score = ready-at (unix milliseconds)**. Holds both scheduled jobs
  (enqueued with a delay) and jobs waiting out a backoff after a failed attempt.
- Job state **`delayed`** (`job.StateDelayed = "delayed"`).

No new `job:{id}` hash fields. Ready-at lives only as the `delayed` ZSET score, the same way the
visibility deadline lives only as the `inflight` ZSET score.

## Lifecycle (updated)

```
enqueue              → ready
enqueue(WithDelay d) → delayed ──[promoter: ready-at ≤ now]──→ ready
   ... → ready → claim → inflight → process
   ack                    → remove from inflight + delete job
   nack, attempts < max   → delayed (score = now + full-jitter backoff) ──promoter──→ ready
   nack, attempts ≥ max   → dlq
   reaper   (bg): inflight where deadline ≤ now → ready
   promoter (bg): delayed  where ready-at ≤ now → ready
```

Two background loops now run per queue: the reaper (recovers crashed in-flight work) and the
promoter (releases due delayed work). They are structurally identical; only their source set and
score meaning differ.

## Components & changes

### `internal/job`

- Add `StateDelayed State = "delayed"`.

### `internal/broker`

- **`Enqueue(ctx, j, opts ...EnqueueOption)`** — variadic options:
  - `WithDelay(d time.Duration)` and `WithReadyAt(t time.Time)` set a ready-at.
  - When a ready-at **in the future** is set: one `TxPipeline` writes the job hash with
    `state=delayed` and `ZADD q:{q}:delayed <readyAt-ms> id`.
  - Otherwise (no option, or a ready-at that is already at/past now): the current ready path, now
    setting `state=ready` explicitly on the hash. (Today an enqueued-but-unclaimed job's hash
    still reads `pending`; this corrects that latent inaccuracy.) A non-future `WithReadyAt`/
    `WithDelay(0)` is therefore equivalent to a plain enqueue — no job sits in `delayed` already
    due.
- **`Promote(ctx, queue) (int, error)`** — one bounded pass via `promote.lua`; returns the number
  of jobs promoted. Batch size reuses the existing reaper batch constant pattern.
- **`Nack(ctx, j)`** reworked:
  - Compute `readyAt = now + nextBackoff(j.Attempts, base, cap, rand)`.
  - Call `nack.lua` with keys `{inflight, delayed, dlq}` and ARGV `{id, jobKeyPrefix, readyAt-ms}`.
- **`nextBackoff(attempts int, base, cap time.Duration, r *rand.Rand) time.Duration`** — pure
  function: `exp = min(cap, base·2^(attempts-1)); return random(0, exp)`. Pure given `r`, so unit
  tests pass a deterministically-seeded `*rand.Rand` and assert exact values.
- **Config & concurrency:** `New(rdb, opts ...Option)` gains `WithBackoff(base, cap time.Duration)`.
  Defaults: `base = 1s`, `cap = 10m`. Because workers call `Nack` concurrently, the broker's rand
  source **must be goroutine-safe**: the broker holds a seeded `*rand.Rand` guarded by a mutex (or
  equivalently a locked source), so `-race` stays clean. The pure `nextBackoff` keeps the math
  testable; the mutex lives at the call site in `Nack`.

### `internal/broker/scripts`

- **`promote.lua`** (new) — mirrors `reaper.lua`:
  ```
  KEYS[1] delayed, KEYS[2] ready
  ARGV[1] now_ms, ARGV[2] job key prefix, ARGV[3] limit
  expired = ZRANGEBYSCORE delayed -inf now LIMIT 0 limit
  for id in expired: ZREM delayed id; HSET prefix..id state ready; ZADD ready 0 id
  return #expired
  ```
- **`nack.lua`** (reworked):
  ```
  KEYS[1] inflight, KEYS[2] delayed, KEYS[3] dlq
  ARGV[1] id, ARGV[2] job key prefix, ARGV[3] ready_at_ms
  ZREM inflight id
  attempts = HGET ...; max = HGET ...
  if attempts < max: HSET state delayed; ZADD delayed ready_at_ms id; return 'retry'
  else:              HSET state dead;    RPUSH dlq id;                return 'dead'
  ```
  (Was: retry path did `HSET state ready` + `ZADD ready 0 id`.)

### `internal/worker`

- Extract the shared loop lifecycle (tick → drain-until-empty → sleep, with graceful shutdown)
  into one helper; `Reaper` and the new `Promoter` each supply a one-pass drain func.
- **`Promoter`** — `NewPromoter(b, queue, interval)` + `Run(ctx) error`, structurally identical to
  `Reaper`.

### `cmd/`

- `cmd/worker` — start a `Promoter` goroutine alongside the reaper; add `--promote-interval`
  (default `1s`); wire `WithBackoff` (optionally `--backoff-base` / `--backoff-cap`).
- `cmd/demo` — optional `--delay` to enqueue scheduled jobs and show the promoter at work.

## Testing

Same real-Redis harness (broker tests on DB 15, worker on DB 14, skip when unavailable).

Broker:

- Enqueue `WithDelay` / `WithReadyAt` → job in `delayed` with score ≈ now+delay, `state=delayed`,
  not in `ready`.
- Plain `Enqueue` → in `ready` with `state=ready` (corrected from `pending`).
- `Enqueue` with `WithDelay(0)` or a past `WithReadyAt` → goes straight to `ready`, not `delayed`.
- `Promote` → a past-due delayed job moves to `ready` (state `ready`, removed from `delayed`); a
  future-due job is left untouched. Prove the score filter is non-vacuous by removing it and
  watching the future-due test fail, then restore.
- `Nack` with retries left → job lands in `delayed` with ready-at in `[now, now+exp]`, not in
  `ready` or `dlq`, `state=delayed`.
- `Nack` exhausted → DLQ, `state=dead` (existing behavior preserved).
- `nextBackoff` pure unit tests (white-box): exponential growth, cap ceiling, jitter within
  `[0, exp]`, using a deterministic rand source.
- End-to-end: enqueue → claim → nack → (delayed) → `Promote` → re-claim with attempts advanced.

Worker:

- `Promoter` loop promotes due jobs over time (mirrors the existing reaper-loop test).

**Changed existing test:** `TestNackRequeuesWhenRetriesRemain` currently asserts a retry lands in
`ready`; it will be updated to assert `delayed` — the intended behavior change.

## Invariants preserved

- At-least-once, never exactly-once.
- Every state move stays atomic (Lua). `nack`'s retry/dead branch and move remain one script;
  only the retry destination changes (ready → delayed).
- Crash safety via the reaper is untouched.
- No queue dependency; built on Redis primitives.

## Known limitations (carried forward)

- No per-claim fencing token on ack/nack/promote (documented in CLAUDE.md). A backoff retry uses
  the attempts recorded on the hash, so a reclaim race could miscount; consistent with
  at-least-once.
