# Relay — Phase 2b: Priority Queues

**Status:** Approved design · **Date:** 2026-06-07
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 2 (depth) — second sub-project (follows Phase 2a: delayed jobs, promoter, backoff)

## Purpose

Make the `ready` set honor per-job priority. Today every ready job is scored `0`, so claim
(`ZPOPMAX`) pops an arbitrary one; the parent spec calls for `q:{name}:ready` to be "scored by
priority." This slice realizes that: higher-priority jobs are claimed first, and within a
priority, the oldest-ready job is claimed first (FIFO).

The remaining Phase 2 features (idempotency enforcement, rate limiting, metrics) are separate
sub-projects and out of scope here.

## Scope

In scope:

- A `Priority` field on the job, set via a `WithPriority` enqueue option (or directly on the job).
- A composite `ready` ZSET score that encodes priority (primary, descending) and ready-time
  (secondary, FIFO) so a single `ZPOPMAX` yields the correct claim order.
- Computing that score everywhere a job enters `ready`: enqueue (Go), `promote.lua`, `reaper.lua`.

Out of scope (later sub-projects): idempotency enforcement, rate limiting, metrics. Strict
sub-millisecond FIFO (the tiebreak is millisecond-granular — see Known limitations).

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Priority direction | **Higher int = more urgent**, default `0` | Conventional; unset/legacy jobs stay lowest, so behavior is unchanged for code that never sets priority. |
| Tiebreak within a priority | **FIFO (oldest-ready first)** | Predictable, prevents reordering/starvation within a level. |
| Encoding | **Single ready ZSET, composite score** `priority·10^13 − nowMs`, popped by `ZPOPMAX` | Keeps the one ready set and the existing `ZPOPMAX` claim; change is concentrated in where scores are written. |
| Priority range | **`0–255`** (`MaxPriority`), clamped at enqueue | Bounds the float packing so the score is an exact `float64` integer (`255·10^13 < 2^53`). |
| Scale source of truth | **Go constant**, passed to `promote.lua`/`reaper.lua` as an ARGV | One definition; scripts don't hardcode a second copy. |
| API | **`WithPriority(p)` enqueue option** (+ `Job.Priority` field) | Consistent with `WithDelay`/`WithReadyAt`; option overrides the field at enqueue. |

## The ready-score formula

```
readyScore(priority, nowMs) = priority * 10^13 - nowMs
```

Claim already runs `ZPOPMAX` on `ready`, which pops the highest score:

- `priority * 10^13` dominates: `10^13` exceeds any realistic `nowMs` (~`1.7e12` now, ~`2.6e12` in
  30 years), so a higher priority always outscores a lower one regardless of time.
- Subtracting `nowMs`: within one priority, an earlier `nowMs` yields a *larger* score, so
  `ZPOPMAX` pops the **oldest** job first — FIFO.
- Precision: `255 * 10^13 = 2.55e15 < 2^53 (~9.007e15)`, so the packed value is an exact float64
  integer and the `nowMs` subtraction stays exact. Priority-0 jobs get a negative score (`−nowMs`);
  ZSETs handle negative scores fine, and any positive-priority job correctly outranks them.

`nowMs` is "the time the job enters `ready`": enqueue time for an immediate enqueue, promotion
time for a delayed/backed-off job, reap time for a redelivered job. FIFO is therefore by
readiness time, which is the intuitive order.

## Why `claim.lua` does not change

`claim.lua` already pops `ready` with `ZPOPMAX`. Today all scores are `0`, so it pops an arbitrary
member. Once meaningful scores are written, the *same* `ZPOPMAX` returns highest-priority-then-
oldest. The entire behavioral change is in **where ready scores are written**, not in claim.

| Job enters `ready` via | Before | After |
|---|---|---|
| `Enqueue` (immediate) | `ZADD ready 0 id` | `ZADD ready readyScore(prio, now) id` (computed in Go) |
| `promote.lua` (delayed→ready) | `ZADD ready 0 id` | read `priority` from hash; `ZADD ready (prio*scale - now) id` |
| `reaper.lua` (inflight→ready) | `ZADD ready 0 id` | read `priority` from hash; `ZADD ready (prio*scale - now) id` |

`nack.lua` is unchanged: a retry goes to the `delayed` set (scored by ready-at), and the job's
priority rides along on its hash, gaining a composite ready score only when the promoter later
moves it to `ready`. The `delayed` and `inflight` scorings are unchanged.

## Components & changes

### `internal/job`

- Add `Priority int` field; `New` defaults it to `0`.
- Add hash field `priority`; `ToHash` writes `strconv.Itoa`, `FromHash` parses with a wrapped
  error on malformed input (consistent with `attempts`/`max_retries`).

### `internal/broker`

- Constants: `MaxPriority = 255`; `priorityScale = 1e13` (as a typed value usable in arithmetic).
- Pure helper `readyScore(priority int, nowMs int64) float64` returning `priority*priorityScale - nowMs`,
  with priority assumed already clamped.
- `clampPriority(p int) int` → `[0, MaxPriority]`.
- `WithPriority(p int) EnqueueOption`: sets `enqueueConfig.priority` and a `prioritySet` flag.
- `Enqueue`: if `prioritySet`, set `j.Priority = cfg.priority`; clamp `j.Priority`; the immediate
  branch uses `ZADD ready readyScore(j.Priority, time.Now().UnixMilli())`. The delayed branch is
  unchanged (delayed score stays ready-at ms). Clamped priority is written to the hash.
- `Promote` and `Reap`: pass `priorityScale` as an additional ARGV to their scripts.

### `internal/broker/scripts`

- `promote.lua`: for each due id, read `priority` from the hash (`HGET prefix..id 'priority'`,
  default 0), compute `priority*scale - now`, and `ZADD ready <score> id` (was `ZADD ready 0 id`).
  New ARGV: the priority scale.
- `reaper.lua`: same change — read `priority`, compute the composite score, `ZADD ready <score> id`.
  New ARGV: the priority scale.
- `claim.lua`, `nack.lua`, `ack.lua`, `heartbeat.lua`: unchanged.

### `cmd`

- `cmd/demo`: optional `--priority` flag; when set, enqueue with `broker.WithPriority`.

## Testing

Real Redis (broker tests DB 15, worker DB 14, skip when unavailable).

`internal/job`:

- `Priority` round-trips through `ToHash`/`FromHash`.
- `New` defaults `Priority` to 0.
- Malformed `priority` field → wrapped error from `FromHash`.

`internal/broker` (white-box where noted):

- `readyScore` (white-box): higher priority ⇒ higher score; within a priority, smaller `nowMs` ⇒
  higher score; a priority-1 job outscores a priority-0 job enqueued at any time.
- `Enqueue` with `WithPriority(p)` → hash `priority` == p and ready ZScore == `readyScore(p, ~now)`
  (bounded check).
- Clamping: `WithPriority(300)` stores 255; `WithPriority(-5)` stores 0.
- **Claim returns highest priority first**: enqueue p=1, p=9, p=5 → three claims yield 9, 5, 1.
  Fully deterministic (distinct priorities).
- **FIFO within a priority**: enqueue two p=5 jobs ~2 ms apart → claim returns the older first.
- **Priority preserved through delay→promote**: enqueue p=7 with a delay and a p=0 immediately;
  force the delayed job due; `Promote`; the next claim returns the p=7 job ahead of the p=0 job.
- **Priority preserved through reap**: claim a p=8 job with visibility 0; `Reap`; it returns to
  ready with a priority-8 score (verify it outranks a p=0 job, or check the ZScore band).

`cmd`: build/vet only (thin wiring).

## Known limitations

- **Millisecond-granular FIFO.** Two same-priority jobs that enter `ready` in the same millisecond
  tie on score and fall back to Redis's job-id ordering. A strict per-queue sequence counter is
  possible future hardening; ms resolution is sufficient for a demo-grade queue.
- **Bounded priority range (0–255).** Chosen so the composite score stays an exact float64 integer.
  Carried forward from the encoding decision, not an accident.

## Invariants preserved

- At-least-once, never exactly-once.
- The atomic claim is sacred — `claim.lua` is unchanged and still a single `ZPOPMAX`.
- Crash safety via the reaper is preserved (reaper still requeues expired inflight jobs; it just
  writes a priority-aware score now).
- Every state move stays atomic (Lua / one transaction).
- Build from scratch on Redis primitives.
