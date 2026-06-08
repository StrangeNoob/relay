# Relay — Phase 2d: Per-Queue Rate Limiting

**Status:** Approved design · **Date:** 2026-06-08
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 2 (depth) — fourth sub-project (follows 2a delayed/backoff, 2b priority, 2c idempotency)

## Purpose

Cap how fast a queue is consumed. The parent spec calls for "per-queue rate limiting (token
bucket)." This slice adds an opt-in per-queue token bucket that gates the claim: workers cannot
claim a queue's jobs faster than a configured rate, regardless of how many are ready — the classic
use being to protect a rate-limited downstream (e.g. a third-party API). A token is consumed only
when a job is actually delivered.

The remaining Phase 2 feature (Prometheus metrics) is a separate sub-project and out of scope.

## Scope

In scope:

- A per-queue token bucket (rate + burst), opt-in via a broker option.
- Folding the bucket gate into the atomic `claim.lua` so a token is consumed only on a successful
  pop (no waste on empty-queue polls or denied claims).

Out of scope: throttling enqueue/ingress (this throttles consumption only); distinguishing
"rate-limited" from "empty queue" to the worker (both just make the worker poll again); dynamic
runtime config in Redis; metrics.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| What it throttles | **Claim / consumption rate** | The task-queue use: cap processing throughput to protect a downstream. |
| Algorithm | **Token bucket** (rate tokens/sec, burst capacity) | Spec-specified; allows bursts up to `burst`, steady state `rate`. |
| Consume timing | **Only on a successful pop** | Empty-queue polls and denied claims must not drain the bucket, or idle workers would starve real work. Requires the bucket check and pop to be one atomic op. |
| Integration | **Extend `claim.lua`** with a skippable gate | One atomic claim, single source of truth, consume-on-pop. Skipped (behavior-identical) when no limit is configured. |
| Config | **Opt-in `WithRateLimit(queue, rate, burst)`** | Per-queue; unconfigured queues are unlimited. |
| Refill clock | **`now` passed from Go** (as claim already does) | Deterministic, testable; no Redis `TIME`. |

## Data model

Adds one key per limited queue; no new job-hash fields.

- `q:{name}:ratelimit` — a Redis **hash** with fields `tokens` (float balance) and `ts` (last-
  update unix ms). Created lazily on the first limited claim with `tokens = burst`, `ts = now`
  (fresh queue starts with a full burst). One bounded key per queue; no TTL.

## Mechanics: token bucket inside `claim.lua`

Go decides whether the queue is limited and passes the bucket key + params. The gate runs only
when enabled:

```
-- KEYS[1] ready, KEYS[2] inflight, KEYS[3] ratelimit (unused when enabled='0')
-- ARGV[1] now_ms, ARGV[2] visibility_ms, ARGV[3] job key prefix,
-- ARGV[4] rate (tokens/sec), ARGV[5] burst, ARGV[6] enabled ('1'/'0')

if ARGV[6] == '1' then
  local rate, burst, now = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[1])
  local data = redis.call('HMGET', KEYS[3], 'tokens', 'ts')
  local tokens = tonumber(data[1]) or burst
  local ts = tonumber(data[2]) or now
  tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)   -- lazy refill
  if tokens < 1 then
    return nil            -- rate-limited: do NOT pop; bucket not written (ts accrues)
  end
  -- a token is available; consume it only if the pop below succeeds
end

local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then
  return nil              -- empty queue: bucket untouched (no waste)
end
local id = popped[1]

if ARGV[6] == '1' then
  -- recompute is unnecessary; reuse tokens/now from above
  redis.call('HSET', KEYS[3], 'tokens', tokens - 1, 'ts', now)
end

-- unchanged claim tail:
local deadline = now + visibility
redis.call('ZADD', KEYS[2], deadline, id)
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HSET', job_key, 'state', 'inflight')
return redis.call('HGETALL', job_key)
```

(The implementer keeps the existing variable names from `claim.lua`; the only additions are the
gate block and the consume-on-pop `HSET`. `tokens`/`now` computed in the gate are reused for the
consume so there's no double refill.)

Properties:

- **Consume-only-on-pop:** the bucket is written only when a job is delivered. Denied claims and
  empty polls leave it untouched, so idle workers can't drain it.
- **Lazy refill** from `(now - ts)`; writing only on consume means time accrues correctly from the
  last consume.
- **Atomic:** refill + decision + pop + consume are one script — competing workers can never
  over-issue tokens, so the cluster-wide rate holds.
- **Unlimited queues unchanged:** `enabled='0'` skips the whole gate; the priority `ZPOPMAX` claim
  is byte-identical to today.

## Components & changes

### `internal/broker`

- `rateLimit` struct `{ rate float64; burst int }`; `Broker` gains `rateLimits map[string]rateLimit`.
- `WithRateLimit(queue string, rate float64, burst int) Option` — registers the limit; **panics**
  if `rate <= 0` or `burst < 1` (loud config error). Lazily initializes the map.
- `ratelimitKey(queue string) string` → `"q:" + queue + ":ratelimit"`.
- `Claim` looks up `b.rateLimits[queue]`; if present, runs `claimScript` with `KEYS` adding the
  ratelimit key and `ARGV` adding `rate, burst, "1"`; otherwise passes a harmless ratelimit key
  (e.g. `ratelimitKey(queue)`, never touched) and `rate=0, burst=0, "0"`. No `Claim` signature
  change.

### `internal/broker/scripts/claim.lua`

- Add the skippable gate (above). KEYS grows to 3; ARGV grows to 6.

### `cmd`

- `cmd/worker`: `--rate` (float64, default 0 = off) and `--burst` (int, default 0) flags; when
  `*rate > 0`, build the broker with `broker.WithRateLimit(*queue, *rate, *burst)`.

## Testing

Real Redis (broker tests DB 15, skip when unavailable).

Behavior-preserving (unlimited):

- All existing claim/priority/delay/promote/reap tests pass (the gate is skipped when no limit).
- A claim on an unconfigured queue creates no `q:{name}:ratelimit` key.

Rate-limited behavior (broker built with `WithRateLimit`):

- **Burst then deny:** burst=2, rate small; enqueue 5; three rapid claims → first two return jobs,
  third returns `ok=false`; `ready` ZCard == 3 (only 2 popped), `inflight` ZCard == 2.
- **Refill over time:** burst=1, rate=100/s; claim 1 (ok), immediate claim 2 (`ok=false`); sleep
  ~30 ms; claim 3 (ok) — a token refilled.
- **Consume-only-on-pop:** with a limit configured but an *empty* queue, claim many times (all
  `ok=false` from emptiness); then enqueue one job and claim → succeeds (the bucket was not drained
  by the empty polls).
- **Fresh queue starts full:** burst=3 → first three claims all succeed.

Concurrency (`-race`):

- burst=5, tiny rate, enqueue 50, fire 20 concurrent claims → exactly 5 succeed and 15 return
  `ok=false` (the atomic bucket never over-issues).

`cmd`: build/vet only.

## Known limitations

- **Cluster-wide config consistency.** The rate/burst are passed per call from each worker, so all
  workers on a queue must register the same `WithRateLimit`; mismatched configs make the shared
  bucket refill inconsistently. (A future Redis-stored config would remove this.)
- **Worker can't distinguish throttled from empty.** A rate-limited claim looks like an empty queue
  to the worker (it polls again). Adequate for throttling; finer back-pressure/metrics are future
  work.
- **Consumption throttle only.** Enqueue is not rate-limited.

## Invariants preserved

- At-least-once delivery.
- The atomic claim is sacred — the gate is inside the same single Lua script; for unlimited queues
  `claim.lua` is behavior-identical.
- Crash safety via the reaper is untouched (a rate-limited job simply stays in `ready`).
- Build from scratch on Redis primitives.
