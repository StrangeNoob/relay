# Relay — Phase 2c: Idempotency Enforcement

**Status:** Approved design · **Date:** 2026-06-07
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Phase:** 2 (depth) — third sub-project (follows Phase 2a delayed/backoff and Phase 2b priority)

## Purpose

Enforce the idempotency key that already exists on the job model. Today `Job.IdempotencyKey` is
stored and round-tripped but the broker ignores it; the parent spec reserves `q:{name}:dedup` for
"idempotency keys for enqueue dedup." This slice makes a keyed `Enqueue` drop duplicates within a
TTL window, so submitting the same logical job twice enqueues it once.

The remaining Phase 2 features (per-queue rate limiting, Prometheus metrics) are separate
sub-projects and out of scope.

## Scope

In scope:

- A TTL time-window dedup at enqueue, keyed by `Job.IdempotencyKey`, per queue.
- A unified atomic `enqueue.lua` that performs the optional dedup claim and the job write in one
  step (replacing the current `TxPipeline` in `Enqueue`).
- A sentinel `ErrDuplicate` returned when a keyed enqueue is dropped.
- A `WithIdempotencyKey` enqueue option and a `WithDedupTTL` broker option (default 24h).

Out of scope: lifetime/in-flight dedup (releasing the key on ack/nack) — this is the TTL-window
model only, with no coupling to terminal transitions; rate limiting; metrics.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Dedup model | **TTL time-window** | Drop duplicates within a window; entry auto-expires. Simple, bounds memory, touches only `Enqueue` (no ack/nack coupling). |
| Duplicate signal | **Sentinel `ErrDuplicate`** (signature stays `Enqueue(...) error`) | Callers detect via `errors.Is`; keyless flows never see it, so no call-site churn. |
| Atomicity | **One `enqueue.lua`** doing `SET NX` + `HSET` + `ZADD` | A `SET NX` then a separate write has a crash window (key claimed, job lost); `MULTI` can't branch on the `SET NX` result. Lua is the only atomic option. |
| Enqueue unification | **All enqueues go through `enqueue.lua`** (replaces `TxPipeline`) | One atomic path, no duplicated write logic between a keyless Tx path and a keyed Lua path. |
| Dedup storage | **Per-key string** `q:{name}:dedup:{key}` with `EX` | Redis sets/hashes lack per-member TTL; per-key strings give each marker its own expiry. |
| TTL | **Broker-level default 24h**, `WithDedupTTL` | A sane window; configurable. |

## Data model

Adds one key family; no new job-hash fields.

- `q:{name}:dedup:{key}` — **string**, value = the winning job's id, `EX` = dedup TTL. One marker
  per idempotency key per queue.

`Job.IdempotencyKey` already exists on the job model and is encoded in the `job:{id}` hash.

## The atomic enqueue (`enqueue.lua`)

Replaces the `TxPipeline` currently in `Enqueue`. Go decides ready-vs-delayed and computes the
score exactly as today, then hands the script everything it needs.

```
-- KEYS[1] = job hash key      job:{id}
-- KEYS[2] = target set        q:{name}:ready  OR  q:{name}:delayed
-- KEYS[3] = dedup key         q:{name}:dedup:{idempotency-key}  (unused when useDedup=0)
-- ARGV[1] = job id            (ZADD member + dedup marker value)
-- ARGV[2] = score             (ready: priority composite; delayed: ready-at ms)
-- ARGV[3] = dedup TTL seconds
-- ARGV[4] = useDedup          "1" to dedup, "0" to skip
-- ARGV[5..] = job hash field/value pairs (flattened ToHash)
--
-- Returns 'ok' if enqueued, 'dup' if dropped as a duplicate.

if ARGV[4] == '1' then
  if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'EX', tonumber(ARGV[3])) == false then
    return 'dup'
  end
end
for i = 5, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 'ok'
```

The dedup claim and the write are one indivisible script: there is no crash window between
claiming the key and writing the job, and two concurrent same-key enqueues are serialized by
Redis (first wins, second returns `'dup'`) — competing-producer safety, the mirror of the atomic
claim's competing-consumer safety. On `'dup'`, nothing is written (no hash, no ZADD).

## Components & changes

### `internal/broker`

- `var ErrDuplicate = errors.New("broker: duplicate enqueue")`.
- `dedupKey(queue, key string) string` → `"q:" + queue + ":dedup:" + key`.
- `WithDedupTTL(d time.Duration) Option`; `Broker` gains a `dedupTTL time.Duration` field, default
  `24 * time.Hour` set in `New`.
- `WithIdempotencyKey(k string) EnqueueOption`: sets `enqueueConfig.idempotencyKey` + a
  `idempotencyKeySet` flag; `Enqueue` applies it to `j.IdempotencyKey` (override), mirroring
  `WithPriority`.
- `Enqueue` reworked: apply options; clamp priority; decide target set + score (unchanged logic);
  build the dedup args (`useDedup="1"` and the dedup key only when `j.IdempotencyKey != ""`); run
  `enqueueScript`; map a `"dup"` reply to `ErrDuplicate`, else nil; wrap other errors with `%w`.
- The `TxPipeline` in `Enqueue` is removed.

### `internal/broker/scripts`

- `enqueue.lua` (new, per above), embedded via `go:embed` and wrapped in `redis.NewScript`.
- All other scripts unchanged.

### `cmd`

- `cmd/demo`: `--idempotency-key string` flag; when non-empty, every generated job is enqueued
  with `broker.WithIdempotencyKey(key)`. The enqueue-error handling must treat `ErrDuplicate` as
  benign — log it and continue, not `os.Exit(1)` — so the demo visibly drops duplicates.

## Testing

Real Redis (broker tests DB 15, skip when unavailable).

Behavior-preserving (keyless) — the Lua rewrite must not change existing behavior:

- All existing enqueue/claim/priority/delay/promote/reap tests still pass.
- A keyless `Enqueue` creates no `q:{name}:dedup:*` marker.

Dedup behavior:

- Keyed `Enqueue` → job enqueued; `q:emails:dedup:{key}` exists with value = job id and a TTL in
  `(0, configured]` (check via `TTL`/`PTTL`).
- Second keyed `Enqueue` with the same key → `errors.Is(err, broker.ErrDuplicate)`; ready depth
  stays 1; the duplicate job's hash (`job:{dupID}`) is **absent** (proves the gate precedes the
  write).
- Two different keys → both enqueued.
- Marker deleted (simulating TTL expiry) → re-enqueue with the same key succeeds.
- Dedup on a **delayed** enqueue (`WithDelay` + key) → job in `delayed`, marker set, duplicate
  dropped.
- `WithDedupTTL(d)` respected — the marker's TTL is ≈ `d`.
- `WithIdempotencyKey(k)` sets the key (equivalent to setting `j.IdempotencyKey`).

Concurrency (`-race`):

- N concurrent same-key enqueues → exactly one returns nil and one job is in the queue; the rest
  return `ErrDuplicate`. (Competing-producer safety.)

`cmd`: build/vet only.

## Known limitations

- **TTL-window only, no completion release.** A key stays claimed for the full TTL even after the
  job completes; re-enqueueing the same key within the window is dropped regardless of whether the
  original already ran. Lifetime/in-flight dedup (release on ack) is possible future work.
- **At-least-once still holds.** Idempotency keys dedup *enqueues*; they do not make *delivery*
  exactly-once. A consumer can still receive a job more than once (crash/redelivery); consumers
  that need exactly-once effects must dedup on their side using the key.

## Invariants preserved

- At-least-once, never exactly-once (this adds enqueue dedup, not exactly-once delivery).
- The atomic claim is sacred — `claim.lua` is unchanged.
- Every state move stays atomic: `enqueue.lua` makes the dedup-claim + write one script (the
  enqueue was already atomic via `TxPipeline`; it stays atomic and gains the dedup gate).
- Crash safety via the reaper is untouched.
- Build from scratch on Redis primitives.
