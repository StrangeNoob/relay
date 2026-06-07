# Relay Phase 2c — Idempotency Enforcement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop duplicate enqueues that share an idempotency key, within a TTL window, atomically.

**Architecture:** `Enqueue` moves from a `TxPipeline` to a single `enqueue.lua` that does an optional `SET dedup NX EX ttl` then the `HSET`+`ZADD`. A keyed duplicate returns the sentinel `broker.ErrDuplicate` and writes nothing. Markers are per-key strings `q:{name}:dedup:{key}` with `EX`.

**Tech Stack:** Go, Redis (go-redis v9), Lua via `go:embed`. Tests run against a real Redis.

**Spec:** [`docs/superpowers/specs/2026-06-07-relay-phase2c-idempotency-enforcement-design.md`](../specs/2026-06-07-relay-phase2c-idempotency-enforcement-design.md)

**Prerequisites:** Redis reachable at `localhost:6379` (or `REDIS_ADDR`) — broker tests skip without it. Confirm `redis-cli ping` → `PONG`. Work happens on the current branch/worktree.

---

## File Structure

- `internal/broker/dedup.go` — create: `ErrDuplicate`, `dedupKey`, `WithDedupTTL`.
- `internal/broker/dedup_test.go` — create: white-box unit tests (package `broker`).
- `internal/broker/broker.go` — modify: `dedupTTL` field + default; `enqueueConfig` idempotency fields; `WithIdempotencyKey`; `Enqueue` reworked onto `enqueue.lua` + dedup.
- `internal/broker/scripts/enqueue.lua` — create: atomic write (+ dedup branch).
- `internal/broker/scripts.go` — modify: embed `enqueueScript`.
- `internal/broker/broker_test.go` — modify: dedup tests + concurrency test.
- `cmd/demo/main.go` — modify: `--idempotency-key` flag, `ErrDuplicate`-tolerant handling.
- `CLAUDE.md` — modify: reflect idempotency shipped.

`claim.lua`, `nack.lua`, `promote.lua`, `reaper.lua`, `ack.lua`, `heartbeat.lua`, `internal/job`, `internal/worker` are unchanged.

---

## Task 1: Dedup scaffolding (errors, key, TTL option)

**Files:**
- Create: `internal/broker/dedup.go`
- Modify: `internal/broker/broker.go` (`dedupTTL` field + default in `New`)
- Test: `internal/broker/dedup_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/broker/dedup_test.go` (white-box — `package broker`):

```go
package broker

import (
	"testing"
	"time"
)

func TestDedupKeyFormat(t *testing.T) {
	if got := dedupKey("emails", "abc"); got != "q:emails:dedup:abc" {
		t.Errorf("dedupKey = %q, want %q", got, "q:emails:dedup:abc")
	}
}

func TestDedupTTLDefaultAndOverride(t *testing.T) {
	if b := New(nil); b.dedupTTL != 24*time.Hour {
		t.Errorf("default dedupTTL = %v, want 24h", b.dedupTTL)
	}
	if b := New(nil, WithDedupTTL(time.Hour)); b.dedupTTL != time.Hour {
		t.Errorf("dedupTTL = %v, want 1h", b.dedupTTL)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestDedupKeyFormat|TestDedupTTLDefaultAndOverride' 2>&1 | head`
Expected: build failure — `undefined: dedupKey`, `undefined: WithDedupTTL`, `b.dedupTTL undefined`.

- [ ] **Step 3: Create dedup.go**

Create `internal/broker/dedup.go`:

```go
package broker

import (
	"errors"
	"time"
)

// ErrDuplicate is returned by Enqueue when a job carries an idempotency key that
// was already enqueued within the dedup TTL window. Nothing is written; the
// logical job is already in the system, so callers can treat this as a benign
// no-op via errors.Is(err, ErrDuplicate).
var ErrDuplicate = errors.New("broker: duplicate enqueue")

// dedupKey is the Redis key holding the idempotency marker for one key on one
// queue: `q:{name}:dedup:{key}`. It is a string with an EX TTL — Redis sets lack
// per-member expiry, so each marker is its own key and expires independently.
func dedupKey(queue, key string) string {
	return "q:" + queue + ":dedup:" + key
}

// WithDedupTTL sets how long an idempotency marker is remembered. Within this
// window a second enqueue with the same key is dropped; afterwards the key is
// free again. Default 24h.
func WithDedupTTL(d time.Duration) Option {
	return func(b *Broker) { b.dedupTTL = d }
}
```

- [ ] **Step 4: Add the `dedupTTL` field and default**

In `internal/broker/broker.go`, add `dedupTTL time.Duration` to the `Broker` struct (after `backoffMax`):

```go
	rdb         *redis.Client
	backoffBase time.Duration
	backoffMax  time.Duration
	dedupTTL    time.Duration

	rndMu sync.Mutex
	rnd   *rand.Rand
```

In `New`, set the default (after `backoffMax: 10 * time.Minute,`):

```go
		backoffMax:  10 * time.Minute,
		dedupTTL:    24 * time.Hour,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestDedupKeyFormat|TestDedupTTLDefaultAndOverride' -v 2>&1 | tail`
Expected: PASS for both.

- [ ] **Step 6: Commit**

```bash
git add internal/broker/dedup.go internal/broker/dedup_test.go internal/broker/broker.go
git commit -m "Add dedup scaffolding: ErrDuplicate, dedupKey, WithDedupTTL"
```

---

## Task 2: Move Enqueue onto enqueue.lua (behavior-preserving refactor)

**Files:**
- Create: `internal/broker/scripts/enqueue.lua`
- Modify: `internal/broker/scripts.go` (embed)
- Modify: `internal/broker/broker.go` (`Enqueue` body)

This is a refactor: replace the `TxPipeline` with a Lua script that produces an identical `HSET`+`ZADD`. No new test — the existing enqueue/claim/priority/delay tests are the regression guard.

- [ ] **Step 1: Create the enqueue Lua script (no dedup yet)**

Create `internal/broker/scripts/enqueue.lua`:

```lua
-- enqueue.lua — write a job's hash and place it in its target set, atomically.
--
-- Go decides the target set (ready or delayed) and the score, then hands them in
-- with the flattened job hash. Doing the write in one script keeps it atomic
-- (the dedup gate added next slots in ahead of the write without a round-trip).
--
-- KEYS[1] = job hash key  job:{id}
-- KEYS[2] = target set    q:{name}:ready OR q:{name}:delayed
-- ARGV[1] = job id (ZADD member)
-- ARGV[2] = score (ready: priority composite; delayed: ready-at ms)
-- ARGV[3..] = job hash field/value pairs (flattened ToHash)
--
-- Returns 'ok'.

for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 'ok'
```

- [ ] **Step 2: Embed the script**

In `internal/broker/scripts.go`, after the last existing embed block, add:

```go
//go:embed scripts/enqueue.lua
var enqueueSrc string

var enqueueScript = redis.NewScript(enqueueSrc)
```

- [ ] **Step 3: Rework `Enqueue` to use the script**

In `internal/broker/broker.go`, replace the entire `Enqueue` method body (the current `TxPipeline` version) with:

```go
func (b *Broker) Enqueue(ctx context.Context, j job.Job, opts ...EnqueueOption) error {
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.prioritySet {
		j.Priority = cfg.priority
	}
	j.Priority = clampPriority(j.Priority)

	now := time.Now()
	var targetKey string
	var score float64
	if cfg.readyAt.After(now) {
		j.State = job.StateDelayed
		targetKey = delayedKey(j.Queue)
		score = float64(cfg.readyAt.UnixMilli())
	} else {
		j.State = job.StateReady
		targetKey = readyKey(j.Queue)
		score = readyScore(j.Priority, now.UnixMilli())
	}

	h := j.ToHash()
	args := make([]any, 0, 2+2*len(h))
	args = append(args, j.ID, score)
	for k, v := range h {
		args = append(args, k, v)
	}

	if err := enqueueScript.Run(ctx, b.rdb, []string{jobKey(j.ID), targetKey}, args...).Err(); err != nil {
		return fmt.Errorf("broker: enqueuing job %s: %w", j.ID, err)
	}
	return nil
}
```

Note: this removes the `redis.Z`/`TxPipeline`/`pipe.HSet`/`pipe.ZAdd` usage from `Enqueue`. If that leaves the `redis` import unused elsewhere, keep it — `redis.NewScript`/`redis.Options` are still used in the package. (They are; do not remove the import.)

- [ ] **Step 4: Run the regression suite**

Run: `go test -race ./internal/broker/ ./internal/worker/ 2>&1 | tail`
Expected: `ok` for both — all existing enqueue/claim/priority/delay/promote/reap/worker tests pass unchanged (the script produces an identical hash + ZADD).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/scripts/enqueue.lua internal/broker/scripts.go internal/broker/broker.go
git commit -m "Move Enqueue onto an atomic enqueue.lua script"
```

---

## Task 3: Idempotency dedup at enqueue

**Files:**
- Modify: `internal/broker/scripts/enqueue.lua` (add dedup branch)
- Modify: `internal/broker/broker.go` (`enqueueConfig`, `WithIdempotencyKey`, `Enqueue` dedup wiring)
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/broker/broker_test.go` (it is `package broker_test`; it already imports `context`, `time`, `testing`, `redis`, `broker`, `job` — add `errors` to the import block if not present):

```go
func TestEnqueueWithIdempotencyKeyCreatesMarker(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("x"))
	if err := b.Enqueue(ctx, j, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	val, err := rdb.Get(ctx, "q:emails:dedup:k1").Result()
	if err != nil {
		t.Fatalf("dedup marker missing: %v", err)
	}
	if val != j.ID {
		t.Errorf("marker value = %q, want job id %q", val, j.ID)
	}
	ttl, _ := rdb.TTL(ctx, "q:emails:dedup:k1").Result()
	if ttl <= 0 || ttl > 24*time.Hour {
		t.Errorf("marker TTL = %v, want within (0, 24h]", ttl)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready size = %d, want 1", n)
	}
}

func TestEnqueueDuplicateKeyReturnsErrDuplicate(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	first := job.New("emails", []byte("first"))
	if err := b.Enqueue(ctx, first, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	second := job.New("emails", []byte("second"))
	err := b.Enqueue(ctx, second, broker.WithIdempotencyKey("k1"))
	if !errors.Is(err, broker.ErrDuplicate) {
		t.Fatalf("second Enqueue err = %v, want ErrDuplicate", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready size = %d, want 1 (duplicate dropped)", n)
	}
	if n, _ := rdb.Exists(ctx, "job:"+second.ID).Result(); n != 0 {
		t.Errorf("duplicate job hash exists; the gate must precede the write")
	}
}

func TestEnqueueDifferentKeysBothEnqueued(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("a")), broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("Enqueue k1: %v", err)
	}
	if err := b.Enqueue(ctx, job.New("emails", []byte("b")), broker.WithIdempotencyKey("k2")); err != nil {
		t.Fatalf("Enqueue k2: %v", err)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 2 {
		t.Errorf("ready size = %d, want 2", n)
	}
}

func TestEnqueueReenqueueAfterMarkerExpiry(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	first := job.New("emails", []byte("first"))
	if err := b.Enqueue(ctx, first, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Simulate the TTL elapsing by deleting the marker.
	if err := rdb.Del(ctx, "q:emails:dedup:k1").Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}
	second := job.New("emails", []byte("second"))
	if err := b.Enqueue(ctx, second, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("re-enqueue after expiry: %v", err)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 2 {
		t.Errorf("ready size = %d, want 2 (re-enqueue allowed)", n)
	}
}

func TestEnqueueKeylessCreatesNoMarker(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	keys, _ := rdb.Keys(ctx, "q:emails:dedup:*").Result()
	if len(keys) != 0 {
		t.Errorf("dedup keys = %v, want none for a keyless enqueue", keys)
	}
}

func TestEnqueueDelayedRespectsDedup(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	first := job.New("emails", []byte("first"))
	if err := b.Enqueue(ctx, first, broker.WithDelay(time.Hour), broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 1 {
		t.Errorf("delayed size = %d, want 1", n)
	}
	if n, _ := rdb.Exists(ctx, "q:emails:dedup:k1").Result(); n != 1 {
		t.Errorf("dedup marker missing for delayed enqueue")
	}
	second := job.New("emails", []byte("second"))
	if err := b.Enqueue(ctx, second, broker.WithDelay(time.Hour), broker.WithIdempotencyKey("k1")); !errors.Is(err, broker.ErrDuplicate) {
		t.Fatalf("second Enqueue err = %v, want ErrDuplicate", err)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 1 {
		t.Errorf("delayed size = %d, want 1 after dup", n)
	}
}

func TestWithDedupTTLRespected(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithDedupTTL(time.Hour))

	j := job.New("emails", []byte("x"))
	if err := b.Enqueue(ctx, j, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ttl, _ := rdb.TTL(ctx, "q:emails:dedup:k1").Result()
	if ttl <= 55*time.Minute || ttl > time.Hour {
		t.Errorf("marker TTL = %v, want ~1h", ttl)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestEnqueueWithIdempotencyKey|TestEnqueueDuplicate|TestEnqueueDifferentKeys|TestEnqueueReenqueue|TestEnqueueKeyless|TestEnqueueDelayedRespectsDedup|TestWithDedupTTLRespected' 2>&1 | tail`
Expected: build failure first — `undefined: broker.WithIdempotencyKey`.

- [ ] **Step 3: Add idempotency to `enqueueConfig` and `WithIdempotencyKey`**

In `internal/broker/broker.go`, replace the `enqueueConfig` struct with (it currently has `readyAt`, `priority`, `prioritySet`):

```go
// enqueueConfig holds resolved enqueue options. A zero readyAt means "now".
type enqueueConfig struct {
	readyAt           time.Time
	priority          int
	prioritySet       bool
	idempotencyKey    string
	idempotencyKeySet bool
}
```

Add `WithIdempotencyKey` after `WithPriority`:

```go
// WithIdempotencyKey sets the job's idempotency key for this enqueue, overriding
// any value already on the job. A non-empty key makes the enqueue dedup: a second
// enqueue with the same key on the same queue, within the dedup TTL, is dropped
// with ErrDuplicate.
func WithIdempotencyKey(k string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.idempotencyKey = k
		c.idempotencyKeySet = true
	}
}
```

- [ ] **Step 4: Add the dedup branch to enqueue.lua**

Replace the entire contents of `internal/broker/scripts/enqueue.lua` with:

```lua
-- enqueue.lua — write a job's hash and place it in its target set, atomically,
-- with an optional idempotency-key dedup gate.
--
-- Go decides the target set (ready or delayed), the score, and whether to dedup,
-- then hands them in with the flattened job hash. The dedup claim and the write
-- are one script, so there is no crash window between claiming the key and
-- writing the job, and concurrent same-key enqueues are serialized by Redis.
--
-- KEYS[1] = job hash key  job:{id}
-- KEYS[2] = target set    q:{name}:ready OR q:{name}:delayed
-- KEYS[3] = dedup key     q:{name}:dedup:{key}   (unused when useDedup = '0')
-- ARGV[1] = job id (ZADD member + dedup marker value)
-- ARGV[2] = score (ready: priority composite; delayed: ready-at ms)
-- ARGV[3] = dedup TTL in seconds
-- ARGV[4] = useDedup, '1' to dedup or '0' to skip
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

- [ ] **Step 5: Rework `Enqueue` to wire dedup**

In `internal/broker/broker.go`, replace the `Enqueue` method (the Task 2 version) with:

```go
func (b *Broker) Enqueue(ctx context.Context, j job.Job, opts ...EnqueueOption) error {
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.prioritySet {
		j.Priority = cfg.priority
	}
	j.Priority = clampPriority(j.Priority)
	if cfg.idempotencyKeySet {
		j.IdempotencyKey = cfg.idempotencyKey
	}

	now := time.Now()
	var targetKey string
	var score float64
	if cfg.readyAt.After(now) {
		j.State = job.StateDelayed
		targetKey = delayedKey(j.Queue)
		score = float64(cfg.readyAt.UnixMilli())
	} else {
		j.State = job.StateReady
		targetKey = readyKey(j.Queue)
		score = readyScore(j.Priority, now.UnixMilli())
	}

	// Dedup only when the job carries an idempotency key. TTL is clamped to at
	// least 1s because Redis SET EX rejects a non-positive expiry.
	useDedup := "0"
	ttlSecs := 0
	dk := ""
	if j.IdempotencyKey != "" {
		useDedup = "1"
		dk = dedupKey(j.Queue, j.IdempotencyKey)
		ttlSecs = int(b.dedupTTL.Seconds())
		if ttlSecs < 1 {
			ttlSecs = 1
		}
	}

	h := j.ToHash()
	args := make([]any, 0, 4+2*len(h))
	args = append(args, j.ID, score, ttlSecs, useDedup)
	for k, v := range h {
		args = append(args, k, v)
	}

	res, err := enqueueScript.Run(ctx, b.rdb, []string{jobKey(j.ID), targetKey, dk}, args...).Text()
	if err != nil {
		return fmt.Errorf("broker: enqueuing job %s: %w", j.ID, err)
	}
	if res == "dup" {
		return ErrDuplicate
	}
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestEnqueue|TestWithDedupTTLRespected|TestClaim' -v 2>&1 | tail -30`
Expected: PASS for the seven new dedup tests and all existing `TestEnqueue*`/`TestClaim*` tests.

- [ ] **Step 7: Commit**

```bash
git add internal/broker/scripts/enqueue.lua internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Enforce idempotency keys with TTL dedup at enqueue"
```

---

## Task 4: Competing-producer concurrency test

**Files:**
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the test**

Add to `internal/broker/broker_test.go` (add `sync` and `sync/atomic` to the import block if not present):

```go
func TestConcurrentEnqueueSameKeyDeduplicates(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	const n = 50
	var okCount, dupCount int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := job.New("emails", []byte("x"))
			j.IdempotencyKey = "same"
			switch err := b.Enqueue(ctx, j); {
			case err == nil:
				atomic.AddInt64(&okCount, 1)
			case errors.Is(err, broker.ErrDuplicate):
				atomic.AddInt64(&dupCount, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if okCount != 1 {
		t.Errorf("ok count = %d, want exactly 1", okCount)
	}
	if dupCount != int64(n-1) {
		t.Errorf("dup count = %d, want %d", dupCount, n-1)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); c != 1 {
		t.Errorf("ready size = %d, want exactly 1", c)
	}
}
```

- [ ] **Step 2: Run it under the race detector**

Run: `go test -race ./internal/broker/ -run TestConcurrentEnqueueSameKeyDeduplicates -v 2>&1 | tail`
Expected: PASS — exactly one enqueue wins, the rest get `ErrDuplicate`, one job in the queue. (This passes because `enqueue.lua` runs atomically; if dedup were a separate `SET NX` + write it could fail.)

- [ ] **Step 3: Commit**

```bash
git add internal/broker/broker_test.go
git commit -m "Add competing-producer dedup concurrency test"
```

---

## Task 5: Demo flag + docs

**Files:**
- Modify: `cmd/demo/main.go`
- Modify: `CLAUDE.md`

Thin wiring + docs, verified by build/vet.

- [ ] **Step 1: Add `--idempotency-key` to cmd/demo**

First READ `cmd/demo/main.go` to confirm the exact variable names (`delay`, `priority`, the loop var, `logger`, the `opts` slice, the import block). Then:

(a) Add `"errors"` to the import block (standard-library group) if not already present.

(b) Add a flag in the flag block (near `--priority`):

```go
	idempotencyKey := flag.String("idempotency-key", "", "idempotency key applied to every enqueued job (empty = none)")
```

(c) In the enqueue loop, append the option and tolerate `ErrDuplicate`. The opts/enqueue block becomes:

```go
		var opts []broker.EnqueueOption
		if *delay > 0 {
			opts = append(opts, broker.WithDelay(*delay))
		}
		if *priority != 0 {
			opts = append(opts, broker.WithPriority(*priority))
		}
		if *idempotencyKey != "" {
			opts = append(opts, broker.WithIdempotencyKey(*idempotencyKey))
		}
		switch err := b.Enqueue(ctx, j, opts...); {
		case errors.Is(err, broker.ErrDuplicate):
			logger.Info("duplicate dropped", "i", i, "key", *idempotencyKey)
		case err != nil:
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
```

(Match the existing loop/logger variable names; reuse the existing `opts` slice if one is already declared.)

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`:

1. In the `q:{name}:dedup` data-model table row, replace the `**planned (Phase 2)** — idempotency keys for enqueue dedup` cell text with:
   `per-idempotency-key string marker (`q:{name}:dedup:{key}`) with TTL; **enqueue dedup** — a keyed duplicate is dropped with `ErrDuplicate``
2. In the Build order "Phase 2 — depth (in progress)" line, add `idempotency ✅` to the completed list and remove `idempotency enforcement` from the still-to-do list (leaving per-queue rate limiting, Prometheus metrics).
3. In the status/inventory `internal/broker` bullet, add `WithIdempotencyKey` to the `Enqueue` options list (so it reads `WithDelay`/`WithReadyAt`/`WithPriority`/`WithIdempotencyKey`).
4. In "Known limitations", add a bullet:
   `- **Idempotency is enqueue-only, TTL-window.** A keyed duplicate is dropped within the dedup TTL (default 24h); the key is not released on completion. Delivery remains at-least-once — consumers needing exactly-once effects still dedup on the key.`

(Adapt wording if the exact strings differ; the intent: dedup is implemented, idempotency is done in Phase 2, `WithIdempotencyKey` is an option, and the enqueue-only/TTL limitation is recorded.)

- [ ] **Step 3: Verify build, vet, format**

Run: `go build ./... && go vet ./... && gofmt -l cmd/ internal/ && echo OK`
Expected: `OK` (empty gofmt output, no errors).

- [ ] **Step 4: Commit**

```bash
git add cmd/demo/main.go CLAUDE.md
git commit -m "Add demo idempotency-key flag; document idempotency enforcement"
```

---

## Task 6: Full verification

- [ ] **Step 1: Whole suite under race (uncached)**

Run: `go test -race -count=1 ./... 2>&1 | tail`
Expected: `ok` for `internal/broker`, `internal/job`, `internal/worker`; `no test files` for the `cmd` packages.

- [ ] **Step 2: Lint and format**

Run: `gofmt -l internal/ cmd/ ; go vet ./... && golangci-lint run ./... 2>&1 | tail`
Expected: empty `gofmt`, no vet errors, `0 issues` from golangci-lint. (If `golangci-lint` is missing, install pinned: `curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v2.12.2` then re-run.)

- [ ] **Step 3: (Optional) end-to-end dedup smoke**

With Redis running:
```bash
go build -o /tmp/relay-demo ./cmd/demo
redis-cli -n 0 DEL q:idem:ready >/dev/null
/tmp/relay-demo -queue idem -count 5 -idempotency-key order-42
# expect: 1 enqueued, 4 "duplicate dropped" log lines
echo "ready depth: $(redis-cli -n 0 ZCARD q:idem:ready)  (expect 1)"
redis-cli -n 0 DEL q:idem:ready q:idem:dedup:order-42 >/dev/null
rm -f /tmp/relay-demo
```
Expected: ready depth 1; four duplicate-dropped lines.

---

## Notes for the implementer

- **Backward compatibility:** `Enqueue`'s signature is unchanged; `ErrDuplicate` is only ever returned for a keyed enqueue whose marker already exists, so all existing keyless callers and tests are unaffected.
- **Atomicity:** the dedup `SET NX` and the `HSET`+`ZADD` are one Lua script — never split them into separate round-trips (that reintroduces a crash window).
- **`enqueue.lua` is the only enqueue path now** — both keyless and keyed jobs go through it; do not reintroduce a `TxPipeline` branch.
- **TTL clamp:** `SET EX` needs a positive integer; `Enqueue` clamps sub-second `dedupTTL` to 1s.
- Test DBs: broker tests use Redis DB 15 — do not change.
