# Relay Phase 2a — Delayed Jobs, Promoter, Backoff — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add scheduled/delayed jobs, a promoter that releases them, and rework `nack` to retry with exponential backoff + full jitter.

**Architecture:** Introduce a per-queue `q:{name}:delayed` ZSET (score = ready-at ms) as the holding area for both scheduled enqueues and backoff retries. A `Promote` Lua pass (mirroring the reaper) moves due jobs to `ready`; a `worker.Promoter` loop runs it on a tick. `Nack` computes a full-jitter backoff in Go and routes retries into `delayed` instead of straight to `ready`.

**Tech Stack:** Go, Redis (go-redis v9), Lua via `go:embed`. Tests run against a real Redis.

**Spec:** [`docs/superpowers/specs/2026-06-07-relay-phase2a-delayed-jobs-promoter-backoff-design.md`](../specs/2026-06-07-relay-phase2a-delayed-jobs-promoter-backoff-design.md)

**Prerequisites:** A Redis must be reachable at `localhost:6379` (or `REDIS_ADDR`) for the broker/worker tests — they skip otherwise, which would hide failures. Confirm with `redis-cli ping` → `PONG`.

---

## File Structure

- `internal/job/job.go` — modify: add `StateDelayed`.
- `internal/broker/backoff.go` — create: pure `backoffCeiling` + `nextBackoff`.
- `internal/broker/backoff_test.go` — create: white-box unit tests (package `broker`).
- `internal/broker/broker.go` — modify: `delayedKey`; `EnqueueOption`/`WithDelay`/`WithReadyAt`; `Enqueue` rework; `New` variadic + `Option`/`WithBackoff` + rand fields; `Promote`; `Nack` rework; `defaultPromoteBatch`.
- `internal/broker/scripts.go` — modify: embed `promoteScript`.
- `internal/broker/scripts/promote.lua` — create.
- `internal/broker/scripts/nack.lua` — modify: retry routes to `delayed` at a passed ready-at.
- `internal/broker/broker_test.go` — modify: enqueue-delay tests, promote tests, nack-backoff test, update `TestNackRequeuesWhenRetriesRemain`.
- `internal/worker/loop.go` — create: shared `runDrainLoop`.
- `internal/worker/reaper.go` — modify: use `runDrainLoop`, drop `reapAll`.
- `internal/worker/promoter.go` — create: `Promoter`.
- `internal/worker/promoter_test.go` — create: promoter loop test (package `worker_test`).
- `cmd/worker/main.go` — modify: promoter goroutine + backoff/promote flags.
- `cmd/demo/main.go` — modify: `--delay` flag.

---

## Task 1: Backoff math (pure functions)

**Files:**
- Create: `internal/broker/backoff.go`
- Test: `internal/broker/backoff_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/broker/backoff_test.go` (white-box — `package broker` — so it can call the unexported functions):

```go
package broker

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoffCeilingGrowsAndCaps(t *testing.T) {
	base := time.Second
	maxDelay := 10 * time.Second
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Second},  // treated as attempt 1
		{1, 1 * time.Second},  // base * 2^0
		{2, 2 * time.Second},  // base * 2^1
		{3, 4 * time.Second},  // base * 2^2
		{4, 8 * time.Second},  // base * 2^3
		{5, 10 * time.Second}, // 16s capped to 10s
		{10, 10 * time.Second},
	}
	for _, c := range cases {
		if got := backoffCeiling(c.attempts, base, maxDelay); got != c.want {
			t.Errorf("backoffCeiling(%d) = %v, want %v", c.attempts, got, c.want)
		}
	}
}

func TestNextBackoffWithinCeiling(t *testing.T) {
	base := time.Second
	maxDelay := 10 * time.Second
	r := rand.New(rand.NewSource(1))
	for _, attempts := range []int{1, 2, 3, 5, 10} {
		ceil := backoffCeiling(attempts, base, maxDelay)
		for i := 0; i < 100; i++ {
			d := nextBackoff(attempts, base, maxDelay, r)
			if d < 0 || d >= ceil {
				t.Fatalf("attempts=%d: nextBackoff=%v out of [0,%v)", attempts, d, ceil)
			}
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestBackoffCeiling|TestNextBackoff' 2>&1 | head`
Expected: build failure — `undefined: backoffCeiling`, `undefined: nextBackoff`.

- [ ] **Step 3: Write minimal implementation**

Create `internal/broker/backoff.go`:

```go
package broker

import (
	"math/rand"
	"time"
)

// backoffCeiling is the maximum delay for the given attempt: base doubled once
// per attempt, clamped to maxDelay. It doubles in a loop (stopping at the cap)
// rather than computing base*2^n directly, which would overflow for large n.
func backoffCeiling(attempts int, base, maxDelay time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	exp := base
	for i := 1; i < attempts && exp < maxDelay; i++ {
		exp *= 2
	}
	if exp > maxDelay {
		exp = maxDelay
	}
	return exp
}

// nextBackoff returns a full-jitter delay: a uniform random duration in
// [0, backoffCeiling). Full jitter (AWS-style) spreads synchronized retries to
// avoid a thundering herd. Pure given r, so tests can seed r deterministically.
func nextBackoff(attempts int, base, maxDelay time.Duration, r *rand.Rand) time.Duration {
	ceil := backoffCeiling(attempts, base, maxDelay)
	if ceil <= 0 {
		return 0
	}
	return time.Duration(r.Int63n(int64(ceil)))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestBackoffCeiling|TestNextBackoff' -v 2>&1 | tail`
Expected: PASS for both.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/backoff.go internal/broker/backoff_test.go
git commit -m "Add full-jitter backoff math for retries"
```

---

## Task 2: Delayed enqueue (functional options + delayed routing)

**Files:**
- Modify: `internal/job/job.go` (add `StateDelayed`)
- Modify: `internal/broker/broker.go` (add `delayedKey`, `EnqueueOption`, `WithDelay`, `WithReadyAt`; rework `Enqueue`)
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Add the `StateDelayed` constant**

In `internal/job/job.go`, find the state const block:

```go
	// StateDead is a job that exhausted its retry budget and was moved to the
	// dead-letter queue for inspection.
	StateDead State = "dead"
)
```

Insert before the closing `)`:

```go
	// StateDelayed is a job waiting in the delayed set for its ready-at time —
	// either scheduled with a delay or waiting out a backoff after a failure.
	StateDelayed State = "delayed"
```

- [ ] **Step 2: Write the failing tests**

In `internal/broker/broker_test.go`, add these three tests (after `TestEnqueueAddsToReady`):

```go
func TestEnqueuePlainSetsReadyState(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	state, _ := rdb.HGet(ctx, "job:"+j.ID, "state").Result()
	if state != string(job.StateReady) {
		t.Errorf("state = %q, want %q", state, job.StateReady)
	}
}

func TestEnqueueWithDelayGoesToDelayed(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	const delay = time.Minute
	before := time.Now().UnixMilli()
	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithDelay(delay)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	after := time.Now().UnixMilli()

	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 0 {
		t.Errorf("ready size = %d, want 0 (job is delayed)", n)
	}
	score, err := rdb.ZScore(ctx, "q:emails:delayed", j.ID).Result()
	if err != nil {
		t.Fatalf("job not in delayed set: %v", err)
	}
	lo, hi := float64(before+delay.Milliseconds()), float64(after+delay.Milliseconds())
	if score < lo || score > hi {
		t.Errorf("delayed score = %v, want within [%v, %v]", score, lo, hi)
	}
	state, _ := rdb.HGet(ctx, "job:"+j.ID, "state").Result()
	if state != string(job.StateDelayed) {
		t.Errorf("state = %q, want %q", state, job.StateDelayed)
	}
}

func TestEnqueueWithPastReadyAtGoesToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithReadyAt(time.Now().Add(-time.Hour))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 0 {
		t.Errorf("delayed size = %d, want 0 (ready-at is in the past)", n)
	}
	members, _ := rdb.ZRange(ctx, "q:emails:ready", 0, -1).Result()
	if len(members) != 1 || members[0] != j.ID {
		t.Errorf("ready set = %v, want [%s]", members, j.ID)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestEnqueuePlainSetsReadyState|TestEnqueueWithDelayGoesToDelayed|TestEnqueueWithPastReadyAtGoesToReady' 2>&1 | head`
Expected: build failure — `undefined: broker.WithDelay`, `broker.WithReadyAt`.

- [ ] **Step 4: Add `delayedKey` helper**

In `internal/broker/broker.go`, after the `dlqKey` function:

```go
// delayedKey is the Redis key for a queue's delayed set: `q:{name}:delayed`, a
// ZSET scored by each job's ready-at time. The promoter scans it.
func delayedKey(queue string) string { return "q:" + queue + ":delayed" }
```

- [ ] **Step 5: Add enqueue options and rework `Enqueue`**

In `internal/broker/broker.go`, replace the entire `Enqueue` method with:

```go
// enqueueConfig holds resolved enqueue options. A zero readyAt means "now".
type enqueueConfig struct {
	readyAt time.Time
}

// EnqueueOption customises a single Enqueue call.
type EnqueueOption func(*enqueueConfig)

// WithDelay schedules the job to become claimable after d from now. A d <= 0 is
// equivalent to a plain enqueue.
func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		if d > 0 {
			c.readyAt = time.Now().Add(d)
		}
	}
}

// WithReadyAt schedules the job to become claimable at t. A t at or before now
// is equivalent to a plain enqueue.
func WithReadyAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) { c.readyAt = t }
}

// Enqueue makes a job available for workers to claim. With no option it goes
// straight to the ready set; with a future ready-at it goes to the delayed set
// for the promoter to release later. The job hash and the set membership are
// written in one transaction so a crash can never leave one without the other.
func (b *Broker) Enqueue(ctx context.Context, j job.Job, opts ...EnqueueOption) error {
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	pipe := b.rdb.TxPipeline()
	if cfg.readyAt.After(time.Now()) {
		j.State = job.StateDelayed
		pipe.HSet(ctx, jobKey(j.ID), j.ToHash())
		pipe.ZAdd(ctx, delayedKey(j.Queue), redis.Z{Score: float64(cfg.readyAt.UnixMilli()), Member: j.ID})
	} else {
		j.State = job.StateReady
		pipe.HSet(ctx, jobKey(j.ID), j.ToHash())
		pipe.ZAdd(ctx, readyKey(j.Queue), redis.Z{Score: 0, Member: j.ID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("broker: enqueuing job %s: %w", j.ID, err)
	}
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestEnqueue' -v 2>&1 | tail -20`
Expected: PASS for all `TestEnqueue*` (the new three plus the existing `TestEnqueuePersistsJob` and `TestEnqueueAddsToReady`).

- [ ] **Step 7: Commit**

```bash
git add internal/job/job.go internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add delayed enqueue via functional options"
```

---

## Task 3: Promoter pass (promote.lua + broker.Promote)

**Files:**
- Create: `internal/broker/scripts/promote.lua`
- Modify: `internal/broker/scripts.go` (embed)
- Modify: `internal/broker/broker.go` (add `defaultPromoteBatch`, `Promote`)
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/broker/broker_test.go`, add:

```go
func TestPromoteMovesDueJob(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Fast-forward: rewrite the ready-at score into the past so it is due now.
	if err := rdb.ZAdd(ctx, "q:emails:delayed",
		redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: j.ID}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	n, err := b.Promote(ctx, "emails")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 1 {
		t.Errorf("promoted %d jobs, want 1", n)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); c != 0 {
		t.Errorf("delayed size = %d, want 0 after promote", c)
	}
	members, _ := rdb.ZRange(ctx, "q:emails:ready", 0, -1).Result()
	if len(members) != 1 || members[0] != j.ID {
		t.Errorf("ready set = %v, want promoted [%s]", members, j.ID)
	}
	state, _ := rdb.HGet(ctx, "job:"+j.ID, "state").Result()
	if state != string(job.StateReady) {
		t.Errorf("state = %q, want %q", state, job.StateReady)
	}
}

func TestPromoteLeavesNotDueJobs(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	n, err := b.Promote(ctx, "emails")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 0 {
		t.Errorf("promoted %d jobs, want 0 (none due)", n)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); c != 1 {
		t.Errorf("delayed size = %d, want 1 (still scheduled)", c)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); c != 0 {
		t.Errorf("ready size = %d, want 0 (must not promote early)", c)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestPromote' 2>&1 | head`
Expected: build failure — `b.Promote undefined`.

- [ ] **Step 3: Create the promote Lua script**

Create `internal/broker/scripts/promote.lua`:

```lua
-- promote.lua — release delayed jobs whose ready-at time has arrived.
--
-- The mirror image of reaper.lua: instead of recovering past-due inflight jobs,
-- it moves due delayed jobs (scheduled jobs and backoff retries) into ready so a
-- worker can claim them. Bounded per call so a large backlog cannot block Redis.
--
-- KEYS[1] = delayed set q:{name}:delayed (ZSET scored by ready-at)
-- KEYS[2] = ready set    q:{name}:ready
-- ARGV[1] = now in unix milliseconds
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = max jobs to promote in this pass
--
-- Returns the number of jobs promoted.

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', prefix .. id, 'state', 'ready')
  redis.call('ZADD', KEYS[2], 0, id)
end

return #due
```

- [ ] **Step 4: Embed the script**

In `internal/broker/scripts.go`, after the `reaperScript` block:

```go
//go:embed scripts/promote.lua
var promoteSrc string

var promoteScript = redis.NewScript(promoteSrc)
```

- [ ] **Step 5: Add `defaultPromoteBatch` and the `Promote` method**

In `internal/broker/broker.go`, after the `defaultReapBatch` const add:

```go
// defaultPromoteBatch bounds how many jobs a single Promote pass releases, so
// one call cannot block Redis scanning a huge delayed backlog. Callers loop
// until it returns 0 to drain everything due.
const defaultPromoteBatch = 100
```

Then, after the `Reap` method, add:

```go
// Promote releases delayed jobs whose ready-at time has passed, moving them from
// the delayed set back to ready (atomically, in promote.lua). It returns the
// number promoted in this pass; a return of defaultPromoteBatch means more may
// remain, so call again.
func (b *Broker) Promote(ctx context.Context, queue string) (int, error) {
	n, err := promoteScript.Run(ctx, b.rdb,
		[]string{delayedKey(queue), readyKey(queue)},
		time.Now().UnixMilli(), jobKeyPrefix, defaultPromoteBatch,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("broker: promoting %q: %w", queue, err)
	}
	return n, nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestPromote' -v 2>&1 | tail`
Expected: PASS for both.

- [ ] **Step 7: Prove the not-due test is non-vacuous**

Temporarily edit `internal/broker/scripts/promote.lua`: change `'-inf', now,` to `'-inf', '+inf',`. Run `go test ./internal/broker/ -run 'TestPromote' 2>&1 | tail`. Expected: `TestPromoteLeavesNotDueJobs` FAILS (it promotes the not-due job). Then **revert** the edit and re-run to confirm both PASS again.

- [ ] **Step 8: Commit**

```bash
git add internal/broker/scripts/promote.lua internal/broker/scripts.go internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add promoter pass to release due delayed jobs"
```

---

## Task 4: Backoff config + nack rework

**Files:**
- Modify: `internal/broker/broker.go` (`Broker` fields, `New` variadic, `Option`, `WithBackoff`, `Nack`)
- Modify: `internal/broker/scripts/nack.lua`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Update the existing nack test and add a backoff test**

In `internal/broker/broker_test.go`, **replace** the body of `TestNackRequeuesWhenRetriesRemain` with the version below (retry now lands in `delayed`, not `ready`), and add `TestNackBackoffReadyAtWithinCeiling` after it:

```go
func TestNackRequeuesWhenRetriesRemain(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// Default MaxRetries is 5; after one claim Attempts is 1, so retries remain.
	claimed := claimOne(t, b, job.New("emails", []byte("hello")))
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 0 {
		t.Errorf("inflight size = %d, want 0 after nack", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 0 {
		t.Errorf("ready size = %d, want 0 (retry waits in delayed)", n)
	}
	if n, _ := rdb.LLen(ctx, "q:emails:dlq").Result(); n != 0 {
		t.Errorf("dlq size = %d, want 0 (retries remain)", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 1 {
		t.Errorf("delayed size = %d, want 1 (retry scheduled)", n)
	}
	state, _ := rdb.HGet(ctx, "job:"+claimed.ID, "state").Result()
	if state != string(job.StateDelayed) {
		t.Errorf("job state = %q, want %q", state, job.StateDelayed)
	}
}

func TestNackBackoffReadyAtWithinCeiling(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// Default backoff base is 1s; after one claim Attempts is 1, so the ceiling
	// is 1s and the jittered ready-at lands within (now, now+1s].
	claimed := claimOne(t, b, job.New("emails", []byte("hello")))
	before := time.Now().UnixMilli()
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	score, err := rdb.ZScore(ctx, "q:emails:delayed", claimed.ID).Result()
	if err != nil {
		t.Fatalf("job not in delayed set: %v", err)
	}
	lo, hi := float64(before), float64(time.Now().Add(time.Second).UnixMilli())
	if score < lo || score > hi {
		t.Errorf("delayed ready-at = %v, want within [%v, %v]", score, lo, hi)
	}
}

func TestNackThenPromoteRedelivers(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// Claim, fail, and nack -> the job waits in delayed under a backoff.
	first := claimOne(t, b, job.New("emails", []byte("hello")))
	if err := b.Nack(ctx, first); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	// Fast-forward the backoff so the retry is due now.
	if err := rdb.ZAdd(ctx, "q:emails:delayed",
		redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: first.ID}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if n, err := b.Promote(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Promote: n=%d err=%v, want 1", n, err)
	}

	// A second claim re-delivers the same job with its attempt count advanced.
	second, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("re-Claim: err=%v ok=%v", err, ok)
	}
	if second.ID != first.ID {
		t.Errorf("re-claimed %s, want the same job %s", second.ID, first.ID)
	}
	if second.Attempts != 2 {
		t.Errorf("re-claim Attempts = %d, want 2", second.Attempts)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestNackRequeuesWhenRetriesRemain|TestNackBackoffReadyAtWithinCeiling|TestNackThenPromoteRedelivers' 2>&1 | tail`
Expected: FAIL — `TestNackRequeuesWhenRetriesRemain` finds the job in `ready` (old behavior, delayed size 0); `TestNackBackoffReadyAtWithinCeiling` and `TestNackThenPromoteRedelivers` find it not in `delayed`.

- [ ] **Step 3: Add backoff fields, `Option`, `WithBackoff`, and rework `New`**

In `internal/broker/broker.go`, update the imports to add `"math/rand"` and `"sync"`:

```go
import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/job"
)
```

Replace the `Broker` struct and `New` function with:

```go
// Broker talks to a single Redis instance. It is safe for concurrent use: all
// queue state lives in Redis, and the only mutable in-process state is the
// jitter source, which is mutex-guarded.
type Broker struct {
	rdb         *redis.Client
	backoffBase time.Duration
	backoffMax  time.Duration

	rndMu sync.Mutex
	rnd   *rand.Rand
}

// Option customises a Broker at construction.
type Option func(*Broker)

// WithBackoff sets the retry backoff base and ceiling. The nth retry waits a
// full-jitter delay in [0, min(maxDelay, base*2^(n-1))).
func WithBackoff(base, maxDelay time.Duration) Option {
	return func(b *Broker) {
		b.backoffBase = base
		b.backoffMax = maxDelay
	}
}

// New returns a Broker backed by the given Redis client. Defaults: backoff base
// 1s, ceiling 10m.
func New(rdb *redis.Client, opts ...Option) *Broker {
	b := &Broker{
		rdb:         rdb,
		backoffBase: time.Second,
		backoffMax:  10 * time.Minute,
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}
```

- [ ] **Step 4: Rework the `Nack` method**

In `internal/broker/broker.go`, replace the entire `Nack` method with:

```go
// Nack reports that a claimed job failed. If the job still has attempts left it
// is requeued to the delayed set with a full-jitter backoff so the retry waits;
// once its retry budget is spent it is moved to the dead-letter queue. The
// decision and the move are atomic in nack.lua; the backoff delay is computed
// here (jitter needs randomness) and passed in as a ready-at timestamp.
func (b *Broker) Nack(ctx context.Context, j job.Job) error {
	b.rndMu.Lock()
	delay := nextBackoff(j.Attempts, b.backoffBase, b.backoffMax, b.rnd)
	b.rndMu.Unlock()
	readyAt := time.Now().Add(delay).UnixMilli()

	if err := nackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), delayedKey(j.Queue), dlqKey(j.Queue)},
		j.ID, jobKeyPrefix, readyAt,
	).Err(); err != nil {
		return fmt.Errorf("broker: nacking job %s: %w", j.ID, err)
	}
	return nil
}
```

- [ ] **Step 5: Rework the nack Lua script**

Replace the body of `internal/broker/scripts/nack.lua` (keep the file's leading comment style; the key change is the retry branch and the new ARGV) with:

```lua
-- nack.lua — handle a failed delivery.
--
-- Always removes the job from the inflight set, then decides its fate from the
-- attempt count on the job hash (claim bumps it): retries left -> requeue to the
-- delayed set at a caller-computed ready-at (the backoff), so the retry waits;
-- budget spent -> move to the dead-letter queue. Reading the counts from the
-- hash here keeps the decision atomic with the move.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- KEYS[2] = delayed set  q:{name}:delayed
-- KEYS[3] = dead-letter  q:{name}:dlq
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = retry ready-at in unix milliseconds (precomputed backoff)
--
-- Returns 'retry' or 'dead'.

local id = ARGV[1]
local job_key = ARGV[2] .. id
local ready_at = tonumber(ARGV[3])

redis.call('ZREM', KEYS[1], id)

local attempts = tonumber(redis.call('HGET', job_key, 'attempts')) or 0
local max_retries = tonumber(redis.call('HGET', job_key, 'max_retries')) or 0

if attempts < max_retries then
  redis.call('HSET', job_key, 'state', 'delayed')
  redis.call('ZADD', KEYS[2], ready_at, id)
  return 'retry'
end

redis.call('HSET', job_key, 'state', 'dead')
redis.call('RPUSH', KEYS[3], id)
return 'dead'
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestNack' -v 2>&1 | tail -20`
Expected: PASS for `TestNackRequeuesWhenRetriesRemain`, `TestNackBackoffReadyAtWithinCeiling`, and `TestNackDeadLettersWhenExhausted` (the dead path is unchanged).

- [ ] **Step 7: Run the whole broker suite under race**

Run: `go test -race ./internal/broker/ 2>&1 | tail`
Expected: `ok` — confirms the mutex-guarded rand is race-clean.

- [ ] **Step 8: Commit**

```bash
git add internal/broker/broker.go internal/broker/scripts/nack.lua internal/broker/broker_test.go
git commit -m "Rework nack to retry with full-jitter backoff via delayed set"
```

---

## Task 5: Promoter loop (shared drain loop + Promoter)

**Files:**
- Create: `internal/worker/loop.go`
- Modify: `internal/worker/reaper.go` (use shared loop, drop `reapAll`)
- Create: `internal/worker/promoter.go`
- Create: `internal/worker/promoter_test.go`

- [ ] **Step 1: Write the failing promoter test**

Create `internal/worker/promoter_test.go`:

```go
package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
	"github.com/StrangeNoob/relay/internal/worker"
)

func TestPromoterRunPromotesDueJobs(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Make it due now.
	if err := rdb.ZAdd(ctx, "q:emails:delayed",
		redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: j.ID}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	p := worker.NewPromoter(b, "emails", 20*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("promoter did not promote the due job within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); c != 0 {
		t.Errorf("delayed size = %d, want 0 after promote", c)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/worker/ -run TestPromoterRunPromotesDueJobs 2>&1 | head`
Expected: build failure — `undefined: worker.NewPromoter`.

- [ ] **Step 3: Create the shared drain loop**

Create `internal/worker/loop.go`:

```go
package worker

import (
	"context"
	"log/slog"
	"time"
)

// drainPass runs one bounded pass of a maintenance operation (reap or promote)
// and reports how many items it moved.
type drainPass func(ctx context.Context) (int, error)

// runDrainLoop periodically runs pass until ctx is cancelled, then returns nil.
// Each tick drains fully — it repeats pass until a pass moves nothing — so a
// burst is cleared promptly rather than one batch per interval. Transient errors
// are logged (with name for context) and retried on the next tick rather than
// killing the loop.
func runDrainLoop(ctx context.Context, interval time.Duration, logger *slog.Logger, name string, pass drainPass) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		for {
			n, err := pass(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("relay "+name+": pass failed", "err", err)
				break
			}
			if n == 0 {
				break
			}
		}
		if !wait(ctx, interval) {
			return nil
		}
	}
}
```

- [ ] **Step 4: Refactor the reaper onto the shared loop**

Replace the `Run` method and the `reapAll` method in `internal/worker/reaper.go` with a single `Run`:

```go
// Run requeues expired jobs every interval until ctx is cancelled, then returns
// nil. Each tick drains all currently-expired jobs before sleeping again.
func (r *Reaper) Run(ctx context.Context) error {
	return runDrainLoop(ctx, r.interval, r.logger, "reaper", func(c context.Context) (int, error) {
		return r.broker.Reap(c, r.queue)
	})
}
```

(Delete the old `reapAll` method entirely.)

- [ ] **Step 5: Create the Promoter**

Create `internal/worker/promoter.go`:

```go
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
)

// Promoter periodically releases delayed jobs whose ready-at time has arrived.
// It is the scheduling counterpart to the reaper: the reaper recovers timed-out
// in-flight work, the promoter releases scheduled and backed-off work. Run one
// per queue.
type Promoter struct {
	broker   *broker.Broker
	queue    string
	interval time.Duration
	logger   *slog.Logger
}

// NewPromoter builds a promoter that scans queue every interval.
func NewPromoter(b *broker.Broker, queue string, interval time.Duration) *Promoter {
	return &Promoter{
		broker:   b,
		queue:    queue,
		interval: interval,
		logger:   slog.Default(),
	}
}

// Run promotes due delayed jobs every interval until ctx is cancelled, then
// returns nil. Each tick drains all currently-due jobs before sleeping again.
func (p *Promoter) Run(ctx context.Context) error {
	return runDrainLoop(ctx, p.interval, p.logger, "promoter", func(c context.Context) (int, error) {
		return p.broker.Promote(c, p.queue)
	})
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -race ./internal/worker/ 2>&1 | tail`
Expected: `ok` — the new promoter test passes and all existing reaper/worker tests still pass after the refactor.

- [ ] **Step 7: Commit**

```bash
git add internal/worker/loop.go internal/worker/reaper.go internal/worker/promoter.go internal/worker/promoter_test.go
git commit -m "Add promoter loop on a shared drain-loop helper"
```

---

## Task 6: Wire the promoter and backoff into cmd

**Files:**
- Modify: `cmd/worker/main.go`
- Modify: `cmd/demo/main.go`

This task is thin `main` wiring over tested packages, so it is verified by build/vet rather than unit tests.

- [ ] **Step 1: Add flags and the promoter goroutine to `cmd/worker`**

In `cmd/worker/main.go`, add three flags in the `flag` block (after `reapInterval`):

```go
	promoteInterval := flag.Duration("promote-interval", time.Second, "how often the promoter releases due delayed jobs")
	backoffBase := flag.Duration("backoff-base", time.Second, "retry backoff base delay")
	backoffMax := flag.Duration("backoff-max", 10*time.Minute, "retry backoff ceiling")
```

Change the broker construction to pass the backoff config:

```go
	b := broker.New(rdb, broker.WithBackoff(*backoffBase, *backoffMax))
```

After the reaper goroutine block, add a promoter goroutine:

```go
	p := worker.NewPromoter(b, *queue, *promoteInterval)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Run(ctx); err != nil {
			logger.Error("promoter exited with error", "err", err)
		}
	}()
```

- [ ] **Step 2: Add a `--delay` flag to `cmd/demo`**

In `cmd/demo/main.go`, add a flag in the `flag` block:

```go
	delay := flag.Duration("delay", 0, "schedule jobs this far in the future (0 = immediate)")
```

Change the enqueue call inside the loop to pass the option when a delay is set:

```go
		var opts []broker.EnqueueOption
		if *delay > 0 {
			opts = append(opts, broker.WithDelay(*delay))
		}
		if err := b.Enqueue(ctx, j, opts...); err != nil {
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
```

- [ ] **Step 3: Verify build and vet**

Run: `go build ./... && go vet ./... && echo OK`
Expected: `OK` with no errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/worker/main.go cmd/demo/main.go
git commit -m "Wire promoter and backoff config into worker and demo commands"
```

---

## Task 7: Full verification

- [ ] **Step 1: Run the whole suite under race**

Run: `go test -race ./... 2>&1 | tail`
Expected: `ok` for `internal/broker`, `internal/job`, `internal/worker`; `no test files` for the two `cmd` packages.

- [ ] **Step 2: Lint and format**

Run: `gofmt -l internal/ cmd/ ; go vet ./... && golangci-lint run ./... 2>&1 | tail`
Expected: empty `gofmt` output, no vet errors, `0 issues` from golangci-lint.

- [ ] **Step 3: (Optional) end-to-end smoke with a delay**

With Redis running:
```bash
go run ./cmd/demo -queue p2demo -count 10 -delay 3s
go run ./cmd/worker -queue p2demo -concurrency 2 -fail-rate 0.3 &
# watch: jobs appear ~3s later (promoter), failures wait then retry (backoff)
sleep 8; kill %1
```
Expected: jobs are processed starting ~3s after enqueue; failed jobs are retried after a short delay rather than immediately.

---

## Notes for the implementer

- **Backward compatibility:** `broker.New` becomes variadic and `Enqueue` gains variadic options; existing call sites (`broker.New(rdb)`, `b.Enqueue(ctx, j)`) keep compiling unchanged.
- **Test DBs:** broker tests use Redis DB 15, worker tests DB 14 — do not change this; it isolates the parallel package test binaries.
- **`maxDelay`, not `max`/`cap`:** the parameter names avoid shadowing Go's `max`/`cap` builtins.
- **Don't bump attempts on promote** (mirrors the reaper) — the next claim counts the redelivery.
