# Relay Phase 2b — Priority Queues — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `ready` set honor per-job priority — higher priority claimed first, oldest-first within a priority — without changing the claim path.

**Architecture:** A job gains a clamped `Priority` (0–255). Wherever a job enters `ready` (enqueue, promote, reaper) it is scored `priority*10^13 - nowMs`; `claim.lua` already pops `ZPOPMAX`, so the highest score (highest priority, then oldest) is claimed. The scale is a Go constant passed to the Lua scripts.

**Tech Stack:** Go, Redis (go-redis v9), Lua via `go:embed`. Tests run against a real Redis.

**Spec:** [`docs/superpowers/specs/2026-06-07-relay-phase2b-priority-queues-design.md`](../specs/2026-06-07-relay-phase2b-priority-queues-design.md)

**Prerequisites:** Redis reachable at `localhost:6379` (or `REDIS_ADDR`) — broker tests skip without it, hiding failures. Confirm `redis-cli ping` → `PONG`. Work happens on the current branch/worktree.

---

## File Structure

- `internal/job/job.go` — modify: add `Priority` field, `priority` hash field, encode/decode.
- `internal/job/job_test.go` — modify: add priority tests.
- `internal/broker/priority.go` — create: `MaxPriority`, `priorityScale`, `clampPriority`, `readyScore`.
- `internal/broker/priority_test.go` — create: white-box unit tests (package `broker`).
- `internal/broker/broker.go` — modify: `enqueueConfig` priority fields, `WithPriority`, `Enqueue` scoring + clamp, `Promote`/`Reap` pass the scale ARGV.
- `internal/broker/broker_test.go` — modify: enqueue/claim priority tests, promote/reap preservation tests.
- `internal/broker/scripts/promote.lua` — modify: priority-aware ready score.
- `internal/broker/scripts/reaper.lua` — modify: priority-aware ready score.
- `cmd/demo/main.go` — modify: `--priority` flag.
- `CLAUDE.md` — modify: reflect priority shipped.

`claim.lua`, `nack.lua`, `ack.lua`, `heartbeat.lua`, `internal/worker/*` are unchanged.

---

## Task 1: Job priority field

**Files:**
- Modify: `internal/job/job.go`
- Test: `internal/job/job_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/job/job_test.go`:

```go
func TestNewDefaultsPriorityZero(t *testing.T) {
	j := job.New("emails", []byte("x"))
	if j.Priority != 0 {
		t.Errorf("Priority = %d, want 0", j.Priority)
	}
}

func TestPriorityRoundTrip(t *testing.T) {
	want := job.New("emails", []byte("x"))
	want.Priority = 7

	got, err := job.FromHash(want.ToHash())
	if err != nil {
		t.Fatalf("FromHash: %v", err)
	}
	if got.Priority != 7 {
		t.Errorf("Priority = %d, want 7", got.Priority)
	}
}

func TestFromHashRejectsMalformedPriority(t *testing.T) {
	h := job.New("emails", []byte("x")).ToHash()
	h["priority"] = "not-a-number"
	if _, err := job.FromHash(h); err == nil {
		t.Error("FromHash with malformed priority = nil error, want error")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/job/ -run 'TestNewDefaultsPriorityZero|TestPriorityRoundTrip|TestFromHashRejectsMalformedPriority' 2>&1 | head`
Expected: build failure — `j.Priority undefined` / `want.Priority undefined`.

- [ ] **Step 3: Add the `Priority` field**

In `internal/job/job.go`, in the `Job` struct, add `Priority` after `MaxRetries`:

```go
	MaxRetries int       // attempts allowed before the job is dead-lettered
	Priority   int       // claim order; higher is more urgent (clamped 0-255 at enqueue)
	CreatedAt  time.Time // wall-clock time the job was constructed
```

In `New`, set the default in the returned struct literal (after `MaxRetries: DefaultMaxRetries,`):

```go
		MaxRetries: DefaultMaxRetries,
		Priority:   0,
		CreatedAt:  time.Now(),
```

- [ ] **Step 4: Encode and decode `priority`**

In `internal/job/job.go`, add the field-name constant to the hash-field `const` block (after `fieldMaxRetries`):

```go
	fieldMaxRetries  = "max_retries"
	fieldPriority    = "priority"
	fieldCreatedAt   = "created_at"
```

In `ToHash`, add the entry (after the `fieldMaxRetries` line):

```go
		fieldMaxRetries:  strconv.Itoa(j.MaxRetries),
		fieldPriority:    strconv.Itoa(j.Priority),
		fieldCreatedAt:   j.CreatedAt.Format(time.RFC3339Nano),
```

In `FromHash`, parse it with a wrapped error (after the `maxRetries` block, before building the returned `Job`):

```go
	priority, err := strconv.Atoi(h[fieldPriority])
	if err != nil {
		return Job{}, fmt.Errorf("job: parsing %s %q: %w", fieldPriority, h[fieldPriority], err)
	}
```

And set it in the returned `Job` literal (after `MaxRetries: maxRetries,`):

```go
		MaxRetries:     maxRetries,
		Priority:       priority,
		CreatedAt:      createdAt,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/job/ 2>&1 | tail -3`
Expected: `ok` (the three new tests plus all existing job tests pass — `gofmt -w internal/job/job.go` first if struct alignment changed).

- [ ] **Step 6: Commit**

```bash
gofmt -w internal/job/job.go
git add internal/job/job.go internal/job/job_test.go
git commit -m "Add priority field to job model"
```

---

## Task 2: Ready-score math (pure helpers)

**Files:**
- Create: `internal/broker/priority.go`
- Test: `internal/broker/priority_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/broker/priority_test.go` (white-box — `package broker`):

```go
package broker

import "testing"

func TestReadyScoreOrdering(t *testing.T) {
	const now = int64(1_700_000_000_000)

	// Higher priority scores higher (ZPOPMAX claims it first).
	if readyScore(5, now) <= readyScore(1, now) {
		t.Error("higher priority should score higher")
	}
	// Within a priority, the older job (smaller now) scores higher → FIFO.
	if readyScore(3, now) <= readyScore(3, now+1000) {
		t.Error("older job should score higher within a priority")
	}
	// Priority dominates time: priority 1 far in the future still beats priority 0 now.
	if readyScore(1, now+1_000_000_000) <= readyScore(0, now) {
		t.Error("priority must dominate the time tiebreak")
	}
}

func TestClampPriority(t *testing.T) {
	cases := map[int]int{-5: 0, 0: 0, 100: 100, 255: 255, 300: 255}
	for in, want := range cases {
		if got := clampPriority(in); got != want {
			t.Errorf("clampPriority(%d) = %d, want %d", in, got, want)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestReadyScoreOrdering|TestClampPriority' 2>&1 | head`
Expected: build failure — `undefined: readyScore`, `undefined: clampPriority`.

- [ ] **Step 3: Write the implementation**

Create `internal/broker/priority.go`:

```go
package broker

// MaxPriority is the largest accepted priority; higher is more urgent. The range
// is bounded so the composite ready score stays an exact float64 integer
// (MaxPriority*priorityScale stays below 2^53).
const MaxPriority = 255

// priorityScale weights priority above ready-time in the composite ready score.
// It exceeds any realistic now-in-milliseconds (~1.7e12 today, ~2.6e12 in 30
// years), so a higher priority always outranks a lower one regardless of time.
const priorityScale = 10_000_000_000_000 // 1e13

// clampPriority bounds p into [0, MaxPriority].
func clampPriority(p int) int {
	if p < 0 {
		return 0
	}
	if p > MaxPriority {
		return MaxPriority
	}
	return p
}

// readyScore is the ZSET score for a job entering the ready set. Priority
// dominates (descending, so ZPOPMAX claims the most urgent first); subtracting
// the readiness time makes the oldest job of a given priority win the tie (FIFO).
// priority must already be clamped to [0, MaxPriority].
func readyScore(priority int, nowMs int64) float64 {
	return float64(priority)*priorityScale - float64(nowMs)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestReadyScoreOrdering|TestClampPriority' -v 2>&1 | tail`
Expected: PASS for both.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/priority.go internal/broker/priority_test.go
git commit -m "Add composite ready-score helpers for priority"
```

---

## Task 3: Priority on enqueue + claim order

**Files:**
- Modify: `internal/broker/broker.go`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/broker/broker_test.go`:

```go
func TestEnqueueWithPrioritySetsScore(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("x"))
	before := time.Now().UnixMilli()
	if err := b.Enqueue(ctx, j, broker.WithPriority(5)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	after := time.Now().UnixMilli()

	if p, _ := rdb.HGet(ctx, "job:"+j.ID, "priority").Result(); p != "5" {
		t.Errorf("hash priority = %q, want 5", p)
	}
	score, err := rdb.ZScore(ctx, "q:emails:ready", j.ID).Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	// score = 5*1e13 - nowMs, with nowMs in [before, after]
	lo := 5*1e13 - float64(after)
	hi := 5*1e13 - float64(before)
	if score < lo || score > hi {
		t.Errorf("ready score = %v, want within [%v, %v]", score, lo, hi)
	}
}

func TestEnqueueClampsPriority(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	hi := job.New("emails", []byte("hi"))
	if err := b.Enqueue(ctx, hi, broker.WithPriority(300)); err != nil {
		t.Fatalf("Enqueue hi: %v", err)
	}
	if p, _ := rdb.HGet(ctx, "job:"+hi.ID, "priority").Result(); p != "255" {
		t.Errorf("clamped-high priority = %q, want 255", p)
	}

	lo := job.New("emails", []byte("lo"))
	if err := b.Enqueue(ctx, lo, broker.WithPriority(-5)); err != nil {
		t.Fatalf("Enqueue lo: %v", err)
	}
	if p, _ := rdb.HGet(ctx, "job:"+lo.ID, "priority").Result(); p != "0" {
		t.Errorf("clamped-low priority = %q, want 0", p)
	}
}

func TestClaimReturnsHighestPriorityFirst(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	low := job.New("emails", []byte("low"))
	low.Priority = 1
	mid := job.New("emails", []byte("mid"))
	mid.Priority = 5
	high := job.New("emails", []byte("high"))
	high.Priority = 9
	// Enqueue in mixed order; claim must still be priority-ordered.
	for _, j := range []job.Job{mid, low, high} {
		if err := b.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	want := []struct {
		id   string
		prio int
	}{{high.ID, 9}, {mid.ID, 5}, {low.ID, 1}}
	for i, w := range want {
		got, ok, err := b.Claim(ctx, "emails", time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim %d: err=%v ok=%v", i, err, ok)
		}
		if got.ID != w.id || got.Priority != w.prio {
			t.Errorf("claim %d = id %s prio %d, want id %s prio %d", i, got.ID, got.Priority, w.id, w.prio)
		}
	}
}

func TestClaimFIFOWithinSamePriority(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	first := job.New("emails", []byte("first"))
	first.Priority = 5
	if err := b.Enqueue(ctx, first); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // distinct millisecond → deterministic FIFO
	second := job.New("emails", []byte("second"))
	second.Priority = 5
	if err := b.Enqueue(ctx, second); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	got1, _, _ := b.Claim(ctx, "emails", time.Minute)
	got2, _, _ := b.Claim(ctx, "emails", time.Minute)
	if got1.ID != first.ID {
		t.Errorf("first claim = %s, want oldest %s", got1.ID, first.ID)
	}
	if got2.ID != second.ID {
		t.Errorf("second claim = %s, want %s", got2.ID, second.ID)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestEnqueueWithPrioritySetsScore|TestEnqueueClampsPriority|TestClaimReturnsHighestPriorityFirst|TestClaimFIFOWithinSamePriority' 2>&1 | tail`
Expected: build failure — `undefined: broker.WithPriority` (and once that compiles, the claim-order/FIFO tests would fail because the ready score is still `0`).

- [ ] **Step 3: Add priority to `enqueueConfig` and the `WithPriority` option**

In `internal/broker/broker.go`, replace the `enqueueConfig` struct and add `WithPriority` after `WithReadyAt`:

```go
// enqueueConfig holds resolved enqueue options. A zero readyAt means "now".
type enqueueConfig struct {
	readyAt     time.Time
	priority    int
	prioritySet bool
}
```

```go
// WithPriority sets the job's claim priority for this enqueue (higher is more
// urgent), overriding any value already on the job. It is clamped to
// [0, MaxPriority].
func WithPriority(p int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.priority = p
		c.prioritySet = true
	}
}
```

- [ ] **Step 4: Apply priority and the composite score in `Enqueue`**

In `internal/broker/broker.go`, update `Enqueue`. After the option loop, apply/clamp priority; and change the immediate-branch `ZAdd` score from `0` to `readyScore(...)`:

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

	pipe := b.rdb.TxPipeline()
	if cfg.readyAt.After(time.Now()) {
		j.State = job.StateDelayed
		pipe.HSet(ctx, jobKey(j.ID), j.ToHash())
		pipe.ZAdd(ctx, delayedKey(j.Queue), redis.Z{Score: float64(cfg.readyAt.UnixMilli()), Member: j.ID})
	} else {
		j.State = job.StateReady
		pipe.HSet(ctx, jobKey(j.ID), j.ToHash())
		pipe.ZAdd(ctx, readyKey(j.Queue), redis.Z{Score: readyScore(j.Priority, time.Now().UnixMilli()), Member: j.ID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("broker: enqueuing job %s: %w", j.ID, err)
	}
	return nil
}
```

(Only the priority lines and the immediate-branch `ZAdd` score change; the delayed branch is unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestEnqueue|TestClaim' -v 2>&1 | tail -25`
Expected: PASS for the four new tests and all existing `TestEnqueue*`/`TestClaim*` tests (including the concurrency test).

- [ ] **Step 6: Commit**

```bash
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Score ready jobs by priority on enqueue"
```

---

## Task 4: Preserve priority through promote and reap

**Files:**
- Modify: `internal/broker/scripts/promote.lua`
- Modify: `internal/broker/scripts/reaper.lua`
- Modify: `internal/broker/broker.go` (pass the scale ARGV)
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/broker/broker_test.go`:

```go
func TestPromotePreservesPriority(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// A mid-priority job sits in ready; a higher-priority job is delayed.
	mid := job.New("emails", []byte("mid"))
	mid.Priority = 3
	if err := b.Enqueue(ctx, mid); err != nil {
		t.Fatalf("Enqueue mid: %v", err)
	}
	high := job.New("emails", []byte("high"))
	high.Priority = 7
	if err := b.Enqueue(ctx, high, broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue high: %v", err)
	}
	// Make the delayed high-priority job due now.
	if err := rdb.ZAdd(ctx, "q:emails:delayed",
		redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: high.ID}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if n, err := b.Promote(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Promote: n=%d err=%v, want 1", n, err)
	}

	// Both are in ready now; the promoted priority-7 job must be claimed first.
	got, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}
	if got.ID != high.ID || got.Priority != 7 {
		t.Errorf("claimed id=%s prio=%d, want high id=%s prio=7", got.ID, got.Priority, high.ID)
	}
}

func TestReapPreservesPriority(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	// Claim a high-priority job with visibility 0 so it is immediately reapable.
	high := job.New("emails", []byte("high"))
	high.Priority = 8
	if err := b.Enqueue(ctx, high); err != nil {
		t.Fatalf("Enqueue high: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", 0); err != nil || !ok {
		t.Fatalf("Claim high: err=%v ok=%v", err, ok)
	}
	// A mid-priority job waits in ready.
	mid := job.New("emails", []byte("mid"))
	mid.Priority = 3
	if err := b.Enqueue(ctx, mid); err != nil {
		t.Fatalf("Enqueue mid: %v", err)
	}
	if n, err := b.Reap(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Reap: n=%d err=%v, want 1", n, err)
	}

	// The reaped priority-8 job must be claimed before the priority-3 job.
	got, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}
	if got.ID != high.ID || got.Priority != 8 {
		t.Errorf("claimed id=%s prio=%d, want high id=%s prio=8", got.ID, got.Priority, high.ID)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestPromotePreservesPriority|TestReapPreservesPriority' -v 2>&1 | tail`
Expected: FAIL — promote/reaper still write a ready score of `0`, so the priority-3 ready job (positive score `3e13-now`) is claimed before the promoted/reaped job (score `0`), and the assertions on `high` fail.

- [ ] **Step 3: Make `promote.lua` priority-aware**

Replace the body of `internal/broker/scripts/promote.lua` below the header comment. Update the header's ARGV list to add the scale, and the loop to compute the composite score:

```lua
-- KEYS[1] = delayed set q:{name}:delayed (ZSET scored by ready-at)
-- KEYS[2] = ready set    q:{name}:ready
-- ARGV[1] = now in unix milliseconds
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = max jobs to promote in this pass
-- ARGV[4] = priority scale (weights priority above time in the ready score)
--
-- Returns the number of jobs promoted.

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])
local scale = tonumber(ARGV[4])

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(due) do
  local job_key = prefix .. id
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', job_key, 'state', 'ready')
  local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
  redis.call('ZADD', KEYS[2], priority * scale - now, id)
end

return #due
```

- [ ] **Step 4: Make `reaper.lua` priority-aware**

Replace the body of `internal/broker/scripts/reaper.lua` below the header comment. Update the ARGV list to add the scale, and the loop to compute the composite score:

```lua
-- KEYS[1] = inflight set q:{name}:inflight (ZSET scored by deadline)
-- KEYS[2] = ready set    q:{name}:ready
-- ARGV[1] = now in unix milliseconds
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = max jobs to requeue in this pass
-- ARGV[4] = priority scale (weights priority above time in the ready score)
--
-- Returns the number of jobs requeued.

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])
local scale = tonumber(ARGV[4])

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(expired) do
  local job_key = prefix .. id
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', job_key, 'state', 'ready')
  local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
  redis.call('ZADD', KEYS[2], priority * scale - now, id)
end

return #expired
```

(Keep each file's existing top-of-file description comment; only the ARGV doc lines and the loop body change. The `reaper.lua` "Attempts are intentionally NOT bumped" note, if present, stays.)

- [ ] **Step 5: Pass the scale ARGV from `Promote` and `Reap`**

In `internal/broker/broker.go`, add `priorityScale` as the final ARGV in both script runs.

`Promote`:

```go
	n, err := promoteScript.Run(ctx, b.rdb,
		[]string{delayedKey(queue), readyKey(queue)},
		time.Now().UnixMilli(), jobKeyPrefix, defaultPromoteBatch, priorityScale,
	).Int()
```

`Reap`:

```go
	n, err := reaperScript.Run(ctx, b.rdb,
		[]string{inflightKey(queue), readyKey(queue)},
		time.Now().UnixMilli(), jobKeyPrefix, defaultReapBatch, priorityScale,
	).Int()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestPromote|TestReap|TestPromotePreservesPriority|TestReapPreservesPriority' -v 2>&1 | tail -20`
Expected: PASS — the two new preservation tests plus the existing promote/reap tests (which don't assert a specific ready score, so they still pass).

- [ ] **Step 7: Run the whole broker suite under race**

Run: `go test -race ./internal/broker/ 2>&1 | tail`
Expected: `ok`.

- [ ] **Step 8: Commit**

```bash
git add internal/broker/scripts/promote.lua internal/broker/scripts/reaper.lua internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Preserve priority when promoting and reaping into ready"
```

---

## Task 5: Demo flag + docs

**Files:**
- Modify: `cmd/demo/main.go`
- Modify: `CLAUDE.md`

This task is thin wiring + docs, verified by build/vet.

- [ ] **Step 1: Add `--priority` to `cmd/demo`**

In `cmd/demo/main.go`, add a flag in the flag block (near the existing `--delay`):

```go
	priority := flag.Int("priority", 0, "priority for enqueued jobs (higher is more urgent, 0-255)")
```

In the enqueue loop, where the `opts` slice is built (it already appends `broker.WithDelay` when `*delay > 0`), add a priority option when non-zero:

```go
		var opts []broker.EnqueueOption
		if *delay > 0 {
			opts = append(opts, broker.WithDelay(*delay))
		}
		if *priority != 0 {
			opts = append(opts, broker.WithPriority(*priority))
		}
		if err := b.Enqueue(ctx, j, opts...); err != nil {
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
```

(Match the existing variable names in the file; if `opts` is already declared, reuse it rather than redeclaring.)

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`, make these edits to reflect priority shipping:

1. In the `q:{name}:ready` data-model table row, change `claim pops the best score (currently score 0 for all — priority is Phase 2)` to:
   `claim pops the best score = priority (higher first), oldest-first within a priority`
2. In the Build order, in the "Phase 2 — depth (in progress)" line, add `priority ✅` to the completed list (it currently lists `delayed jobs + promoter ✅; backoff + jitter ✅`), and remove `priority` from the still-to-do list.
3. In the "What this is" / status `internal/broker` bullet, change `Enqueue` (with `WithDelay`/`WithReadyAt` options) to `Enqueue (with WithDelay/WithReadyAt/WithPriority options)`.

- [ ] **Step 3: Verify build, vet, format**

Run: `go build ./... && go vet ./... && gofmt -l cmd/ internal/ && echo OK`
Expected: `OK` (empty gofmt output, no errors).

- [ ] **Step 4: Commit**

```bash
git add cmd/demo/main.go CLAUDE.md
git commit -m "Add demo priority flag; document priority queues"
```

---

## Task 6: Full verification

- [ ] **Step 1: Whole suite under race (uncached)**

Run: `go test -race -count=1 ./... 2>&1 | tail`
Expected: `ok` for `internal/broker`, `internal/job`, `internal/worker`; `no test files` for the `cmd` packages.

- [ ] **Step 2: Lint and format**

Run: `gofmt -l internal/ cmd/ ; go vet ./... && golangci-lint run ./... 2>&1 | tail`
Expected: empty `gofmt` output, no vet errors, `0 issues` from golangci-lint. (If `golangci-lint` is not installed, install it pinned: `curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v2.12.2` then re-run.)

- [ ] **Step 3: (Optional) end-to-end priority smoke**

With Redis running:
```bash
go build -o /tmp/relay-demo ./cmd/demo && go build -o /tmp/relay-worker ./cmd/worker
redis-cli -n 0 DEL q:pq:ready >/dev/null
/tmp/relay-demo -queue pq -count 5 -priority 0
/tmp/relay-demo -queue pq -count 1 -priority 9
# the priority-9 job should be claimed before the priority-0 jobs:
/tmp/relay-worker -queue pq -concurrency 1 -fail-rate 0 &
sleep 1; kill %1
redis-cli -n 0 DEL q:pq:ready q:pq:inflight q:pq:dlq >/dev/null
rm -f /tmp/relay-demo /tmp/relay-worker
```
Expected: the worker logs the priority-9 job processed first (or among the first), demonstrating priority ordering.

---

## Notes for the implementer

- **Backward compatibility:** `WithPriority` is a new variadic option; existing `Enqueue` calls and the `Job` struct's other users are unaffected. The ready score for a default (priority-0) job becomes `-nowMs` instead of `0`; no existing test asserts the literal ready score, so this does not break anything.
- **`claim.lua` and `nack.lua` are intentionally untouched** — claim already pops `ZPOPMAX`; nack routes retries to `delayed` (priority rides on the hash and is applied at promotion).
- **Scale is one constant** (`priorityScale` in `internal/broker/priority.go`), passed to the Lua scripts as an ARGV; do not hardcode `1e13` inside the `.lua` files.
- **Priority is clamped once, at enqueue.** The scripts trust the stored value (defaulting a missing field to 0).
- Test DBs: broker tests use Redis DB 15 — do not change.
