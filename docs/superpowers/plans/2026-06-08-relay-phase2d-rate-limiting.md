# Relay Phase 2d — Per-Queue Rate Limiting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cap how fast a queue is consumed with an opt-in per-queue token bucket, folded into the atomic claim so a token is consumed only when a job is actually delivered.

**Architecture:** `WithRateLimit(queue, rate, burst)` registers a limit. `claim.lua` gains a skippable token-bucket gate (per-queue `q:{name}:ratelimit` hash, lazy refill); when no limit is configured the gate is skipped and claim behaves byte-identically to today.

**Tech Stack:** Go, Redis (go-redis v9), Lua via `go:embed`. Tests run against a real Redis.

**Spec:** [`docs/superpowers/specs/2026-06-08-relay-phase2d-rate-limiting-design.md`](../specs/2026-06-08-relay-phase2d-rate-limiting-design.md)

**Prerequisites:** Redis reachable at `localhost:6379` (or `REDIS_ADDR`) — broker tests skip without it. Confirm `redis-cli ping` → `PONG`. Work happens on the current branch/worktree.

---

## File Structure

- `internal/broker/ratelimit.go` — create: `rateLimit` struct, `ratelimitKey`, `WithRateLimit`.
- `internal/broker/ratelimit_test.go` — create: white-box unit tests (package `broker`).
- `internal/broker/broker.go` — modify: `rateLimits` field; `Claim` passes the bucket key + rate/burst/enabled args.
- `internal/broker/scripts/claim.lua` — modify: add the skippable token-bucket gate.
- `internal/broker/broker_test.go` — modify: rate-limit behavior tests + concurrency test.
- `cmd/worker/main.go` — modify: `--rate` / `--burst` flags.
- `CLAUDE.md` — modify: reflect rate limiting shipped.

`nack.lua`, `ack.lua`, `promote.lua`, `reaper.lua`, `enqueue.lua`, `heartbeat.lua`, `internal/job`, `internal/worker` are unchanged.

---

## Task 1: Rate-limit config scaffolding

**Files:**
- Create: `internal/broker/ratelimit.go`
- Modify: `internal/broker/broker.go` (add `rateLimits` field)
- Test: `internal/broker/ratelimit_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/broker/ratelimit_test.go` (white-box — `package broker`):

```go
package broker

import "testing"

func TestRateLimitKeyFormat(t *testing.T) {
	if got := ratelimitKey("emails"); got != "q:emails:ratelimit" {
		t.Errorf("ratelimitKey = %q, want %q", got, "q:emails:ratelimit")
	}
}

func TestWithRateLimitRegisters(t *testing.T) {
	b := New(nil, WithRateLimit("emails", 100, 200))
	rl, ok := b.rateLimits["emails"]
	if !ok || rl.rate != 100 || rl.burst != 200 {
		t.Errorf("rateLimits[emails] = %+v ok=%v, want {rate:100 burst:200}", rl, ok)
	}
	if _, ok := b.rateLimits["sms"]; ok {
		t.Error("sms should have no limit registered")
	}
}

func TestWithRateLimitPanicsOnBadConfig(t *testing.T) {
	cases := []struct {
		rate  float64
		burst int
	}{{0, 10}, {-1, 10}, {100, 0}, {100, -1}}
	for _, c := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithRateLimit(rate=%v, burst=%d) did not panic", c.rate, c.burst)
				}
			}()
			New(nil, WithRateLimit("q", c.rate, c.burst))
		}()
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestRateLimitKeyFormat|TestWithRateLimitRegisters|TestWithRateLimitPanicsOnBadConfig' 2>&1 | head`
Expected: build failure — `undefined: ratelimitKey`, `undefined: WithRateLimit`, `b.rateLimits undefined`.

- [ ] **Step 3: Create ratelimit.go**

Create `internal/broker/ratelimit.go`:

```go
package broker

import "fmt"

// rateLimit is a per-queue token-bucket configuration: rate tokens accrue per
// second up to a maximum of burst tokens.
type rateLimit struct {
	rate  float64
	burst int
}

// ratelimitKey is the Redis key for a queue's token bucket: `q:{name}:ratelimit`,
// a hash with fields `tokens` (current balance) and `ts` (last-update unix ms).
func ratelimitKey(queue string) string { return "q:" + queue + ":ratelimit" }

// WithRateLimit caps how fast queue can be claimed: at most burst jobs in a
// burst, refilling at rate jobs/second. Queues with no registered limit are
// unthrottled. Panics on a non-positive rate or a burst below 1 — a nonsensical
// limit is a programming error, not something to silently ignore.
//
// All workers on a queue must register the same limit: they share one Redis
// bucket and pass these params on every claim.
func WithRateLimit(queue string, rate float64, burst int) Option {
	if rate <= 0 || burst < 1 {
		panic(fmt.Sprintf("broker: WithRateLimit(%q, %v, %d): rate must be > 0 and burst >= 1", queue, rate, burst))
	}
	return func(b *Broker) {
		if b.rateLimits == nil {
			b.rateLimits = make(map[string]rateLimit)
		}
		b.rateLimits[queue] = rateLimit{rate: rate, burst: burst}
	}
}
```

- [ ] **Step 4: Add the `rateLimits` field**

In `internal/broker/broker.go`, add `rateLimits map[string]rateLimit` to the `Broker` struct (after `dedupTTL`):

```go
	rdb         *redis.Client
	backoffBase time.Duration
	backoffMax  time.Duration
	dedupTTL    time.Duration
	rateLimits  map[string]rateLimit

	rndMu sync.Mutex
	rnd   *rand.Rand
```

(No change to `New` — a nil map reads fine, and `WithRateLimit` lazily initializes it.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestRateLimitKeyFormat|TestWithRateLimitRegisters|TestWithRateLimitPanicsOnBadConfig' -v 2>&1 | tail`
Expected: PASS for all three.

- [ ] **Step 6: Commit**

```bash
git add internal/broker/ratelimit.go internal/broker/ratelimit_test.go internal/broker/broker.go
git commit -m "Add per-queue rate-limit config scaffolding"
```

---

## Task 2: Token-bucket gate in claim.lua

**Files:**
- Modify: `internal/broker/scripts/claim.lua`
- Modify: `internal/broker/broker.go` (`Claim`)
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/broker/broker_test.go`:

```go
func TestRateLimitBurstThenDeny(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithRateLimit("emails", 1, 2)) // 1/s, burst 2

	for i := 0; i < 5; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
			t.Fatalf("claim %d: err=%v ok=%v, want a job", i, err, ok)
		}
	}
	// Third claim within the same second: ~0 refill, bucket empty → denied.
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil {
		t.Fatalf("claim 3: %v", err)
	} else if ok {
		t.Error("third claim returned a job, want rate-limited (ok=false)")
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 3 {
		t.Errorf("ready = %d, want 3 (only 2 popped)", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 2 {
		t.Errorf("inflight = %d, want 2", n)
	}
}

func TestRateLimitFreshQueueStartsFull(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithRateLimit("emails", 1, 3)) // burst 3

	for i := 0; i < 4; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
			t.Fatalf("claim %d: err=%v ok=%v, want a job (full burst)", i, err, ok)
		}
	}
	if _, ok, _ := b.Claim(ctx, "emails", time.Minute); ok {
		t.Error("4th claim succeeded, want denied (burst exhausted)")
	}
}

func TestRateLimitConsumeOnlyOnPop(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithRateLimit("emails", 1, 1)) // burst 1

	// Empty-queue claims must NOT drain the bucket.
	for i := 0; i < 5; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil {
			t.Fatalf("empty claim %d: %v", i, err)
		} else if ok {
			t.Fatalf("empty claim %d returned a job", i)
		}
	}
	// The single burst token must still be available for a real job.
	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("claim after empty polls: err=%v ok=%v, want a job (bucket not drained)", err, ok)
	}
}

func TestRateLimitRefillOverTime(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithRateLimit("emails", 100, 1)) // 100/s → 10ms/token, burst 1

	for i := 0; i < 3; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("claim 1: err=%v ok=%v", err, ok)
	}
	if _, ok, _ := b.Claim(ctx, "emails", time.Minute); ok {
		t.Error("claim 2 immediately succeeded, want denied")
	}
	time.Sleep(30 * time.Millisecond) // refills ~3 tokens, capped at burst 1
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("claim 3 after refill: err=%v ok=%v, want a job", err, ok)
	}
}

func TestClaimUnlimitedCreatesNoBucket(t *testing.T) {
	b, rdb := newTestBroker(t) // no rate limit configured
	ctx := context.Background()
	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}
	if n, _ := rdb.Exists(ctx, "q:emails:ratelimit").Result(); n != 0 {
		t.Errorf("ratelimit bucket created for an unlimited queue")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/broker/ -run 'TestRateLimit|TestClaimUnlimitedCreatesNoBucket' 2>&1 | tail`
Expected: FAIL — `Claim` does not yet pass rate-limit args, so the bucket gate never runs: the deny/burst tests see jobs returned past the burst (ok=true where ok=false expected).

- [ ] **Step 3: Add the gate to claim.lua**

Replace the entire contents of `internal/broker/scripts/claim.lua` with:

```lua
-- claim.lua — the atomic claim, the heart of Relay.
--
-- In one indivisible step it (optionally) checks a per-queue rate-limit token
-- bucket, pops the most important ready job, marks it in-flight under a
-- visibility deadline, and bumps its attempt count. Because the whole thing is
-- one Redis script, two workers can never claim the same job or over-issue
-- rate-limit tokens. Splitting any of it into separate round-trips would
-- reintroduce the races this project exists to solve, so all of it stays here.
--
-- KEYS[1] = ready set     q:{name}:ready    (ZSET scored by priority)
-- KEYS[2] = inflight set  q:{name}:inflight (ZSET scored by visibility deadline)
-- KEYS[3] = ratelimit     q:{name}:ratelimit (hash tokens/ts; unused when ARGV[6]='0')
-- ARGV[1] = now in unix milliseconds (passed in so the script is deterministic)
-- ARGV[2] = visibility timeout in milliseconds
-- ARGV[3] = job hash key prefix ("job:") so we can find the popped job's hash
-- ARGV[4] = rate limit: tokens per second
-- ARGV[5] = rate limit: burst (bucket capacity)
-- ARGV[6] = rate limiting enabled, '1' or '0'
--
-- Returns the claimed job's hash as a flat HGETALL array, or a Redis nil reply
-- when the ready set is empty OR the queue is rate-limited.

local now = tonumber(ARGV[1])

-- Optional token-bucket gate. A token is consumed only when a job is actually
-- popped (below), so empty-queue polls and denied claims never drain the bucket
-- and cannot starve real work.
local tokens
if ARGV[6] == '1' then
  local rate = tonumber(ARGV[4])
  local burst = tonumber(ARGV[5])
  local data = redis.call('HMGET', KEYS[3], 'tokens', 'ts')
  tokens = tonumber(data[1]) or burst
  local ts = tonumber(data[2]) or now
  tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)
  if tokens < 1 then
    return nil -- rate-limited: do not pop; leave the bucket so time keeps accruing
  end
end

-- Pop the highest-scored (highest-priority) member. ZPOPMAX returns
-- {member, score} or {} when the set is empty.
local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then
  return nil -- empty queue: bucket left untouched (no token wasted)
end

local id = popped[1]

-- A job is being delivered: consume one token now (only here).
if ARGV[6] == '1' then
  redis.call('HSET', KEYS[3], 'tokens', tokens - 1, 'ts', now)
end

local deadline = now + tonumber(ARGV[2])

-- Place it in the inflight set scored by its deadline; the reaper later scans
-- this set for entries whose deadline has passed and requeues them.
redis.call('ZADD', KEYS[2], deadline, id)

-- Update the job hash: count the delivery attempt and record the new state.
local job_key = ARGV[3] .. id
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HSET', job_key, 'state', 'inflight')

return redis.call('HGETALL', job_key)
```

- [ ] **Step 4: Pass rate-limit args from `Claim`**

In `internal/broker/broker.go`, replace the `claimScript.Run(...)` call at the top of `Claim` with the version that looks up the queue's limit and passes the bucket key + params. The method becomes:

```go
func (b *Broker) Claim(ctx context.Context, queue string, visibility time.Duration) (job.Job, bool, error) {
	rate, burst, enabled := 0.0, 0, "0"
	if rl, ok := b.rateLimits[queue]; ok {
		rate, burst, enabled = rl.rate, rl.burst, "1"
	}

	res, err := claimScript.Run(ctx, b.rdb,
		[]string{readyKey(queue), inflightKey(queue), ratelimitKey(queue)},
		time.Now().UnixMilli(), visibility.Milliseconds(), jobKeyPrefix, rate, burst, enabled,
	).Result()
	if errors.Is(err, redis.Nil) {
		return job.Job{}, false, nil // nothing ready, or rate-limited
	}
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}

	h, err := hashFromLua(res)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}
	j, err := job.FromHash(h)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}
	return j, true, nil
}
```

(Only the new first block and the `claimScript.Run` KEYS/ARGV change; the error handling and decoding tail are unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/broker/ -run 'TestRateLimit|TestClaim' -v 2>&1 | tail -30`
Expected: PASS for the five new tests and all existing `TestClaim*` tests (unlimited queues are behavior-preserving: `enabled='0'` skips the gate).

- [ ] **Step 6: Full broker + worker suite under race**

Run: `go test -race ./internal/broker/ ./internal/worker/ 2>&1 | tail`
Expected: `ok` for both (the worker's claim loop calls `Claim`, which now passes the extra args; no regression).

- [ ] **Step 7: Commit**

```bash
git add internal/broker/scripts/claim.lua internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Gate claim with an optional per-queue token bucket"
```

---

## Task 3: Concurrency test — atomic bucket never over-issues

**Files:**
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the test**

Add to `internal/broker/broker_test.go` (`sync` and `sync/atomic` are already imported from the Phase 2c concurrency test):

```go
func TestRateLimitConcurrentClaimsRespectBurst(t *testing.T) {
	_, rdb := newTestBroker(t)
	ctx := context.Background()
	b := broker.New(rdb, broker.WithRateLimit("emails", 1, 5)) // burst 5, ~no refill in the window

	const njobs = 50
	for i := 0; i < njobs; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	const nclaims = 20
	var claimed int64
	var wg sync.WaitGroup
	for i := 0; i < nclaims; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := b.Claim(ctx, "emails", time.Minute)
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			if ok {
				atomic.AddInt64(&claimed, 1)
			}
		}()
	}
	wg.Wait()

	if claimed != 5 {
		t.Errorf("claimed %d, want exactly 5 (the burst)", claimed)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 5 {
		t.Errorf("inflight = %d, want 5", n)
	}
}
```

- [ ] **Step 2: Run under the race detector**

Run: `go test -race ./internal/broker/ -run TestRateLimitConcurrentClaimsRespectBurst -v 2>&1 | tail`
Expected: PASS — exactly 5 of 20 concurrent claims succeed; the atomic bucket prevents over-issue. No race detected.

- [ ] **Step 3: Commit**

```bash
git add internal/broker/broker_test.go
git commit -m "Add concurrent rate-limit burst test"
```

---

## Task 4: Worker flags + docs

**Files:**
- Modify: `cmd/worker/main.go`
- Modify: `CLAUDE.md`

Thin wiring + docs, verified by build/vet.

- [ ] **Step 1: Add `--rate` / `--burst` to cmd/worker**

First READ `cmd/worker/main.go` to confirm the exact names (the flag block, `*queue`, and where the broker is built: `b := broker.New(rdb, broker.WithBackoff(*backoffBase, *backoffMax))`). Then:

(a) Add two flags in the flag block (after `backoffMax`):

```go
	rate := flag.Float64("rate", 0, "max claims/second for this queue (0 = unlimited)")
	burst := flag.Int("burst", 0, "rate-limit burst capacity (defaults to 1 when --rate is set)")
```

(b) Replace the broker construction line with a version that conditionally adds the rate limit:

```go
	brokerOpts := []broker.Option{broker.WithBackoff(*backoffBase, *backoffMax)}
	if *rate > 0 {
		if *burst < 1 {
			*burst = 1
		}
		brokerOpts = append(brokerOpts, broker.WithRateLimit(*queue, *rate, *burst))
	}
	b := broker.New(rdb, brokerOpts...)
```

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`, READ the relevant lines first, then:

1. In the Redis data-model table, add a row (after the `q:{name}:dedup` row):
   `| `q:{name}:ratelimit` | hash | — | per-queue token bucket (`tokens`, `ts`); claim consumes a token only on a successful pop |`
2. In the Build order "Phase 2 — depth (in progress)" line: add `rate limiting ✅` to the completed list and remove `per-queue rate limiting` from the still-to-do list (leaving only Prometheus metrics).
3. In the status/inventory `internal/broker` bullet (or wherever broker construction options are mentioned), note the new `WithRateLimit` broker option alongside `WithBackoff`.
4. In "Known limitations", add a bullet:
   `- **Rate-limit config is per-worker, not stored in Redis.** All workers on a queue must register the same `WithRateLimit` (they share one Redis bucket and pass rate/burst on every claim); mismatched configs refill inconsistently. A rate-limited claim is indistinguishable from an empty queue to the worker (it polls again).`

(Adapt wording if exact strings differ; intent: the ratelimit key is documented, rate limiting is done in Phase 2, `WithRateLimit` exists, and the per-worker-config limitation is recorded.)

- [ ] **Step 3: Verify build, vet, format**

Run: `go build ./... && go vet ./... && gofmt -l cmd/ internal/ && echo OK`
Expected: `OK` (empty gofmt output, no errors).

- [ ] **Step 4: Commit**

```bash
git add cmd/worker/main.go CLAUDE.md
git commit -m "Add worker rate-limit flags; document per-queue rate limiting"
```

---

## Task 5: Full verification

- [ ] **Step 1: Whole suite under race (uncached)**

Run: `go test -race -count=1 ./... 2>&1 | tail`
Expected: `ok` for `internal/broker`, `internal/job`, `internal/worker`; `no test files` for the `cmd` packages.

- [ ] **Step 2: Lint and format**

Run: `gofmt -l internal/ cmd/ ; go vet ./... && golangci-lint run ./... 2>&1 | tail`
Expected: empty `gofmt`, no vet errors, `0 issues` from golangci-lint. (If `golangci-lint` is missing, install pinned: `curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v2.12.2` then re-run.)

- [ ] **Step 3: (Optional) end-to-end rate-limit smoke**

With Redis running:
```bash
go build -o /tmp/relay-demo ./cmd/demo && go build -o /tmp/relay-worker ./cmd/worker
redis-cli -n 0 DEL q:rl:ready q:rl:inflight q:rl:ratelimit >/dev/null
/tmp/relay-demo -queue rl -count 50
# limit to 5/s, burst 5: ~50 jobs should take ~9s to drain
/tmp/relay-worker -queue rl -concurrency 4 -fail-rate 0 -rate 5 -burst 5 &
sleep 3; echo "after 3s, ready depth: $(redis-cli -n 0 ZCARD q:rl:ready)  (expect ~30+ remaining, not 0)"
sleep 8; echo "after 11s, ready depth: $(redis-cli -n 0 ZCARD q:rl:ready)  (expect ~0)"
kill %1 2>/dev/null
redis-cli -n 0 DEL q:rl:ready q:rl:inflight q:rl:dlq q:rl:ratelimit >/dev/null
rm -f /tmp/relay-demo /tmp/relay-worker
```
Expected: the queue drains at roughly the configured rate (still backlogged at 3s, ~drained by 11s), not instantly.

---

## Notes for the implementer

- **Backward compatibility:** `Claim`'s signature is unchanged; unconfigured queues pass `enabled='0'` and the gate is skipped, so `claim.lua` is behavior-identical for them (existing claim tests are the guard).
- **Atomicity:** the bucket refill, the deny decision, the pop, and the token consume are one Lua script — never split them.
- **Consume-only-on-pop:** the bucket is written only after a successful `ZPOPMAX`. Do not write it on the deny or empty paths (that would let idle workers drain it).
- **`now` is passed from Go** for both the deadline and the bucket refill — do not call Redis `TIME`.
- Test DBs: broker tests use Redis DB 15 — do not change.
