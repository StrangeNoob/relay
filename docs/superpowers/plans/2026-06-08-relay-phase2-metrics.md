# Phase 2 Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Relay observable by instrumenting every broker job-state transition with Prometheus counters/histogram and exposing live per-queue depth gauges at a `/metrics` endpoint on `cmd/worker`.

**Architecture:** A `Metrics` interface in `broker` with a no-op default (opt-in via `WithMetrics`) keeps the broker free of a hard prometheus dependency and makes instrumentation purely additive. Counters/histogram are pushed inline after each Redis op succeeds; per-queue depth gauges are pulled at scrape time by a `prometheus.Collector` running `ZCARD`/`LLEN`. The concrete recorder + collector live in a new `internal/metrics` package that satisfies `broker.Metrics` structurally (no import of `broker`).

**Tech Stack:** Go, `github.com/redis/go-redis/v9`, `github.com/prometheus/client_golang` (new dependency), real-Redis integration tests.

**Spec:** [`docs/superpowers/specs/2026-06-08-relay-phase2-metrics-design.md`](../specs/2026-06-08-relay-phase2-metrics-design.md)

---

## File Structure

- **Create `internal/broker/metrics.go`** — `Metrics` interface, `noopMetrics` default, `WithMetrics` option. Responsibility: the consumer-side instrumentation contract and its no-op.
- **Modify `internal/broker/broker.go`** — add `metrics Metrics` field to `Broker`, initialise to `noopMetrics{}` in `New`, and record at each transition (`Enqueue`, `Claim`, `Ack`, `Nack`, `Reap`, `Promote`).
- **Create `internal/broker/metrics_test.go`** (`package broker`) — internal unit tests for option wiring / noop default.
- **Create `internal/broker/instrumentation_test.go`** (`package broker_test`) — a fake recorder + behavior tests that each transition records the right call against real Redis.
- **Modify `internal/broker/broker_test.go`** — refactor `newTestBroker` to delegate to a new `newTestBrokerWith(t, opts...)` so instrumentation tests can inject `WithMetrics`.
- **Create `internal/metrics/recorder.go`** — `Recorder` (prometheus counters + histogram over a private registry) implementing `broker.Metrics`.
- **Create `internal/metrics/depth.go`** — `DepthCollector` (`prometheus.Collector`) reading `ZCARD`/`LLEN` at scrape time.
- **Create `internal/metrics/recorder_test.go`** — compile assertion + `testutil` value assertions.
- **Create `internal/metrics/depth_test.go`** — `DepthCollector` against real Redis (DB 13).
- **Modify `cmd/worker/main.go`** — `--metrics-addr` flag; when set, build recorder, wire `WithMetrics`, register depth collector, serve `/metrics`, shut down gracefully.
- **Modify `CLAUDE.md`** — flip Phase 2 to complete, mark `internal/metrics` ✅, document `WithMetrics` + the new dependency + known limitations.
- **Modify `go.mod` / `go.sum`** — add `prometheus/client_golang` (happens automatically on first import + `go mod tidy`).

---

## Task 1: `Metrics` interface, noop default, and `WithMetrics` option

**Files:**
- Create: `internal/broker/metrics.go`
- Create: `internal/broker/metrics_test.go` (`package broker`)
- Modify: `internal/broker/broker.go` (the `Broker` struct + `New`)

- [ ] **Step 1: Write the failing test**

Create `internal/broker/metrics_test.go`:

```go
package broker

import "testing"

func TestNewDefaultsToNoopMetrics(t *testing.T) {
	b := New(nil)
	if b.metrics == nil {
		t.Fatal("New: metrics field is nil, want noopMetrics default")
	}
	if _, ok := b.metrics.(noopMetrics); !ok {
		t.Fatalf("New: metrics = %T, want noopMetrics", b.metrics)
	}
}

func TestWithMetricsInstallsRecorder(t *testing.T) {
	rec := noopMetrics{} // any Metrics value; identity is what we check
	var custom Metrics = rec
	b := New(nil, WithMetrics(custom))
	if b.metrics == nil {
		t.Fatal("WithMetrics: metrics is nil")
	}
}

func TestWithMetricsNilIsIgnored(t *testing.T) {
	b := New(nil, WithMetrics(nil))
	if _, ok := b.metrics.(noopMetrics); !ok {
		t.Fatalf("WithMetrics(nil): metrics = %T, want noopMetrics retained", b.metrics)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestNewDefaultsToNoopMetrics|TestWithMetrics' -v`
Expected: FAIL — compile error, `b.metrics` undefined / `noopMetrics`/`WithMetrics`/`Metrics` undefined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/broker/metrics.go`:

```go
package broker

import "time"

// Metrics receives a callback for every job-state transition the broker makes.
// It is the broker's consumer-side instrumentation contract: the broker depends
// on this small interface, not on any metrics library, so a Prometheus recorder
// (internal/metrics) — or a test fake — can be plugged in via WithMetrics. The
// default is noopMetrics, so a broker built without WithMetrics records nothing
// and behaves exactly as before.
//
// Every method takes the queue name so the implementation can label its series
// per queue. Reap/Promote add a batch count because one call moves many jobs;
// the rest are single events. ObserveLatency reports a job's end-to-end time in
// the system (enqueue -> ack).
type Metrics interface {
	IncEnqueued(queue string)
	IncDeduplicated(queue string)
	IncClaimed(queue string)
	IncProcessed(queue string)
	IncRetried(queue string)
	IncDead(queue string)
	AddReaped(queue string, n int)
	AddPromoted(queue string, n int)
	ObserveLatency(queue string, d time.Duration)
}

// noopMetrics is the default recorder: every method does nothing. It lets the
// broker call b.metrics unconditionally without nil checks, and keeps metrics
// entirely opt-in.
type noopMetrics struct{}

func (noopMetrics) IncEnqueued(string)              {}
func (noopMetrics) IncDeduplicated(string)          {}
func (noopMetrics) IncClaimed(string)               {}
func (noopMetrics) IncProcessed(string)             {}
func (noopMetrics) IncRetried(string)               {}
func (noopMetrics) IncDead(string)                  {}
func (noopMetrics) AddReaped(string, int)           {}
func (noopMetrics) AddPromoted(string, int)         {}
func (noopMetrics) ObserveLatency(string, time.Duration) {}

// WithMetrics installs a Metrics recorder. A nil recorder is ignored so callers
// cannot accidentally replace the safe no-op with something that panics.
func WithMetrics(m Metrics) Option {
	return func(b *Broker) {
		if m != nil {
			b.metrics = m
		}
	}
}
```

In `internal/broker/broker.go`, add the field to the `Broker` struct (alongside the existing fields):

```go
	metrics Metrics
```

And in `New`, set the default BEFORE applying options (so `WithMetrics` can override it). The current `New` builds the struct then ranges over opts; ensure the literal includes `metrics: noopMetrics{}`:

```go
	b := &Broker{
		rdb: rdb,
		// ... existing field initialisers unchanged ...
		metrics: noopMetrics{},
	}
	for _, opt := range opts {
		opt(b)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestNewDefaultsToNoopMetrics|TestWithMetrics' -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/metrics.go internal/broker/metrics_test.go internal/broker/broker.go
git commit -m "Add broker Metrics interface, noop default, and WithMetrics option"
```

---

## Task 2: Fake recorder + Enqueue instrumentation

**Files:**
- Modify: `internal/broker/broker_test.go` (refactor `newTestBroker`)
- Create: `internal/broker/instrumentation_test.go` (`package broker_test`)
- Modify: `internal/broker/broker.go` (`Enqueue`)

- [ ] **Step 1: Refactor the test harness to accept options**

In `internal/broker/broker_test.go`, replace the `newTestBroker` body so it delegates to a new variadic helper (keep `newTestBroker` working for all existing callers):

```go
func newTestBroker(t *testing.T) (*broker.Broker, *redis.Client) {
	t.Helper()
	return newTestBrokerWith(t)
}

// newTestBrokerWith is newTestBroker with broker options, for tests that need to
// inject a recorder or other configuration.
func newTestBrokerWith(t *testing.T, opts ...broker.Option) (*broker.Broker, *redis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testRedisDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return broker.New(rdb, opts...), rdb
}
```

- [ ] **Step 2: Write the failing test (fake recorder + enqueue)**

Create `internal/broker/instrumentation_test.go`:

```go
package broker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

// fakeMetrics is a Metrics recorder that counts calls per queue in memory, so a
// test can assert exactly which transition the broker recorded. It is safe for
// concurrent use because the broker may record from multiple goroutines.
type fakeMetrics struct {
	mu         sync.Mutex
	enqueued   map[string]int
	deduped    map[string]int
	claimed    map[string]int
	processed  map[string]int
	retried    map[string]int
	dead       map[string]int
	reaped     map[string]int
	promoted   map[string]int
	latencies  []time.Duration
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		enqueued:  map[string]int{},
		deduped:   map[string]int{},
		claimed:   map[string]int{},
		processed: map[string]int{},
		retried:   map[string]int{},
		dead:      map[string]int{},
		reaped:    map[string]int{},
		promoted:  map[string]int{},
	}
}

func (f *fakeMetrics) IncEnqueued(q string)     { f.bump(f.enqueued, q) }
func (f *fakeMetrics) IncDeduplicated(q string) { f.bump(f.deduped, q) }
func (f *fakeMetrics) IncClaimed(q string)      { f.bump(f.claimed, q) }
func (f *fakeMetrics) IncProcessed(q string)    { f.bump(f.processed, q) }
func (f *fakeMetrics) IncRetried(q string)      { f.bump(f.retried, q) }
func (f *fakeMetrics) IncDead(q string)         { f.bump(f.dead, q) }
func (f *fakeMetrics) AddReaped(q string, n int)   { f.add(f.reaped, q, n) }
func (f *fakeMetrics) AddPromoted(q string, n int) { f.add(f.promoted, q, n) }
func (f *fakeMetrics) ObserveLatency(q string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, d)
}

func (f *fakeMetrics) bump(m map[string]int, q string) { f.add(m, q, 1) }
func (f *fakeMetrics) add(m map[string]int, q string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m[q] += n
}

func (f *fakeMetrics) get(m map[string]int, q string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return m[q]
}

func TestEnqueueRecordsEnqueued(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := fm.get(fm.enqueued, "emails"); got != 1 {
		t.Errorf("enqueued[emails] = %d, want 1", got)
	}
	if got := fm.get(fm.deduped, "emails"); got != 0 {
		t.Errorf("deduped[emails] = %d, want 0", got)
	}
}

func TestEnqueueDuplicateRecordsDeduplicated(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	j1 := job.New("emails", []byte("a"))
	if err := b.Enqueue(ctx, j1, broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	j2 := job.New("emails", []byte("b"))
	if err := b.Enqueue(ctx, j2, broker.WithIdempotencyKey("k1")); err != broker.ErrDuplicate {
		t.Fatalf("second Enqueue err = %v, want ErrDuplicate", err)
	}
	if got := fm.get(fm.enqueued, "emails"); got != 1 {
		t.Errorf("enqueued[emails] = %d, want 1", got)
	}
	if got := fm.get(fm.deduped, "emails"); got != 1 {
		t.Errorf("deduped[emails] = %d, want 1", got)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestEnqueueRecords|TestEnqueueDuplicateRecords' -v`
Expected: FAIL — `enqueued[emails] = 0, want 1` (instrumentation not added yet). Requires Redis; if skipped, note RED cannot be shown without Redis and proceed only once Redis is available.

- [ ] **Step 4: Add Enqueue instrumentation**

In `internal/broker/broker.go`, in `Enqueue`, record after the script succeeds (replace the tail `if res == "dup" { return ErrDuplicate }; return nil`):

```go
	if res == "dup" {
		b.metrics.IncDeduplicated(j.Queue)
		return ErrDuplicate
	}
	b.metrics.IncEnqueued(j.Queue)
	return nil
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestEnqueueRecords|TestEnqueueDuplicateRecords' -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/broker/broker_test.go internal/broker/instrumentation_test.go internal/broker/broker.go
git commit -m "Instrument Enqueue with enqueued/deduplicated metrics"
```

---

## Task 3: Claim instrumentation

**Files:**
- Modify: `internal/broker/broker.go` (`Claim`)
- Modify: `internal/broker/instrumentation_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/instrumentation_test.go`:

```go
func TestClaimRecordsClaimed(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v, want true/nil", ok, err)
	}
	if got := fm.get(fm.claimed, "emails"); got != 1 {
		t.Errorf("claimed[emails] = %d, want 1", got)
	}
}

func TestClaimEmptyQueueRecordsNothing(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || ok {
		t.Fatalf("Claim on empty: ok=%v err=%v, want false/nil", ok, err)
	}
	if got := fm.get(fm.claimed, "emails"); got != 0 {
		t.Errorf("claimed[emails] = %d, want 0", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestClaimRecordsClaimed|TestClaimEmptyQueueRecordsNothing' -v`
Expected: FAIL — `claimed[emails] = 0, want 1`.

- [ ] **Step 3: Add Claim instrumentation**

In `Claim`, record only on a successful pop. Find the point where the script returned a job and the function is about to return `(j, true, nil)`; add `b.metrics.IncClaimed(queue)` immediately before that successful return. The empty-queue path (returns `ok == false`) must record nothing. (The exact return statement is the one returning the decoded job with `true`; do not touch the `redis.Nil` / empty branch.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestClaimRecordsClaimed|TestClaimEmptyQueueRecordsNothing' -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/instrumentation_test.go
git commit -m "Instrument Claim with claimed metric"
```

---

## Task 4: Ack instrumentation (processed + latency)

**Files:**
- Modify: `internal/broker/broker.go` (`Ack`)
- Modify: `internal/broker/instrumentation_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/instrumentation_test.go`:

```go
func TestAckRecordsProcessedAndLatency(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	j, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Ack(ctx, j); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := fm.get(fm.processed, "emails"); got != 1 {
		t.Errorf("processed[emails] = %d, want 1", got)
	}
	fm.mu.Lock()
	n := len(fm.latencies)
	var d time.Duration
	if n == 1 {
		d = fm.latencies[0]
	}
	fm.mu.Unlock()
	if n != 1 {
		t.Fatalf("latencies len = %d, want 1", n)
	}
	if d < 0 {
		t.Errorf("latency = %v, want non-negative", d)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestAckRecordsProcessedAndLatency -v`
Expected: FAIL — `processed[emails] = 0, want 1`.

- [ ] **Step 3: Add Ack instrumentation**

In `Ack`, after the script `Run(...).Err()` check succeeds and before `return nil`:

```go
	b.metrics.IncProcessed(j.Queue)
	b.metrics.ObserveLatency(j.Queue, time.Since(j.CreatedAt))
	return nil
```

(`job.Job` has `CreatedAt time.Time`; `time.Since` yields enqueue→ack elapsed. `time` is already imported in broker.go.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestAckRecordsProcessedAndLatency -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/instrumentation_test.go
git commit -m "Instrument Ack with processed counter and end-to-end latency"
```

---

## Task 5: Nack instrumentation (retried vs dead)

**Files:**
- Modify: `internal/broker/broker.go` (`Nack`)
- Modify: `internal/broker/instrumentation_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/instrumentation_test.go`:

```go
// nackTestJob enqueues, claims, and returns a job set up so the next Nack takes
// the requested branch. maxRetries controls retry-vs-dead: with maxRetries=5 the
// first nack retries; with maxRetries=0 the first nack dead-letters.
func nackTestJob(t *testing.T, b *broker.Broker, ctx context.Context, maxRetries int) job.Job {
	t.Helper()
	j := job.New("emails", []byte("x"))
	j.MaxRetries = maxRetries
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	return claimed
}

func TestNackWithRetriesLeftRecordsRetried(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	j := nackTestJob(t, b, ctx, 5) // attempts now 1 < 5 -> retry
	if err := b.Nack(ctx, j); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if got := fm.get(fm.retried, "emails"); got != 1 {
		t.Errorf("retried[emails] = %d, want 1", got)
	}
	if got := fm.get(fm.dead, "emails"); got != 0 {
		t.Errorf("dead[emails] = %d, want 0", got)
	}
}

func TestNackWithBudgetSpentRecordsDead(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	j := nackTestJob(t, b, ctx, 0) // attempts now 1, max 0 -> dead
	if err := b.Nack(ctx, j); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if got := fm.get(fm.dead, "emails"); got != 1 {
		t.Errorf("dead[emails] = %d, want 1", got)
	}
	if got := fm.get(fm.retried, "emails"); got != 0 {
		t.Errorf("retried[emails] = %d, want 0", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestNackWithRetriesLeftRecordsRetried|TestNackWithBudgetSpentRecordsDead' -v`
Expected: FAIL — `retried[emails] = 0, want 1` (and dead = 0).

- [ ] **Step 3: Add Nack instrumentation**

In `Nack`, the script already returns `"retry"`/`"dead"` but the code currently discards it with `.Err()`. Capture it with `.Text()` and branch. Replace the script-run block:

```go
	outcome, err := nackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), delayedKey(j.Queue), dlqKey(j.Queue)},
		j.ID, jobKeyPrefix, readyAt,
	).Text()
	if err != nil {
		return fmt.Errorf("broker: nacking job %s: %w", j.ID, err)
	}
	switch outcome {
	case "retry":
		b.metrics.IncRetried(j.Queue)
	case "dead":
		b.metrics.IncDead(j.Queue)
	}
	return nil
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestNackWithRetriesLeftRecordsRetried|TestNackWithBudgetSpentRecordsDead' -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/instrumentation_test.go
git commit -m "Instrument Nack with retried/dead metrics from script outcome"
```

---

## Task 6: Reap + Promote instrumentation

**Files:**
- Modify: `internal/broker/broker.go` (`Reap`, `Promote`)
- Modify: `internal/broker/instrumentation_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/instrumentation_test.go`:

```go
func TestReapRecordsReapedCount(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	// Enqueue + claim two jobs with a tiny visibility so they expire immediately.
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Millisecond); err != nil || !ok {
			t.Fatalf("Claim: ok=%v err=%v", ok, err)
		}
	}
	time.Sleep(10 * time.Millisecond) // let the visibility deadline pass

	n, err := b.Reap(ctx, "emails")
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 2 {
		t.Fatalf("Reap returned %d, want 2", n)
	}
	if got := fm.get(fm.reaped, "emails"); got != 2 {
		t.Errorf("reaped[emails] = %d, want 2", got)
	}
}

func TestPromoteRecordsPromotedCount(t *testing.T) {
	fm := newFakeMetrics()
	b, _ := newTestBrokerWith(t, broker.WithMetrics(fm))
	ctx := context.Background()

	// Enqueue two delayed jobs whose ready-at is already in the past.
	past := time.Now().Add(-time.Second)
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x")), broker.WithReadyAt(past)); err != nil {
			t.Fatalf("Enqueue delayed: %v", err)
		}
	}

	n, err := b.Promote(ctx, "emails")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if n != 2 {
		t.Fatalf("Promote returned %d, want 2", n)
	}
	if got := fm.get(fm.promoted, "emails"); got != 2 {
		t.Errorf("promoted[emails] = %d, want 2", got)
	}
}
```

Note on `WithReadyAt(past)`: `Enqueue` routes to the delayed set only when `cfg.readyAt.After(now)`. A past time would route straight to ready and Promote would find nothing. If `WithReadyAt` with a past time does NOT create a delayed job in this codebase, instead enqueue with a near-future ready-at and sleep past it:

```go
	soon := time.Now().Add(20 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x")), broker.WithReadyAt(soon)); err != nil {
			t.Fatalf("Enqueue delayed: %v", err)
		}
	}
	time.Sleep(40 * time.Millisecond)
```

Use whichever form makes the job land in the delayed set; verify by asserting `n == 2` (the test fails loudly if the jobs were not delayed).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestReapRecordsReapedCount|TestPromoteRecordsPromotedCount' -v`
Expected: FAIL — `reaped[emails] = 0, want 2` / `promoted[emails] = 0, want 2`.

- [ ] **Step 3: Add Reap + Promote instrumentation**

In `Reap`, after a successful run replace `return n, nil` with:

```go
	if n > 0 {
		b.metrics.AddReaped(queue, n)
	}
	return n, nil
```

In `Promote`, after a successful run replace `return n, nil` with:

```go
	if n > 0 {
		b.metrics.AddPromoted(queue, n)
	}
	return n, nil
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestReapRecordsReapedCount|TestPromoteRecordsPromotedCount' -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Full broker suite under race**

Run: `go test -race ./internal/broker/`
Expected: PASS (all existing + new tests; confirms instrumentation didn't disturb behavior).

- [ ] **Step 6: Commit**

```bash
git add internal/broker/broker.go internal/broker/instrumentation_test.go
git commit -m "Instrument Reap and Promote with batch count metrics"
```

---

## Task 7: `internal/metrics` Recorder (Prometheus)

**Files:**
- Create: `internal/metrics/recorder.go`
- Create: `internal/metrics/recorder_test.go`
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

- [ ] **Step 1: Write the failing test**

Create `internal/metrics/recorder_test.go`:

```go
package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/metrics"
)

// Compile-time proof the Recorder satisfies the broker's instrumentation contract.
var _ broker.Metrics = (*metrics.Recorder)(nil)

func TestRecorderCountersIncrement(t *testing.T) {
	rec := metrics.NewRecorder()

	rec.IncEnqueued("emails")
	rec.IncEnqueued("emails")
	rec.IncDeduplicated("emails")
	rec.IncClaimed("emails")
	rec.IncProcessed("emails")
	rec.IncRetried("emails")
	rec.IncDead("emails")
	rec.AddReaped("emails", 3)
	rec.AddPromoted("emails", 4)

	checks := []struct {
		name   string
		metric string
		want   float64
	}{
		{"enqueued", "relay_jobs_enqueued_total", 2},
		{"deduped", "relay_jobs_deduplicated_total", 1},
		{"claimed", "relay_jobs_claimed_total", 1},
		{"processed", "relay_jobs_processed_total", 1},
		{"retried", "relay_jobs_retried_total", 1},
		{"dead", "relay_jobs_dead_total", 1},
		{"reaped", "relay_jobs_reaped_total", 3},
		{"promoted", "relay_jobs_promoted_total", 4},
	}
	for _, c := range checks {
		if got := testutil.ToFloat64(rec.CounterForTest(c.name, "emails")); got != c.want {
			t.Errorf("%s{queue=emails} = %v, want %v", c.metric, got, c.want)
		}
	}
}

func TestRecorderObservesLatency(t *testing.T) {
	rec := metrics.NewRecorder()
	rec.ObserveLatency("emails", 250*time.Millisecond)

	// One observation must be recorded in the histogram for queue=emails.
	got := testutil.CollectAndCount(rec.Registry(), "relay_job_latency_seconds")
	if got == 0 {
		t.Fatal("relay_job_latency_seconds: no series collected after one observation")
	}
}
```

Helper `CounterForTest` is a small test-only accessor (defined in the impl so the test can fetch the right `prometheus.Counter` child). It returns the per-queue child counter for the named metric.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/metrics/ -run 'TestRecorder' -v`
Expected: FAIL — package `internal/metrics` / `prometheus/client_golang` not found.

- [ ] **Step 3: Implement the Recorder**

Create `internal/metrics/recorder.go`:

```go
// Package metrics provides the Prometheus implementation of the broker's
// instrumentation contract (broker.Metrics) plus a pull-based collector for
// per-queue depth gauges. It deliberately does not import internal/broker: it
// satisfies broker.Metrics structurally, keeping the dependency arrow one-way.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "relay"

// Recorder implements broker.Metrics over a private Prometheus registry. Build
// it with NewRecorder, install it with broker.WithMetrics(rec), and serve
// rec.Registry() over HTTP. Counters and the latency histogram are labelled by
// queue.
type Recorder struct {
	reg *prometheus.Registry

	enqueued  *prometheus.CounterVec
	deduped   *prometheus.CounterVec
	claimed   *prometheus.CounterVec
	processed *prometheus.CounterVec
	retried   *prometheus.CounterVec
	dead      *prometheus.CounterVec
	reaped    *prometheus.CounterVec
	promoted  *prometheus.CounterVec
	latency   *prometheus.HistogramVec
}

// NewRecorder builds a Recorder with all metrics registered on a fresh registry.
func NewRecorder() *Recorder {
	reg := prometheus.NewRegistry()

	counter := func(name, help string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: name, Help: help,
		}, []string{"queue"})
		reg.MustRegister(c)
		return c
	}

	r := &Recorder{
		reg:       reg,
		enqueued:  counter("jobs_enqueued_total", "Jobs accepted into a queue."),
		deduped:   counter("jobs_deduplicated_total", "Enqueues dropped as idempotency-key duplicates."),
		claimed:   counter("jobs_claimed_total", "Jobs claimed by a worker."),
		processed: counter("jobs_processed_total", "Jobs acked after successful processing."),
		retried:   counter("jobs_retried_total", "Failed jobs requeued for retry."),
		dead:      counter("jobs_dead_total", "Jobs moved to the dead-letter queue."),
		reaped:    counter("jobs_reaped_total", "Expired in-flight jobs requeued by the reaper."),
		promoted:  counter("jobs_promoted_total", "Delayed jobs promoted to ready."),
	}
	r.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "job_latency_seconds",
		Help:      "End-to-end time from enqueue to ack.",
		// End-to-end latency spans ms to minutes, wider than the default
		// 5ms-10s buckets: ~1ms -> ~9min across 13 buckets.
		Buckets: prometheus.ExponentialBuckets(0.001, 3, 13),
	}, []string{"queue"})
	reg.MustRegister(r.latency)
	return r
}

// Registry exposes the underlying registry for an HTTP handler and for
// registering additional collectors (e.g. DepthCollector).
func (r *Recorder) Registry() *prometheus.Registry { return r.reg }

func (r *Recorder) IncEnqueued(q string)     { r.enqueued.WithLabelValues(q).Inc() }
func (r *Recorder) IncDeduplicated(q string) { r.deduped.WithLabelValues(q).Inc() }
func (r *Recorder) IncClaimed(q string)      { r.claimed.WithLabelValues(q).Inc() }
func (r *Recorder) IncProcessed(q string)    { r.processed.WithLabelValues(q).Inc() }
func (r *Recorder) IncRetried(q string)      { r.retried.WithLabelValues(q).Inc() }
func (r *Recorder) IncDead(q string)         { r.dead.WithLabelValues(q).Inc() }
func (r *Recorder) AddReaped(q string, n int) {
	r.reaped.WithLabelValues(q).Add(float64(n))
}
func (r *Recorder) AddPromoted(q string, n int) {
	r.promoted.WithLabelValues(q).Add(float64(n))
}
func (r *Recorder) ObserveLatency(q string, d time.Duration) {
	r.latency.WithLabelValues(q).Observe(d.Seconds())
}

// CounterForTest returns the per-queue child counter for the named metric. It
// exists so tests can read a specific series; "name" is the short key
// (enqueued, deduped, claimed, processed, retried, dead, reaped, promoted).
func (r *Recorder) CounterForTest(name, queue string) prometheus.Counter {
	var v *prometheus.CounterVec
	switch name {
	case "enqueued":
		v = r.enqueued
	case "deduped":
		v = r.deduped
	case "claimed":
		v = r.claimed
	case "processed":
		v = r.processed
	case "retried":
		v = r.retried
	case "dead":
		v = r.dead
	case "reaped":
		v = r.reaped
	case "promoted":
		v = r.promoted
	default:
		return nil
	}
	return v.WithLabelValues(queue)
}
```

- [ ] **Step 4: Tidy modules and run the test**

Run:
```bash
go mod tidy
go test ./internal/metrics/ -run 'TestRecorder' -v
```
Expected: `go mod tidy` adds `github.com/prometheus/client_golang` (and transitive deps) to go.mod/go.sum; tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/metrics/recorder.go internal/metrics/recorder_test.go go.mod go.sum
git commit -m "Add internal/metrics Recorder implementing broker.Metrics over Prometheus"
```

---

## Task 8: `internal/metrics` DepthCollector

**Files:**
- Create: `internal/metrics/depth.go`
- Create: `internal/metrics/depth_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/metrics/depth_test.go`:

```go
package metrics_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/metrics"
)

// metricsTestRedisDB is this package's dedicated Redis DB. broker tests use 15,
// worker tests 14; metrics claims 13 so parallel `go test ./...` never collides.
const metricsTestRedisDB = 13

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: metricsTestRedisDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestDepthCollectorReportsDepths(t *testing.T) {
	rdb := newTestRedis(t)
	ctx := context.Background()

	// Seed known cardinalities into the per-queue keys the collector reads.
	rdb.ZAdd(ctx, "q:emails:ready", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
	rdb.ZAdd(ctx, "q:emails:inflight", redis.Z{Score: 1, Member: "c"})
	rdb.ZAdd(ctx, "q:emails:delayed", redis.Z{Score: 1, Member: "d"}, redis.Z{Score: 2, Member: "e"}, redis.Z{Score: 3, Member: "f"})
	rdb.RPush(ctx, "q:emails:dlq", "g")

	c := metrics.NewDepthCollector(rdb, "emails")

	want := `
# HELP relay_queue_depth Number of jobs in a queue, by state.
# TYPE relay_queue_depth gauge
relay_queue_depth{queue="emails",state="ready"} 2
relay_queue_depth{queue="emails",state="inflight"} 1
relay_queue_depth{queue="emails",state="delayed"} 3
relay_queue_depth{queue="emails",state="dlq"} 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "relay_queue_depth"); err != nil {
		t.Errorf("CollectAndCompare: %v", err)
	}
}

func TestDepthCollectorEmptyQueueReportsZeros(t *testing.T) {
	rdb := newTestRedis(t)
	c := metrics.NewDepthCollector(rdb, "emails")

	want := `
# HELP relay_queue_depth Number of jobs in a queue, by state.
# TYPE relay_queue_depth gauge
relay_queue_depth{queue="emails",state="ready"} 0
relay_queue_depth{queue="emails",state="inflight"} 0
relay_queue_depth{queue="emails",state="delayed"} 0
relay_queue_depth{queue="emails",state="dlq"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "relay_queue_depth"); err != nil {
		t.Errorf("CollectAndCompare: %v", err)
	}
}

// Ensures the collector can be registered on a registry without panicking.
func TestDepthCollectorRegisters(t *testing.T) {
	rdb := newTestRedis(t)
	reg := prometheus.NewRegistry()
	if err := reg.Register(metrics.NewDepthCollector(rdb, "emails")); err != nil {
		t.Fatalf("Register: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/metrics/ -run TestDepthCollector -v`
Expected: FAIL — `NewDepthCollector` undefined.

- [ ] **Step 3: Implement the DepthCollector**

Create `internal/metrics/depth.go`:

```go
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// scrapeTimeout bounds the Redis work done during one Prometheus scrape so a
// slow or unreachable Redis cannot hang the /metrics handler.
const scrapeTimeout = 5 * time.Second

// DepthCollector reports per-queue depth gauges by reading Redis at scrape time,
// so the values can never go stale between scrapes. It implements
// prometheus.Collector and is registered on the Recorder's registry.
type DepthCollector struct {
	rdb    *redis.Client
	queues []string
	desc   *prometheus.Desc
}

// NewDepthCollector builds a collector that reports depths for the given queues.
func NewDepthCollector(rdb *redis.Client, queues ...string) *DepthCollector {
	return &DepthCollector{
		rdb:    rdb,
		queues: queues,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "queue_depth"),
			"Number of jobs in a queue, by state.",
			[]string{"queue", "state"}, nil,
		),
	}
}

// Describe sends the single gauge descriptor. (Required by prometheus.Collector.)
func (c *DepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect reads ready/inflight/delayed (ZCARD) and dlq (LLEN) for each queue and
// emits one gauge sample per (queue, state). On a Redis error for a given query
// it skips that sample rather than reporting a misleading zero.
func (c *DepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	for _, q := range c.queues {
		c.emit(ctx, ch, q, "ready", c.rdb.ZCard(ctx, "q:"+q+":ready"))
		c.emit(ctx, ch, q, "inflight", c.rdb.ZCard(ctx, "q:"+q+":inflight"))
		c.emit(ctx, ch, q, "delayed", c.rdb.ZCard(ctx, "q:"+q+":delayed"))
		c.emit(ctx, ch, q, "dlq", c.rdb.LLen(ctx, "q:"+q+":dlq"))
	}
}

// emit turns one IntCmd result into a gauge sample, skipping it on error.
func (c *DepthCollector) emit(_ context.Context, ch chan<- prometheus.Metric, queue, state string, cmd *redis.IntCmd) {
	n, err := cmd.Result()
	if err != nil {
		return // skip this sample; do not report a stale/zero depth
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), queue, state)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/metrics/ -run TestDepthCollector -v`
Expected: PASS (3 tests). If `CollectAndCompare` complains about sample ordering, the expected text above lists states in the emit order (ready, inflight, delayed, dlq); `CollectAndCompare` sorts internally, so order in the literal does not matter — fix any mismatch by correcting the expected values, not the code.

- [ ] **Step 5: Full metrics suite under race**

Run: `go test -race ./internal/metrics/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/metrics/depth.go internal/metrics/depth_test.go
git commit -m "Add DepthCollector reporting per-queue depth gauges at scrape time"
```

---

## Task 9: Wire `/metrics` into `cmd/worker`

**Files:**
- Modify: `cmd/worker/main.go`

- [ ] **Step 1: Add the flag and wiring**

Read `cmd/worker/main.go` first to match its existing flag/broker/shutdown style. Then:

1. Add a flag near the others:

```go
	metricsAddr := flag.String("metrics-addr", "", "if set (e.g. :9090), serve Prometheus /metrics on this address")
```

2. When building the broker, conditionally add the recorder. Build the broker options slice so `WithMetrics` is appended only when metrics are enabled, and keep a handle to the recorder:

```go
	var rec *metrics.Recorder
	brokerOpts := []broker.Option{broker.WithBackoff(/* existing args unchanged */)}
	// ... keep existing conditional WithRateLimit append ...
	if *metricsAddr != "" {
		rec = metrics.NewRecorder()
		brokerOpts = append(brokerOpts, broker.WithMetrics(rec))
	}
	b := broker.New(rdb, brokerOpts...)
```

(Adapt to the file's current construction — the key change is appending `broker.WithMetrics(rec)` when `*metricsAddr != ""`. Do not change existing options.)

3. After the broker is built and before/alongside starting the worker pool, start the metrics HTTP server when enabled:

```go
	var metricsSrv *http.Server
	if rec != nil {
		rec.Registry().MustRegister(metrics.NewDepthCollector(rdb, *queue))
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(rec.Registry(), promhttp.HandlerOpts{}))
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: mux}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("relay worker: metrics server failed", "err", err)
			}
		}()
		logger.Info("relay worker: serving metrics", "addr", *metricsAddr)
	}
```

(Use the file's existing logger variable; if it uses `log` rather than `slog`, match that.)

4. In the shutdown path (after the worker pool stops, where ctx is cancelled on SIGINT/SIGTERM), gracefully close the server:

```go
	if metricsSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("relay worker: metrics shutdown", "err", err)
		}
	}
```

5. Add imports: `net/http`, `github.com/prometheus/client_golang/prometheus/promhttp`, `github.com/StrangeNoob/relay/internal/metrics` (and `context`/`time` if not already present).

- [ ] **Step 2: Build and vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both clean.

- [ ] **Step 3: Smoke check the flag (optional, requires Redis)**

Run (manual, then Ctrl-C):
```bash
go run ./cmd/worker -queue demo -concurrency 1 -metrics-addr :9090 &
sleep 1 && curl -s localhost:9090/metrics | grep relay_ | head
kill %1
```
Expected: `relay_*` metric lines printed (depths at least). Skip if no local Redis.

- [ ] **Step 4: Commit**

```bash
git add cmd/worker/main.go
git commit -m "Serve Prometheus /metrics from cmd/worker behind --metrics-addr"
```

---

## Task 10: Update CLAUDE.md and final verification

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Make these edits (match exact surrounding wording when editing):

1. **Status line** — change "Phase 2 nearly complete … only Prometheus metrics remain in Phase 2" to state Phase 2 is complete (metrics shipped). Example: "**Status: Phase 1 complete; Phase 2 complete.** The core engine plus delayed jobs, the promoter, retry backoff, priority, idempotency enforcement, per-queue rate limiting, and Prometheus metrics are built, tested against a real Redis under `-race`, and CI is green."
2. **Remaining-Phase-2 line** — remove the "(Prometheus metrics)" remaining note; only Phase 3 remains.
3. **Broker options** — add `WithMetrics(m)` to the broker-options sentence in the "What this is" section.
4. **`internal/metrics`** — in the Layout block, change the metrics entry to ✅ built and add `cmd/worker --metrics-addr`. Add `internal/metrics/` (Recorder + DepthCollector) to the built list.
5. **Build order** — Phase 2 line: change "metrics still to do" to metrics ✅; Phase 2 done.
6. **Known limitations** — add a metrics bullet: per-queue label cardinality; depth gauges cost one Redis round-trip per (queue,state) per scrape; metrics are per-process (aggregate depth gauges with max/avg, not sum); endpoint only on `cmd/worker` until Phase 3.
7. **Dependencies** — update the "Only dependency" wording to note `github.com/prometheus/client_golang` is a second direct dependency, used purely for metrics instrumentation (not a queue library — the from-scratch rule is intact).
8. **Script/option inventories** — if a broker-options inventory exists, ensure `WithMetrics` is listed.

- [ ] **Step 2: Full verification**

Run:
```bash
go build ./...
go test -race ./...
go vet ./...
gofmt -l internal/ cmd/
```
Expected: build clean; all tests pass (broker DB 15, worker DB 14, metrics DB 13 — no collisions); vet clean; `gofmt -l` prints nothing.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Document Phase 2 completion: Prometheus metrics"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** Metrics interface + noop + WithMetrics (Task 1); enqueue/dedup (Task 2); claim (Task 3); ack+latency (Task 4); retry/dead (Task 5); reap/promote (Task 6); Recorder + all metric names/buckets (Task 7); DepthCollector + depth gauges (Task 8); cmd/worker endpoint + graceful shutdown (Task 9); CLAUDE.md + dependency note + known limitations (Task 10). All spec sections mapped.
- **Type consistency:** `Metrics` method set is identical across the interface (Task 1), the fake (Task 2), and the Recorder (Task 7): `IncEnqueued/IncDeduplicated/IncClaimed/IncProcessed/IncRetried/IncDead` (single, queue), `AddReaped/AddPromoted` (queue, int), `ObserveLatency` (queue, time.Duration). Metric names match the spec table. Test DB numbers: broker 15, worker 14, metrics 13.
- **No placeholders:** every code step shows complete code; the only "adapt to existing file" note is Task 9 (cmd wiring), which lists the exact additions.
- **Known soft spot:** Task 6's delayed-job setup depends on whether `WithReadyAt(pastTime)` lands a job in the delayed set; the task gives both forms and asserts `n == 2` so the implementer can pick the one that actually delays.
