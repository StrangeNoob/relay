package broker_test

import (
	"context"
	"errors"
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
	mu        sync.Mutex
	enqueued  map[string]int
	deduped   map[string]int
	claimed   map[string]int
	processed map[string]int
	retried   map[string]int
	dead      map[string]int
	reaped    map[string]int
	promoted  map[string]int
	latencies []time.Duration
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

func (f *fakeMetrics) IncEnqueued(q string)        { f.bump(f.enqueued, q) }
func (f *fakeMetrics) IncDeduplicated(q string)    { f.bump(f.deduped, q) }
func (f *fakeMetrics) IncClaimed(q string)         { f.bump(f.claimed, q) }
func (f *fakeMetrics) IncProcessed(q string)       { f.bump(f.processed, q) }
func (f *fakeMetrics) IncRetried(q string)         { f.bump(f.retried, q) }
func (f *fakeMetrics) IncDead(q string)            { f.bump(f.dead, q) }
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
	if err := b.Enqueue(ctx, j2, broker.WithIdempotencyKey("k1")); !errors.Is(err, broker.ErrDuplicate) {
		t.Fatalf("second Enqueue err = %v, want ErrDuplicate", err)
	}
	if got := fm.get(fm.enqueued, "emails"); got != 1 {
		t.Errorf("enqueued[emails] = %d, want 1", got)
	}
	if got := fm.get(fm.deduped, "emails"); got != 1 {
		t.Errorf("deduped[emails] = %d, want 1", got)
	}
}

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

	// Enqueue two delayed jobs whose ready-at is just in the future, then wait
	// well past it. The sleep margin is generous (>3x) so the test stays reliable
	// on a loaded CI runner under -race.
	soon := time.Now().Add(30 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("x")), broker.WithReadyAt(soon)); err != nil {
			t.Fatalf("Enqueue delayed: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

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
