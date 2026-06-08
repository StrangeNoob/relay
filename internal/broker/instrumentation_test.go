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
