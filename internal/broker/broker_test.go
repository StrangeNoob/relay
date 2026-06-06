package broker_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

// testRedisDB is this package's dedicated Redis logical database for tests.
//
// `go test ./...` runs each package's test binary in parallel, and every test
// here flushes its DB — so two packages sharing one DB would wipe and steal each
// other's jobs mid-test. Each Redis-using test package therefore claims its own
// DB number (broker uses 15, worker uses 14); a new one should pick another.
const testRedisDB = 15

// newTestBroker connects to a real Redis on this package's dedicated test DB,
// flushes it, and returns a broker together with the raw client so tests can
// assert directly on Redis state. Broker semantics (atomic claim especially)
// only mean something against real Redis, so we never mock it; if Redis is
// unreachable the test is skipped rather than failed, so the suite still runs
// off-CI.
func newTestBroker(t *testing.T) (*broker.Broker, *redis.Client) {
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
	return broker.New(rdb), rdb
}

func TestEnqueuePersistsJob(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := rdb.HGetAll(ctx, "job:"+j.ID).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if got["queue"] != "emails" {
		t.Errorf("stored queue = %q, want %q", got["queue"], "emails")
	}
	if got["payload"] != "hello" {
		t.Errorf("stored payload = %q, want %q", got["payload"], "hello")
	}
}

func TestEnqueueAddsToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	members, err := rdb.ZRange(ctx, "q:emails:ready", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange: %v", err)
	}
	if len(members) != 1 || members[0] != j.ID {
		t.Errorf("ready set = %v, want [%s]", members, j.ID)
	}
}

func TestClaimReturnsEnqueuedJob(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !ok {
		t.Fatal("Claim returned ok=false, want a job")
	}
	if got.ID != j.ID {
		t.Errorf("claimed ID = %q, want %q", got.ID, j.ID)
	}
	if got.Attempts != 1 {
		t.Errorf("claimed Attempts = %d, want 1 (bumped on claim)", got.Attempts)
	}
	if got.State != job.StateInFlight {
		t.Errorf("claimed State = %q, want %q", got.State, job.StateInFlight)
	}
}

func TestClaimMovesReadyToInflight(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	const visibility = time.Minute
	before := time.Now().UnixMilli()
	if _, _, err := b.Claim(ctx, "emails", visibility); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	after := time.Now().UnixMilli()

	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 0 {
		t.Errorf("ready set size = %d, want 0 after claim", n)
	}

	deadline, err := rdb.ZScore(ctx, "q:emails:inflight", j.ID).Result()
	if err != nil {
		t.Fatalf("job not in inflight set: %v", err)
	}
	lo, hi := float64(before+visibility.Milliseconds()), float64(after+visibility.Milliseconds())
	if deadline < lo || deadline > hi {
		t.Errorf("inflight deadline = %v, want within [%v, %v]", deadline, lo, hi)
	}
}

func TestClaimReturnsFalseWhenQueueEmpty(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	got, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if ok {
		t.Errorf("Claim returned ok=true (%+v), want false on empty queue", got)
	}
}

// claimOne enqueues j and immediately claims it back, returning the claimed
// (in-flight) job. It fails the test on any error so call sites stay readable.
func claimOne(t *testing.T, b *broker.Broker, j job.Job) job.Job {
	t.Helper()
	ctx := context.Background()
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, j.Queue, time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if !ok {
		t.Fatal("Claim returned ok=false, want the just-enqueued job")
	}
	return claimed
}

func TestAckDeletesJobHash(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	claimed := claimOne(t, b, job.New("emails", []byte("hello")))
	if err := b.Ack(ctx, claimed); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if n, _ := rdb.Exists(ctx, "job:"+claimed.ID).Result(); n != 0 {
		t.Errorf("job hash still exists after ack")
	}
}

func TestAckRemovesFromInflight(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	claimed := claimOne(t, b, job.New("emails", []byte("hello")))
	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 1 {
		t.Fatalf("precondition: inflight size = %d, want 1", n)
	}

	if err := b.Ack(ctx, claimed); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 0 {
		t.Errorf("inflight size = %d, want 0 after ack", n)
	}
}

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

	members, _ := rdb.ZRange(ctx, "q:emails:ready", 0, -1).Result()
	if len(members) != 1 || members[0] != claimed.ID {
		t.Errorf("ready set = %v, want requeued [%s]", members, claimed.ID)
	}

	state, _ := rdb.HGet(ctx, "job:"+claimed.ID, "state").Result()
	if state != string(job.StateReady) {
		t.Errorf("job state = %q, want %q", state, job.StateReady)
	}
}

func TestNackDeadLettersWhenExhausted(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// MaxRetries 1: a single claim bumps Attempts to 1, exhausting the budget.
	j := job.New("emails", []byte("hello"))
	j.MaxRetries = 1
	claimed := claimOne(t, b, j)

	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	dlq, _ := rdb.LRange(ctx, "q:emails:dlq", 0, -1).Result()
	if len(dlq) != 1 || dlq[0] != claimed.ID {
		t.Errorf("dlq = %v, want [%s]", dlq, claimed.ID)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 0 {
		t.Errorf("ready size = %d, want 0 (job should be dead-lettered, not requeued)", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 0 {
		t.Errorf("inflight size = %d, want 0", n)
	}

	state, _ := rdb.HGet(ctx, "job:"+claimed.ID, "state").Result()
	if state != string(job.StateDead) {
		t.Errorf("job state = %q, want %q", state, job.StateDead)
	}
}

func TestExtendPushesDeadlineForward(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	claimed := claimOne(t, b, job.New("emails", []byte("hello")))
	before, err := rdb.ZScore(ctx, "q:emails:inflight", claimed.ID).Result()
	if err != nil {
		t.Fatalf("precondition ZScore: %v", err)
	}

	ok, err := b.Extend(ctx, claimed, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if !ok {
		t.Fatal("Extend returned false, want true for an in-flight job")
	}

	after, err := rdb.ZScore(ctx, "q:emails:inflight", claimed.ID).Result()
	if err != nil {
		t.Fatalf("ZScore after: %v", err)
	}
	if after <= before {
		t.Errorf("deadline not extended: before=%v after=%v", before, after)
	}
}

func TestExtendReturnsFalseWhenNotInflight(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// A job that was never claimed is not in the inflight set.
	j := job.New("emails", []byte("hello"))
	ok, err := b.Extend(ctx, j, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if ok {
		t.Errorf("Extend returned true for a job that is not in flight")
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 0 {
		t.Errorf("Extend wrongly added a non-inflight job to inflight (size=%d)", n)
	}
}

func TestReapRequeuesExpiredJob(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Visibility 0 sets the deadline to the claim instant, so by the time Reap
	// reads the clock the job is already past due — no sleeping, no flakiness.
	claimed, ok, err := b.Claim(ctx, "emails", 0)
	if err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}

	n, err := b.Reap(ctx, "emails")
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 1 {
		t.Errorf("reaped %d jobs, want 1", n)
	}

	if c, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); c != 0 {
		t.Errorf("inflight size = %d, want 0 after reap", c)
	}
	members, _ := rdb.ZRange(ctx, "q:emails:ready", 0, -1).Result()
	if len(members) != 1 || members[0] != claimed.ID {
		t.Errorf("ready set = %v, want requeued [%s]", members, claimed.ID)
	}
	state, _ := rdb.HGet(ctx, "job:"+claimed.ID, "state").Result()
	if state != string(job.StateReady) {
		t.Errorf("job state = %q, want %q", state, job.StateReady)
	}
}

func TestReapLeavesUnexpiredJobs(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// A full minute of visibility: the job is nowhere near its deadline.
	claimed := claimOne(t, b, job.New("emails", []byte("hello")))

	n, err := b.Reap(ctx, "emails")
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 0 {
		t.Errorf("reaped %d jobs, want 0 (none expired)", n)
	}

	if c, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); c != 1 {
		t.Errorf("inflight size = %d, want 1 (job still in flight)", c)
	}
	if c, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); c != 0 {
		t.Errorf("ready size = %d, want 0 (job must not be requeued early)", c)
	}
	_ = claimed
}

// TestReapEnablesRedeliveryAfterCrash is the at-least-once-on-crash guarantee
// end to end: a worker claims a job and dies without acking; the reaper returns
// it to ready; a second worker re-claims the same job with its attempt count
// advanced. Composed of already-unit-tested steps, kept as a scenario guard.
func TestReapEnablesRedeliveryAfterCrash(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First worker claims with visibility 0, then "crashes" (never acks).
	first, ok, err := b.Claim(ctx, "emails", 0)
	if err != nil || !ok {
		t.Fatalf("first Claim: err=%v ok=%v", err, ok)
	}
	if first.Attempts != 1 {
		t.Errorf("first claim Attempts = %d, want 1", first.Attempts)
	}

	if n, err := b.Reap(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Reap: n=%d err=%v, want 1", n, err)
	}

	// A second worker re-claims the same job; the attempt count advances.
	second, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("re-Claim: err=%v ok=%v", err, ok)
	}
	if second.ID != first.ID {
		t.Errorf("re-claimed job %s, want the same job %s", second.ID, first.ID)
	}
	if second.Attempts != 2 {
		t.Errorf("re-claim Attempts = %d, want 2", second.Attempts)
	}
}

// TestConcurrentClaimsDeliverEachJobOnce is the competing-consumer safety check:
// many workers hammer one queue at once and every job must be claimed exactly
// once. If the claim were not atomic (e.g. peek-then-remove as two round-trips),
// two workers could grab the same id and this would catch it. Run under -race.
func TestConcurrentClaimsDeliverEachJobOnce(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	const njobs = 300
	enqueued := make(map[string]bool, njobs)
	for i := 0; i < njobs; i++ {
		j := job.New("emails", []byte("x"))
		enqueued[j.ID] = true
		if err := b.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	const nworkers = 8
	var mu sync.Mutex
	claimedCount := make(map[string]int)

	var wg sync.WaitGroup
	for w := 0; w < nworkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok, err := b.Claim(ctx, "emails", time.Minute)
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if !ok {
					return // queue drained
				}
				mu.Lock()
				claimedCount[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimedCount) != njobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimedCount), njobs)
	}
	for id, c := range claimedCount {
		if c != 1 {
			t.Errorf("job %s claimed %d times, want exactly 1", id, c)
		}
		if !enqueued[id] {
			t.Errorf("claimed an unknown job %s", id)
		}
	}
}
