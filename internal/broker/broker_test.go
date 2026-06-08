package broker_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
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

func TestEnqueueWithZeroReadyAtGoesToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j, broker.WithReadyAt(time.Time{})); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 0 {
		t.Errorf("delayed size = %d, want 0 (zero ready-at is immediate)", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready size = %d, want 1", n)
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
	// is 1s and the jittered ready-at lands within [now, now+1s).
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
	if n, _ := rdb.ZCard(ctx, "q:emails:delayed").Result(); n != 0 {
		t.Errorf("delayed size = %d, want 0 (exhausted job should be dead, not delayed)", n)
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

	before := time.Now().UnixMilli()
	n, err := b.Promote(ctx, "emails")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	after := time.Now().UnixMilli()
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
	attempts, _ := rdb.HGet(ctx, "job:"+j.ID, "attempts").Result()
	if attempts != "0" {
		t.Errorf("attempts = %q, want 0 (promotion must not count as a delivery)", attempts)
	}
	score, _ := rdb.ZScore(ctx, "q:emails:ready", j.ID).Result()
	// priority 0 → readyScore(0, now) == -now, where now is the promotion time.
	if score < -float64(after) || score > -float64(before) {
		t.Errorf("ready score = %v, want within [%v, %v]", score, -float64(after), -float64(before))
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

// TestConcurrentClaimsDeliverEachJobOnce is the competing-consumer safety check:
// many workers hammer one queue at once and every job must be claimed exactly
// once. If the claim were not atomic (e.g. peek-then-remove as two round-trips),
// two workers could grab the same id and this would catch it. Run under -race.
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
	// score = priority*priorityScale - nowMs; scale is 1e13 (see priority.go).
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
	time.Sleep(2 * time.Millisecond)
	second := job.New("emails", []byte("second"))
	second.Priority = 5
	if err := b.Enqueue(ctx, second); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	got1, ok1, err1 := b.Claim(ctx, "emails", time.Minute)
	if err1 != nil || !ok1 {
		t.Fatalf("first Claim: err=%v ok=%v", err1, ok1)
	}
	got2, ok2, err2 := b.Claim(ctx, "emails", time.Minute)
	if err2 != nil || !ok2 {
		t.Fatalf("second Claim: err=%v ok=%v", err2, ok2)
	}
	if got1.ID != first.ID {
		t.Errorf("first claim = %s, want oldest %s", got1.ID, first.ID)
	}
	if got2.ID != second.ID {
		t.Errorf("second claim = %s, want %s", got2.ID, second.ID)
	}
}

func TestWithPriorityZeroOverridesJobPriority(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("x"))
	j.Priority = 7
	if err := b.Enqueue(ctx, j, broker.WithPriority(0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if p, _ := rdb.HGet(ctx, "job:"+j.ID, "priority").Result(); p != "0" {
		t.Errorf("priority = %q, want 0 (WithPriority(0) must override job.Priority=7)", p)
	}
}

func TestPromotePreservesPriority(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

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
	// Force the delayed job due now so Promote picks it up this pass.
	if err := rdb.ZAdd(ctx, "q:emails:delayed",
		redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: high.ID}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if n, err := b.Promote(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Promote: n=%d err=%v, want 1", n, err)
	}

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

	high := job.New("emails", []byte("high"))
	high.Priority = 8
	if err := b.Enqueue(ctx, high); err != nil {
		t.Fatalf("Enqueue high: %v", err)
	}
	// visibility 0 sets the inflight deadline to the claim's now; Reap passes its
	// own now (>= that), so the job is immediately eligible to be reaped.
	if _, ok, err := b.Claim(ctx, "emails", 0); err != nil || !ok {
		t.Fatalf("Claim high: err=%v ok=%v", err, ok)
	}
	mid := job.New("emails", []byte("mid"))
	mid.Priority = 3
	if err := b.Enqueue(ctx, mid); err != nil {
		t.Fatalf("Enqueue mid: %v", err)
	}
	if n, err := b.Reap(ctx, "emails"); err != nil || n != 1 {
		t.Fatalf("Reap: n=%d err=%v, want 1", n, err)
	}

	got, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}
	if got.ID != high.ID || got.Priority != 8 {
		t.Errorf("claimed id=%s prio=%d, want high id=%s prio=8", got.ID, got.Priority, high.ID)
	}
}

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

func TestWithIdempotencyKeyOverridesJobKey(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("x"))
	j.IdempotencyKey = "preset"
	if err := b.Enqueue(ctx, j, broker.WithIdempotencyKey("override")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if n, _ := rdb.Exists(ctx, "q:emails:dedup:override").Result(); n != 1 {
		t.Errorf("marker q:emails:dedup:override missing; override not applied")
	}
	if n, _ := rdb.Exists(ctx, "q:emails:dedup:preset").Result(); n != 0 {
		t.Errorf("marker q:emails:dedup:preset exists; preset key was not overridden")
	}
}

func TestDedupIsolatedPerQueue(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("a")), broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("Enqueue emails: %v", err)
	}
	// Same key on a different queue must NOT be treated as a duplicate.
	if err := b.Enqueue(ctx, job.New("sms", []byte("b")), broker.WithIdempotencyKey("k1")); err != nil {
		t.Fatalf("Enqueue sms: %v", err)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("emails ready = %d, want 1", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:sms:ready").Result(); n != 1 {
		t.Errorf("sms ready = %d, want 1", n)
	}
}

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

	for i := 0; i < 5; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil {
			t.Fatalf("empty claim %d: %v", i, err)
		} else if ok {
			t.Fatalf("empty claim %d returned a job", i)
		}
	}
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

// TestRateLimitConcurrentClaimsRespectBurst verifies that the atomic token-bucket
// inside claim.lua never over-issues under concurrent pressure. With burst=5 and a
// negligible refill rate, exactly 5 of 20 simultaneous goroutines should succeed;
// the remaining 15 must be denied by the Lua script without a race.
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

// deadLetter enqueues a job with no retry budget, claims it, and nacks it so it
// lands in the DLQ; it returns the dead-lettered job's id.
func deadLetter(t *testing.T, b *broker.Broker, ctx context.Context, queue, payload string) string {
	t.Helper()
	j := job.New(queue, []byte(payload))
	j.MaxRetries = 0
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, queue, time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	return claimed.ID
}

func TestListDLQReturnsDeadJobs(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	id1 := deadLetter(t, b, ctx, "emails", "a")
	id2 := deadLetter(t, b, ctx, "emails", "b")

	jobs, err := b.ListDLQ(ctx, "emails", 0, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("len = %d, want 2", len(jobs))
	}
	if jobs[0].ID != id1 || jobs[1].ID != id2 {
		t.Errorf("ids = %s,%s want %s,%s", jobs[0].ID, jobs[1].ID, id1, id2)
	}
	if jobs[0].State != job.StateDead {
		t.Errorf("state = %q, want dead", jobs[0].State)
	}
}

func TestListDLQPaginates(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		deadLetter(t, b, ctx, "emails", "x")
	}
	page, err := b.ListDLQ(ctx, "emails", 2, 1) // limit 2, offset 1 -> items 2 and 3
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(page) != 2 {
		t.Errorf("len = %d, want 2", len(page))
	}
}

func TestListDLQEmpty(t *testing.T) {
	b, _ := newTestBroker(t)
	jobs, err := b.ListDLQ(context.Background(), "emails", 0, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0", len(jobs))
	}
}

func TestStatsCountsEachState(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// 2 ready
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("r"))); err != nil {
			t.Fatalf("Enqueue ready: %v", err)
		}
	}
	// 1 delayed
	if err := b.Enqueue(ctx, job.New("emails", []byte("d")), broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}
	// 1 inflight: enqueue then claim it
	if err := b.Enqueue(ctx, job.New("emails", []byte("i"))); err != nil {
		t.Fatalf("Enqueue inflight: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	// 1 dlq: push an id directly so the count is unambiguous
	if err := rdb.RPush(ctx, "q:emails:dlq", "deadid").Err(); err != nil {
		t.Fatalf("seed dlq: %v", err)
	}

	s, err := b.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 2 {
		t.Errorf("Ready = %d, want 2", s.Ready)
	}
	if s.Inflight != 1 {
		t.Errorf("Inflight = %d, want 1", s.Inflight)
	}
	if s.Delayed != 1 {
		t.Errorf("Delayed = %d, want 1", s.Delayed)
	}
	if s.DLQ != 1 {
		t.Errorf("DLQ = %d, want 1", s.DLQ)
	}
}

func TestRequeueDLQMovesJobBackToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	id := deadLetter(t, b, ctx, "emails", "x")

	ok, err := b.RequeueDLQ(ctx, "emails", id)
	if err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}
	if !ok {
		t.Fatal("RequeueDLQ returned false, want true")
	}

	if n, _ := rdb.LLen(ctx, "q:emails:dlq").Result(); n != 0 {
		t.Errorf("dlq len = %d, want 0", n)
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready card = %d, want 1", n)
	}
	h, err := rdb.HGetAll(ctx, "job:"+id).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if h["state"] != "ready" {
		t.Errorf("state = %q, want ready", h["state"])
	}
	if h["attempts"] != "0" {
		t.Errorf("attempts = %q, want 0", h["attempts"])
	}

	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim after requeue: ok=%v err=%v", ok, err)
	}
}

func TestRequeueDLQUnknownIDReturnsFalse(t *testing.T) {
	b, _ := newTestBroker(t)
	ok, err := b.RequeueDLQ(context.Background(), "emails", "nope")
	if err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}
	if ok {
		t.Error("RequeueDLQ returned true for an id not in the DLQ, want false")
	}
}

func TestQueuesDiscoversDistinctNames(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("a"))); err != nil {
		t.Fatalf("Enqueue emails: %v", err)
	}
	if err := b.Enqueue(ctx, job.New("sms", []byte("b"))); err != nil {
		t.Fatalf("Enqueue sms: %v", err)
	}
	// a second key family for the same queue must not double-count it
	if err := b.Enqueue(ctx, job.New("emails", []byte("c")), broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue emails delayed: %v", err)
	}

	names, err := b.Queues(ctx)
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(names) != 2 || names[0] != "emails" || names[1] != "sms" {
		t.Errorf("names = %v, want [emails sms]", names)
	}
}

func TestQueuesEmpty(t *testing.T) {
	b, _ := newTestBroker(t)
	names, err := b.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("names = %v, want empty", names)
	}
}
