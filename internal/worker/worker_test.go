package worker_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
	"github.com/StrangeNoob/relay/internal/worker"
)

// testRedisDB is this package's dedicated Redis logical database for tests.
//
// `go test ./...` runs each package's test binary in parallel, and every test
// here flushes its DB — so two packages sharing one DB would wipe and steal each
// other's jobs mid-test. Each Redis-using test package therefore claims its own
// DB number (broker uses 15, worker uses 14); a new one should pick another.
const testRedisDB = 14

// newTestBroker connects to a real Redis on this package's dedicated test DB,
// flushes it, and returns a broker plus the raw client for assertions. Like the
// broker's own tests, the worker runtime is only meaningful against real Redis,
// so we use it directly and skip when it is unavailable.
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

func TestWorkerProcessesAndAcksJob(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	processed := make(chan job.Job, 1)
	handler := func(_ context.Context, got job.Job) error {
		processed <- got
		return nil
	}
	w := worker.New(b, "emails", handler)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	select {
	case got := <-processed:
		if got.ID != j.ID {
			t.Errorf("handler got job %s, want %s", got.ID, j.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2s")
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil on graceful shutdown", err)
	}

	if n, _ := rdb.Exists(ctx, "job:"+j.ID).Result(); n != 0 {
		t.Errorf("job hash still exists; job was not acked")
	}
}

func TestWorkerNacksJobOnHandlerError(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// MaxRetries 1 means the first failure exhausts the budget and the job is
	// dead-lettered, giving a stable terminal state to assert (no retry churn).
	j := job.New("emails", []byte("hello"))
	j.MaxRetries = 1
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	failed := make(chan struct{}, 1)
	handler := func(_ context.Context, _ job.Job) error {
		failed <- struct{}{}
		return errors.New("boom")
	}
	w := worker.New(b, "emails", handler)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within 2s")
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}

	dlq, _ := rdb.LRange(ctx, "q:emails:dlq", 0, -1).Result()
	if len(dlq) != 1 || dlq[0] != j.ID {
		t.Errorf("dlq = %v, want dead-lettered [%s]", dlq, j.ID)
	}
}

func TestReaperRunRequeuesExpiredJobs(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Claim with visibility 0 so the job is immediately past due in inflight.
	if _, ok, err := b.Claim(ctx, "emails", 0); err != nil || !ok {
		t.Fatalf("Claim: err=%v ok=%v", err, ok)
	}

	r := worker.NewReaper(b, "emails", 20*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()

	// Wait for the background reaper to move the job back to ready.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reaper did not requeue the expired job within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}

	if n, _ := rdb.ZCard(ctx, "q:emails:inflight").Result(); n != 0 {
		t.Errorf("inflight size = %d, want 0 after reaper run", n)
	}
}

func TestWorkerHeartbeatsExtendDeadline(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, _ job.Job) error {
		close(started)
		<-release // hold the job in flight long enough to heartbeat
		return nil
	}
	w := worker.New(b, "emails", handler,
		worker.WithVisibility(200*time.Millisecond),
		worker.WithHeartbeatInterval(40*time.Millisecond),
	)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	<-started
	d1, err := rdb.ZScore(ctx, "q:emails:inflight", j.ID).Result()
	if err != nil {
		t.Fatalf("ZScore d1: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // several heartbeat intervals
	d2, err := rdb.ZScore(ctx, "q:emails:inflight", j.ID).Result()
	if err != nil {
		t.Fatalf("ZScore d2: %v", err)
	}
	if d2 <= d1 {
		t.Errorf("deadline not advanced by heartbeat: d1=%v d2=%v", d1, d2)
	}

	close(release)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}

func TestWorkerFinishesInflightJobOnShutdown(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("hello"))
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, _ job.Job) error {
		close(started)
		<-release // hold the job in flight until the test releases it
		return nil
	}
	w := worker.New(b, "emails", handler)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	<-started      // a job is now in flight
	cancel()       // shutdown is requested mid-handler
	close(release) // let the handler complete

	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}

	// Despite shutdown landing mid-handler, the finished job must be acked, not
	// abandoned in the inflight set.
	if n, _ := rdb.Exists(ctx, "job:"+j.ID).Result(); n != 0 {
		t.Errorf("in-flight job was not acked on graceful shutdown")
	}
}
