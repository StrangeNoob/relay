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

// newTestBroker connects to a real Redis on a dedicated test DB, flushes it, and
// returns a broker plus the raw client for assertions. Like the broker's own
// tests, the worker runtime is only meaningful against real Redis, so we use it
// directly and skip when it is unavailable.
func newTestBroker(t *testing.T) (*broker.Broker, *redis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
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
