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
