package client_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/api"
	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/client"
	"github.com/StrangeNoob/relay/internal/job"
)

// clientTestRedisDB is this package's dedicated Redis DB. broker tests use 15,
// worker 14, metrics 13, api 12; client claims 11 so `go test ./...` never
// collides.
const clientTestRedisDB = 11

// newRoundTrip stands up a real broker + API behind an httptest server and a
// client pointed at it. Skips when Redis is unreachable.
func newRoundTrip(t *testing.T) (*client.Client, *broker.Broker, *redis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: clientTestRedisDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	b := broker.New(rdb)
	srv := httptest.NewServer(api.New(b, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		srv.Close()
		_ = rdb.Close()
	})
	return client.New(srv.URL), b, rdb
}

func TestRoundTripEnqueueStatsQueues(t *testing.T) {
	c, _, _ := newRoundTrip(t)
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, "emails", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	s, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 1 {
		t.Errorf("ready = %d, want 1", s.Ready)
	}
	qs, err := c.Queues(ctx)
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(qs) != 1 || qs[0] != "emails" {
		t.Errorf("queues = %v, want [emails]", qs)
	}
}

func TestRoundTripDLQListAndRequeue(t *testing.T) {
	c, b, _ := newRoundTrip(t)
	ctx := context.Background()

	// Drive one job to the DLQ via the broker (enqueue maxRetries=0 -> claim -> nack).
	j := job.New("emails", []byte("dead"))
	j.MaxRetries = 0
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	jobs, err := c.ListDLQ(ctx, "emails", 50, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != claimed.ID || jobs[0].State != "dead" {
		t.Fatalf("dlq jobs = %+v", jobs)
	}

	if err := c.Requeue(ctx, "emails", claimed.ID); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	s, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.DLQ != 0 || s.Ready != 1 {
		t.Errorf("after requeue stats = %+v, want dlq 0 ready 1", s)
	}

	if err := c.Requeue(ctx, "emails", "nope"); err == nil {
		t.Error("Requeue unknown id: want error")
	}
}
