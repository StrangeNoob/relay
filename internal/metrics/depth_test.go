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

func TestDepthCollectorRegisters(t *testing.T) {
	rdb := newTestRedis(t)
	reg := prometheus.NewRegistry()
	if err := reg.Register(metrics.NewDepthCollector(rdb, "emails")); err != nil {
		t.Fatalf("Register: %v", err)
	}
}
