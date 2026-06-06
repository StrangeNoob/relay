// Command demo is a load generator: it enqueues a batch of jobs onto a queue so
// a running worker pool has something to chew on. Thin wiring over the client
// path of internal/broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

func main() {
	addr := flag.String("redis", "localhost:6379", "Redis address")
	queue := flag.String("queue", "default", "queue to enqueue into")
	count := flag.Int("count", 100, "number of jobs to enqueue")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("cannot reach redis", "addr", *addr, "err", err)
		os.Exit(1)
	}

	b := broker.New(rdb)
	for i := range *count {
		payload := fmt.Sprintf(`{"n":%d}`, i)
		j := job.New(*queue, []byte(payload))
		if err := b.Enqueue(ctx, j); err != nil {
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
	}

	logger.Info("enqueued jobs", "count", *count, "queue", *queue, "redis", *addr)
}
