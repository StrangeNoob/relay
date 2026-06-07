// Command demo is a load generator: it enqueues a batch of jobs onto a queue so
// a running worker pool has something to chew on. Thin wiring over the client
// path of internal/broker.
package main

import (
	"context"
	"errors"
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
	delay := flag.Duration("delay", 0, "schedule jobs this far in the future (0 = immediate)")
	priority := flag.Int("priority", 0, "priority for enqueued jobs (higher is more urgent, 0-255)")
	idempotencyKey := flag.String("idempotency-key", "", "idempotency key applied to every enqueued job (empty = none)")
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
		var opts []broker.EnqueueOption
		if *delay > 0 {
			opts = append(opts, broker.WithDelay(*delay))
		}
		if *priority != 0 {
			opts = append(opts, broker.WithPriority(*priority))
		}
		if *idempotencyKey != "" {
			opts = append(opts, broker.WithIdempotencyKey(*idempotencyKey))
		}
		switch err := b.Enqueue(ctx, j, opts...); {
		case errors.Is(err, broker.ErrDuplicate):
			logger.Info("duplicate dropped", "i", i, "key", *idempotencyKey)
		case err != nil:
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
	}

	logger.Info("enqueued jobs", "count", *count, "queue", *queue, "redis", *addr)
}
