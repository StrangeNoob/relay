// Command demo is a load generator: it enqueues a batch of jobs onto a queue
// through the Relay HTTP API (via internal/client) so a running worker pool has
// something to chew on. It is a pure SDK consumer — it needs cmd/server running.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/StrangeNoob/relay/internal/client"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "Relay server base URL")
	queue := flag.String("queue", "default", "queue to enqueue into")
	count := flag.Int("count", 100, "number of jobs to enqueue")
	delay := flag.Duration("delay", 0, "schedule jobs this far in the future (0 = immediate)")
	priority := flag.Int("priority", 0, "priority for enqueued jobs (higher is more urgent, 0-255)")
	idempotencyKey := flag.String("idempotency-key", "", "idempotency key applied to every enqueued job (empty = none)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()
	c := client.New(*server)

	for i := range *count {
		payload := fmt.Sprintf(`{"n":%d}`, i)
		var opts []client.EnqueueOption
		if *delay > 0 {
			opts = append(opts, client.WithDelay(*delay))
		}
		if *priority != 0 {
			opts = append(opts, client.WithPriority(*priority))
		}
		if *idempotencyKey != "" {
			opts = append(opts, client.WithIdempotencyKey(*idempotencyKey))
		}
		switch _, err := c.Enqueue(ctx, *queue, []byte(payload), opts...); {
		case errors.Is(err, client.ErrDuplicate):
			logger.Info("duplicate dropped", "i", i, "key", *idempotencyKey)
		case err != nil:
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
	}

	logger.Info("enqueued jobs", "count", *count, "queue", *queue, "server", *server)
}
