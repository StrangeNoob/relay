// Command worker runs a pool of Relay workers (plus a reaper) against a queue.
//
// It is a thin wiring layer: all behaviour lives in internal/broker and
// internal/worker. The handler here is a stand-in that simulates work and fails
// a configurable fraction of jobs, so a running pool exercises retries, the DLQ,
// and crash recovery against a real Redis.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
	"github.com/StrangeNoob/relay/internal/worker"
)

func main() {
	addr := flag.String("redis", "localhost:6379", "Redis address")
	queue := flag.String("queue", "default", "queue to consume")
	concurrency := flag.Int("concurrency", 4, "number of concurrent workers")
	failRate := flag.Float64("fail-rate", 0.1, "fraction of jobs the demo handler fails (0..1)")
	reapInterval := flag.Duration("reap-interval", 5*time.Second, "how often the reaper scans for expired jobs")
	promoteInterval := flag.Duration("promote-interval", time.Second, "how often the promoter releases due delayed jobs")
	backoffBase := flag.Duration("backoff-base", time.Second, "retry backoff base delay")
	backoffMax := flag.Duration("backoff-max", 10*time.Minute, "retry backoff ceiling")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Cancel the root context on SIGINT/SIGTERM so workers stop claiming and
	// finish their in-flight job before we exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer func() { _ = rdb.Close() }()

	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("cannot reach redis", "addr", *addr, "err", err)
		os.Exit(1)
	}

	b := broker.New(rdb, broker.WithBackoff(*backoffBase, *backoffMax))
	handler := demoHandler(*failRate, logger)

	var wg sync.WaitGroup
	for i := range *concurrency {
		w := worker.New(b, *queue, handler)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				logger.Error("worker exited with error", "worker", id, "err", err)
			}
		}(i)
	}

	r := worker.NewReaper(b, *queue, *reapInterval)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Run(ctx); err != nil {
			logger.Error("reaper exited with error", "err", err)
		}
	}()

	p := worker.NewPromoter(b, *queue, *promoteInterval)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Run(ctx); err != nil {
			logger.Error("promoter exited with error", "err", err)
		}
	}()

	logger.Info("relay worker started", "queue", *queue, "concurrency", *concurrency, "redis", *addr)
	<-ctx.Done()
	logger.Info("shutdown signal received, draining in-flight jobs")
	wg.Wait()
	logger.Info("relay worker stopped cleanly")
}

// demoHandler returns a handler that simulates variable-length work and fails a
// failRate fraction of the time, so a running pool produces retries and DLQ
// traffic worth watching.
func demoHandler(failRate float64, logger *slog.Logger) worker.Handler {
	return func(ctx context.Context, j job.Job) error {
		// Simulate work, but bail out promptly if we are shutting down.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(50+rand.Intn(150)) * time.Millisecond):
		}

		if rand.Float64() < failRate {
			return fmt.Errorf("simulated failure for job %s (attempt %d)", j.ID, j.Attempts)
		}
		logger.Info("processed job", "job", j.ID, "queue", j.Queue, "attempt", j.Attempts, "payload", string(j.Payload))
		return nil
	}
}
