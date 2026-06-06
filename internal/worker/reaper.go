package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
)

// Reaper periodically requeues jobs whose visibility deadline has passed. It is
// the background companion to the worker: workers deliver, the reaper recovers
// what crashed workers left behind. Run one per queue (or share one across
// queues by running several).
type Reaper struct {
	broker   *broker.Broker
	queue    string
	interval time.Duration
	logger   *slog.Logger
}

// NewReaper builds a reaper that scans queue every interval.
func NewReaper(b *broker.Broker, queue string, interval time.Duration) *Reaper {
	return &Reaper{
		broker:   b,
		queue:    queue,
		interval: interval,
		logger:   slog.Default(),
	}
}

// Run requeues expired jobs every interval until ctx is cancelled, then returns
// nil. Each tick drains all currently-expired jobs before sleeping again, so a
// burst of timeouts is cleared promptly rather than one batch per interval.
func (r *Reaper) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := r.reapAll(ctx); err != nil {
			if ctx.Err() != nil {
				return nil // failure caused by shutdown
			}
			// A transient Redis error should not kill the reaper; log and retry
			// on the next tick.
			r.logger.Error("relay reaper: reap failed", "queue", r.queue, "err", err)
		}
		if !wait(ctx, r.interval) {
			return nil
		}
	}
}

// reapAll calls Reap until a pass requeues nothing, meaning the inflight set has
// no more past-due jobs for now.
func (r *Reaper) reapAll(ctx context.Context) error {
	for {
		n, err := r.broker.Reap(ctx, r.queue)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}
