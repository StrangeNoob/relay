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
		logger:   slog.Default().With("queue", queue),
	}
}

// Run requeues expired jobs every interval until ctx is cancelled, then returns
// nil. Each tick drains all currently-expired jobs before sleeping again (see
// runDrainLoop), so a burst of timeouts is cleared promptly.
func (r *Reaper) Run(ctx context.Context) error {
	return runDrainLoop(ctx, r.interval, r.logger, "reaper", func(c context.Context) (int, error) {
		return r.broker.Reap(c, r.queue)
	})
}
