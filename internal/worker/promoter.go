package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
)

// Promoter periodically releases delayed jobs whose ready-at time has arrived.
// It is the scheduling counterpart to the reaper: the reaper recovers timed-out
// in-flight work, the promoter releases scheduled and backed-off work. Run one
// per queue.
type Promoter struct {
	broker   *broker.Broker
	queue    string
	interval time.Duration
	logger   *slog.Logger
}

// NewPromoter builds a promoter that scans queue every interval.
func NewPromoter(b *broker.Broker, queue string, interval time.Duration) *Promoter {
	return &Promoter{
		broker:   b,
		queue:    queue,
		interval: interval,
		logger:   slog.Default().With("queue", queue),
	}
}

// Run promotes due delayed jobs every interval until ctx is cancelled, then
// returns nil. Each tick drains all currently-due jobs before sleeping again.
func (p *Promoter) Run(ctx context.Context) error {
	return runDrainLoop(ctx, p.interval, p.logger, "promoter", func(c context.Context) (int, error) {
		return p.broker.Promote(c, p.queue)
	})
}
