// Package worker is the consumer runtime: it repeatedly claims jobs from a queue
// and runs a user-supplied handler against each, acking on success and nacking
// on failure. It owns the loop and lifecycle; all queue semantics live in the
// broker.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

// Handler processes a single claimed job. Returning nil acks the job; returning
// an error nacks it (the broker decides whether to retry or dead-letter).
type Handler func(ctx context.Context, j job.Job) error

const (
	// defaultVisibility is how long a claimed job stays invisible to other
	// workers before the reaper may reclaim it. It should comfortably exceed a
	// typical handler runtime.
	defaultVisibility = 30 * time.Second
	// defaultPollInterval is how long the loop waits before retrying when the
	// queue is empty, so an idle worker does not spin on Redis.
	defaultPollInterval = 100 * time.Millisecond
)

// Worker claims from a single queue and dispatches to one handler.
type Worker struct {
	broker            *broker.Broker
	queue             string
	handler           Handler
	visibility        time.Duration
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	logger            *slog.Logger
}

// Option customises a Worker at construction.
type Option func(*Worker)

// WithVisibility sets how long a claimed job stays invisible to other workers.
func WithVisibility(d time.Duration) Option {
	return func(w *Worker) { w.visibility = d }
}

// WithHeartbeatInterval sets how often a running handler's deadline is extended.
// It should be comfortably smaller than the visibility timeout. When unset it
// defaults to a third of the visibility timeout.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(w *Worker) { w.heartbeatInterval = d }
}

// WithPollInterval sets how long the loop waits before retrying an empty queue.
func WithPollInterval(d time.Duration) Option {
	return func(w *Worker) { w.pollInterval = d }
}

// WithLogger sets the logger used for non-fatal errors.
func WithLogger(l *slog.Logger) Option {
	return func(w *Worker) { w.logger = l }
}

// New builds a worker for the given queue and handler. Defaults are sane; pass
// options to tune timings or the logger.
func New(b *broker.Broker, queue string, handler Handler, opts ...Option) *Worker {
	w := &Worker{
		broker:       b,
		queue:        queue,
		handler:      handler,
		visibility:   defaultVisibility,
		pollInterval: defaultPollInterval,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(w)
	}
	// Heartbeat thrice per visibility window unless the caller chose otherwise,
	// so a deadline is refreshed well before it can expire.
	if w.heartbeatInterval <= 0 {
		w.heartbeatInterval = w.visibility / 3
	}
	return w
}

// Run claims and processes jobs until ctx is cancelled, then returns nil. On
// cancellation it stops claiming new work; a job already in hand is finished
// (acked/nacked) first — see process — so shutdown never strands in-flight work.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil // shutdown requested; stop claiming
		}

		j, ok, err := w.broker.Claim(ctx, w.queue, w.visibility)
		if err != nil {
			if ctx.Err() != nil {
				return nil // claim failed because we are shutting down
			}
			w.logger.Error("relay worker: claim failed", "queue", w.queue, "err", err)
			if !wait(ctx, w.pollInterval) {
				return nil
			}
			continue
		}
		if !ok {
			if !wait(ctx, w.pollInterval) {
				return nil // queue empty and shutdown requested
			}
			continue
		}

		w.process(ctx, j)
	}
}

// process runs the handler for one job and acks it on success. The ack uses a
// context detached from cancellation (context.WithoutCancel) so that a shutdown
// signalled mid-handler still lets the finished job be acknowledged rather than
// left dangling in the inflight set.
func (w *Worker) process(ctx context.Context, j job.Job) {
	// Detach the finish from cancellation so a job already in hand is always
	// acked or nacked, even when shutdown was signalled mid-handler.
	finishCtx := context.WithoutCancel(ctx)

	// Keep the job's visibility deadline ahead of the reaper for as long as the
	// handler runs. Heartbeats also use the detached context so a graceful
	// shutdown does not let the in-flight job be reclaimed before it finishes.
	stopHeartbeat := w.startHeartbeat(finishCtx, j)
	defer stopHeartbeat()

	if herr := w.handler(ctx, j); herr != nil {
		w.logger.Warn("relay worker: handler failed, nacking", "job", j.ID, "err", herr)
		if err := w.broker.Nack(finishCtx, j); err != nil {
			w.logger.Error("relay worker: nack failed", "job", j.ID, "err", err)
		}
		return
	}
	if err := w.broker.Ack(finishCtx, j); err != nil {
		w.logger.Error("relay worker: ack failed", "job", j.ID, "err", err)
	}
}

// startHeartbeat launches a goroutine that extends j's visibility deadline every
// heartbeatInterval until the returned stop function is called. The stop
// function waits for the goroutine to exit, so no Extend can run after process
// returns. The heartbeat also stops on its own if a beat reports the job is no
// longer in flight (already acked or reaped).
func (w *Worker) startHeartbeat(ctx context.Context, j job.Job) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-t.C:
				ok, err := w.broker.Extend(ctx, j, w.visibility)
				if err != nil {
					w.logger.Error("relay worker: heartbeat failed", "job", j.ID, "err", err)
					return
				}
				if !ok {
					return // job no longer in flight; nothing to extend
				}
			}
		}
	}()
	return func() {
		close(stopped)
		<-done
	}
}

// wait blocks for d or until ctx is cancelled. It reports true if the full
// duration elapsed, false if ctx was cancelled first.
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
