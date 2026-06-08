package broker

import "time"

// Metrics receives a callback for every job-state transition the broker makes.
// It is the broker's consumer-side instrumentation contract: the broker depends
// on this small interface, not on any metrics library, so a Prometheus recorder
// (internal/metrics) — or a test fake — can be plugged in via WithMetrics. The
// default is noopMetrics, so a broker built without WithMetrics records nothing
// and behaves exactly as before.
//
// Every method takes the queue name so the implementation can label its series
// per queue. Reap/Promote add a batch count because one call moves many jobs;
// the rest are single events. ObserveLatency reports a job's end-to-end time in
// the system (enqueue -> ack).
type Metrics interface {
	IncEnqueued(queue string)
	IncDeduplicated(queue string)
	IncClaimed(queue string)
	IncProcessed(queue string)
	IncRetried(queue string)
	IncDead(queue string)
	AddReaped(queue string, n int)
	AddPromoted(queue string, n int)
	ObserveLatency(queue string, d time.Duration)
}

// noopMetrics is the default recorder: every method does nothing. It lets the
// broker call b.metrics unconditionally without nil checks, and keeps metrics
// entirely opt-in.
type noopMetrics struct{}

func (noopMetrics) IncEnqueued(string)                   {}
func (noopMetrics) IncDeduplicated(string)               {}
func (noopMetrics) IncClaimed(string)                    {}
func (noopMetrics) IncProcessed(string)                  {}
func (noopMetrics) IncRetried(string)                    {}
func (noopMetrics) IncDead(string)                       {}
func (noopMetrics) AddReaped(string, int)                {}
func (noopMetrics) AddPromoted(string, int)              {}
func (noopMetrics) ObserveLatency(string, time.Duration) {}

// WithMetrics installs a Metrics recorder. A nil recorder is ignored so callers
// cannot accidentally replace the safe no-op with something that panics.
func WithMetrics(m Metrics) Option {
	return func(b *Broker) {
		if m != nil {
			b.metrics = m
		}
	}
}
