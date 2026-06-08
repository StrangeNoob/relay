// Package metrics provides the Prometheus implementation of the broker's
// instrumentation contract (broker.Metrics) plus a pull-based collector for
// per-queue depth gauges. It deliberately does not import internal/broker: it
// satisfies broker.Metrics structurally, keeping the dependency arrow one-way.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "relay"

// Recorder implements broker.Metrics over a private Prometheus registry. Build
// it with NewRecorder, install it with broker.WithMetrics(rec), and serve
// rec.Registry() over HTTP. Counters and the latency histogram are labelled by
// queue.
type Recorder struct {
	reg *prometheus.Registry

	enqueued  *prometheus.CounterVec
	deduped   *prometheus.CounterVec
	claimed   *prometheus.CounterVec
	processed *prometheus.CounterVec
	retried   *prometheus.CounterVec
	dead      *prometheus.CounterVec
	reaped    *prometheus.CounterVec
	promoted  *prometheus.CounterVec
	latency   *prometheus.HistogramVec
}

// NewRecorder builds a Recorder with all metrics registered on a fresh registry.
func NewRecorder() *Recorder {
	reg := prometheus.NewRegistry()

	counter := func(name, help string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: name, Help: help,
		}, []string{"queue"})
		reg.MustRegister(c)
		return c
	}

	r := &Recorder{
		reg:       reg,
		enqueued:  counter("jobs_enqueued_total", "Jobs accepted into a queue."),
		deduped:   counter("jobs_deduplicated_total", "Enqueues dropped as idempotency-key duplicates."),
		claimed:   counter("jobs_claimed_total", "Jobs claimed by a worker."),
		processed: counter("jobs_processed_total", "Jobs acked after successful processing."),
		retried:   counter("jobs_retried_total", "Failed jobs requeued for retry."),
		dead:      counter("jobs_dead_total", "Jobs moved to the dead-letter queue."),
		reaped:    counter("jobs_reaped_total", "Expired in-flight jobs requeued by the reaper."),
		promoted:  counter("jobs_promoted_total", "Delayed jobs promoted to ready."),
	}
	r.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "job_latency_seconds",
		Help:      "End-to-end time from job creation to ack.",
		// End-to-end latency spans ms to minutes, wider than the default
		// 5ms-10s buckets: ~1ms -> ~9min across 13 buckets.
		Buckets: prometheus.ExponentialBuckets(0.001, 3, 13),
	}, []string{"queue"})
	reg.MustRegister(r.latency)
	return r
}

// Registry exposes the underlying registry for an HTTP handler and for
// registering additional collectors (e.g. DepthCollector).
func (r *Recorder) Registry() *prometheus.Registry { return r.reg }

func (r *Recorder) IncEnqueued(q string)     { r.enqueued.WithLabelValues(q).Inc() }
func (r *Recorder) IncDeduplicated(q string) { r.deduped.WithLabelValues(q).Inc() }
func (r *Recorder) IncClaimed(q string)      { r.claimed.WithLabelValues(q).Inc() }
func (r *Recorder) IncProcessed(q string)    { r.processed.WithLabelValues(q).Inc() }
func (r *Recorder) IncRetried(q string)      { r.retried.WithLabelValues(q).Inc() }
func (r *Recorder) IncDead(q string)         { r.dead.WithLabelValues(q).Inc() }
func (r *Recorder) AddReaped(q string, n int) {
	r.reaped.WithLabelValues(q).Add(float64(n))
}
func (r *Recorder) AddPromoted(q string, n int) {
	r.promoted.WithLabelValues(q).Add(float64(n))
}
func (r *Recorder) ObserveLatency(q string, d time.Duration) {
	r.latency.WithLabelValues(q).Observe(d.Seconds())
}
