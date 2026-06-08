package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/metrics"
)

// Compile-time proof the Recorder satisfies the broker's instrumentation contract.
var _ broker.Metrics = (*metrics.Recorder)(nil)

func TestRecorderCountersIncrement(t *testing.T) {
	rec := metrics.NewRecorder()

	rec.IncEnqueued("emails")
	rec.IncEnqueued("emails")
	rec.IncDeduplicated("emails")
	rec.IncClaimed("emails")
	rec.IncProcessed("emails")
	rec.IncRetried("emails")
	rec.IncDead("emails")
	rec.AddReaped("emails", 3)
	rec.AddPromoted("emails", 4)

	checks := []struct {
		name   string
		metric string
		want   float64
	}{
		{"enqueued", "relay_jobs_enqueued_total", 2},
		{"deduped", "relay_jobs_deduplicated_total", 1},
		{"claimed", "relay_jobs_claimed_total", 1},
		{"processed", "relay_jobs_processed_total", 1},
		{"retried", "relay_jobs_retried_total", 1},
		{"dead", "relay_jobs_dead_total", 1},
		{"reaped", "relay_jobs_reaped_total", 3},
		{"promoted", "relay_jobs_promoted_total", 4},
	}
	for _, c := range checks {
		if got := testutil.ToFloat64(rec.CounterForTest(c.name, "emails")); got != c.want {
			t.Errorf("%s{queue=emails} = %v, want %v", c.metric, got, c.want)
		}
	}
}

func TestRecorderObservesLatency(t *testing.T) {
	rec := metrics.NewRecorder()
	rec.ObserveLatency("emails", 250*time.Millisecond)

	got := testutil.CollectAndCount(rec.Registry(), "relay_job_latency_seconds")
	if got == 0 {
		t.Fatal("relay_job_latency_seconds: no series collected after one observation")
	}
}
