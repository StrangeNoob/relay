package broker

import "testing"

func TestNewDefaultsToNoopMetrics(t *testing.T) {
	b := New(nil)
	if b.metrics == nil {
		t.Fatal("New: metrics field is nil, want noopMetrics default")
	}
	if _, ok := b.metrics.(noopMetrics); !ok {
		t.Fatalf("New: metrics = %T, want noopMetrics", b.metrics)
	}
}

func TestWithMetricsInstallsRecorder(t *testing.T) {
	rec := noopMetrics{} // any Metrics value; identity is what we check
	var custom Metrics = rec
	b := New(nil, WithMetrics(custom))
	if b.metrics == nil {
		t.Fatal("WithMetrics: metrics is nil")
	}
}

func TestWithMetricsNilIsIgnored(t *testing.T) {
	b := New(nil, WithMetrics(nil))
	if _, ok := b.metrics.(noopMetrics); !ok {
		t.Fatalf("WithMetrics(nil): metrics = %T, want noopMetrics retained", b.metrics)
	}
}
