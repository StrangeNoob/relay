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

// distinctMetrics is a sentinel recorder with its own identity, so the test can
// prove WithMetrics installed exactly this value (noopMetrics{} has no identity).
type distinctMetrics struct{ noopMetrics }

func TestWithMetricsInstallsRecorder(t *testing.T) {
	rec := &distinctMetrics{}
	b := New(nil, WithMetrics(rec))
	if b.metrics != rec {
		t.Fatalf("WithMetrics: metrics = %v, want the installed recorder", b.metrics)
	}
}

func TestWithMetricsNilIsIgnored(t *testing.T) {
	b := New(nil, WithMetrics(nil))
	if _, ok := b.metrics.(noopMetrics); !ok {
		t.Fatalf("WithMetrics(nil): metrics = %T, want noopMetrics retained", b.metrics)
	}
}
