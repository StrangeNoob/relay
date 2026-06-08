package metrics

import "github.com/prometheus/client_golang/prometheus"

// CounterForTest returns the per-queue child counter for the named metric so
// tests in the external metrics_test package can read a specific series. It
// lives in an _test.go file so it never ships in the production build. "name" is
// the short key (enqueued, deduped, claimed, processed, retried, dead, reaped,
// promoted); an unknown name returns nil.
func (r *Recorder) CounterForTest(name, queue string) prometheus.Counter {
	var v *prometheus.CounterVec
	switch name {
	case "enqueued":
		v = r.enqueued
	case "deduped":
		v = r.deduped
	case "claimed":
		v = r.claimed
	case "processed":
		v = r.processed
	case "retried":
		v = r.retried
	case "dead":
		v = r.dead
	case "reaped":
		v = r.reaped
	case "promoted":
		v = r.promoted
	default:
		return nil
	}
	return v.WithLabelValues(queue)
}
