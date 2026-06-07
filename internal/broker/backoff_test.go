package broker

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoffCeilingGrowsAndCaps(t *testing.T) {
	base := time.Second
	maxDelay := 10 * time.Second
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Second},  // treated as attempt 1
		{1, 1 * time.Second},  // base * 2^0
		{2, 2 * time.Second},  // base * 2^1
		{3, 4 * time.Second},  // base * 2^2
		{4, 8 * time.Second},  // base * 2^3
		{5, 10 * time.Second}, // 16s capped to 10s
		{10, 10 * time.Second},
	}
	for _, c := range cases {
		if got := backoffCeiling(c.attempts, base, maxDelay); got != c.want {
			t.Errorf("backoffCeiling(%d) = %v, want %v", c.attempts, got, c.want)
		}
	}
}

func TestNextBackoffWithinCeiling(t *testing.T) {
	base := time.Second
	maxDelay := 10 * time.Second
	for _, attempts := range []int{1, 2, 3, 5, 10} {
		// Each attempts-group gets its own independently-seeded source so the
		// test is reproducible regardless of iteration counts or ordering.
		r := rand.New(rand.NewSource(int64(attempts)))
		ceil := backoffCeiling(attempts, base, maxDelay)
		for i := 0; i < 100; i++ {
			d := nextBackoff(attempts, base, maxDelay, r)
			if d < 0 || d >= ceil {
				t.Fatalf("attempts=%d: nextBackoff=%v out of [0,%v)", attempts, d, ceil)
			}
		}
	}
}

func TestBackoffCeilingClampsAndZeroBase(t *testing.T) {
	// base larger than the cap is clamped down to the cap.
	if got := backoffCeiling(1, 10*time.Second, time.Second); got != time.Second {
		t.Errorf("backoffCeiling(1, 10s, 1s) = %v, want 1s", got)
	}
	if got := backoffCeiling(3, 10*time.Second, time.Second); got != time.Second {
		t.Errorf("backoffCeiling(3, 10s, 1s) = %v, want 1s", got)
	}
	// a zero base yields a zero ceiling, and nextBackoff must return 0 (no panic).
	r := rand.New(rand.NewSource(1))
	if got := nextBackoff(1, 0, time.Second, r); got != 0 {
		t.Errorf("nextBackoff with zero base = %v, want 0", got)
	}
}
