package broker

import (
	"math/rand"
	"time"
)

// backoffCeiling returns base*2^(attempts-1), clamped to maxDelay. It doubles in
// a loop and returns early once another double would reach the cap, so it never
// overflows int64 even for very large maxDelay. attempts < 1 is treated as 1.
func backoffCeiling(attempts int, base, maxDelay time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := base
	for i := 1; i < attempts; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

// nextBackoff returns a full-jitter delay: a uniform random duration in
// [0, backoffCeiling). Full jitter (AWS-style) spreads synchronized retries to
// avoid a thundering herd. Pure given r, so tests can seed r deterministically.
func nextBackoff(attempts int, base, maxDelay time.Duration, r *rand.Rand) time.Duration {
	ceil := backoffCeiling(attempts, base, maxDelay)
	if ceil <= 0 {
		return 0
	}
	return time.Duration(r.Int63n(int64(ceil)))
}
