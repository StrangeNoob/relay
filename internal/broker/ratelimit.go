package broker

import "fmt"

// rateLimit is a per-queue token-bucket configuration: rate tokens accrue per
// second up to a maximum of burst tokens.
type rateLimit struct {
	rate  float64
	burst int
}

// ratelimitKey is the Redis key for a queue's token bucket: `q:{name}:ratelimit`,
// a hash with fields `tokens` (current balance) and `ts` (last-update unix ms).
func ratelimitKey(queue string) string { return "q:" + queue + ":ratelimit" }

// WithRateLimit caps how fast queue can be claimed: at most burst jobs in a
// burst, refilling at rate jobs/second. Queues with no registered limit are
// unthrottled. Panics on a non-positive rate or a burst below 1 — a nonsensical
// limit is a programming error, not something to silently ignore.
//
// All workers on a queue must register the same limit: they share one Redis
// bucket and pass these params on every claim.
func WithRateLimit(queue string, rate float64, burst int) Option {
	if rate <= 0 || burst < 1 {
		panic(fmt.Sprintf("broker: WithRateLimit(%q, %v, %d): rate must be > 0 and burst >= 1", queue, rate, burst))
	}
	return func(b *Broker) {
		if b.rateLimits == nil {
			b.rateLimits = make(map[string]rateLimit)
		}
		b.rateLimits[queue] = rateLimit{rate: rate, burst: burst}
	}
}
