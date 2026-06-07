package broker

import (
	"errors"
	"time"
)

// ErrDuplicate is returned by Enqueue when a job carries an idempotency key that
// was already enqueued within the dedup TTL window. Nothing is written; the
// logical job is already in the system, so callers can treat this as a benign
// no-op via errors.Is(err, ErrDuplicate).
var ErrDuplicate = errors.New("broker: duplicate enqueue")

// dedupKey is the Redis key holding the idempotency marker for one key on one
// queue: `q:{name}:dedup:{key}`. It is a string with an EX TTL — Redis sets lack
// per-member expiry, so each marker is its own key and expires independently.
// The marker's value is the winning job's id — useful for observability (which
// job owns the slot); the broker does not read it back.
func dedupKey(queue, key string) string {
	return "q:" + queue + ":dedup:" + key
}

// WithDedupTTL sets how long an idempotency marker is remembered. Within this
// window a second enqueue with the same key is dropped; afterwards the key is
// free again. Default 24h.
func WithDedupTTL(d time.Duration) Option {
	return func(b *Broker) { b.dedupTTL = d }
}
