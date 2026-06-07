// Package broker is Relay's core engine. It implements queue semantics —
// enqueue, atomic claim, ack, nack, reaper, promoter — on top of plain Redis
// primitives. Redis is only the durable substrate; every guarantee (at-least-
// once delivery, visibility timeouts, competing-consumer safety) is enforced by
// the logic in this package and its embedded Lua scripts.
package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/job"
)

// Broker talks to a single Redis instance. It is safe for concurrent use: all
// queue state lives in Redis, and the only mutable in-process state is the
// jitter source, which is mutex-guarded.
type Broker struct {
	rdb         *redis.Client
	backoffBase time.Duration
	backoffMax  time.Duration
	dedupTTL    time.Duration

	rndMu sync.Mutex
	rnd   *rand.Rand
}

// Option customises a Broker at construction.
type Option func(*Broker)

// WithBackoff sets the retry backoff base and ceiling. The nth retry waits a
// full-jitter delay in [0, min(maxDelay, base*2^(n-1))); if base > maxDelay the
// ceiling is always maxDelay.
func WithBackoff(base, maxDelay time.Duration) Option {
	return func(b *Broker) {
		b.backoffBase = base
		b.backoffMax = maxDelay
	}
}

// New returns a Broker backed by the given Redis client. Defaults: backoff base
// 1s, ceiling 10m.
func New(rdb *redis.Client, opts ...Option) *Broker {
	b := &Broker{
		rdb:         rdb,
		backoffBase: time.Second,
		backoffMax:  10 * time.Minute,
		dedupTTL:    24 * time.Hour,
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// jobKeyPrefix namespaces every job hash. It is also handed to the claim script
// so the Lua side reconstructs the same `job:{id}` key Go writes.
const jobKeyPrefix = "job:"

// jobKey is the Redis key for a job's hash: `job:{id}`.
func jobKey(id string) string { return jobKeyPrefix + id }

// readyKey is the Redis key for a queue's ready set: `q:{name}:ready`, a ZSET
// scored by priority so a claim can pop the most important job.
func readyKey(queue string) string { return "q:" + queue + ":ready" }

// inflightKey is the Redis key for a queue's inflight set: `q:{name}:inflight`,
// a ZSET scored by each claimed job's visibility deadline. The reaper scans it.
func inflightKey(queue string) string { return "q:" + queue + ":inflight" }

// dlqKey is the Redis key for a queue's dead-letter list: `q:{name}:dlq`, where
// jobs land once they exhaust their retry budget.
func dlqKey(queue string) string { return "q:" + queue + ":dlq" }

// delayedKey is the Redis key for a queue's delayed set: `q:{name}:delayed`, a
// ZSET scored by each job's ready-at time. The promoter scans it.
func delayedKey(queue string) string { return "q:" + queue + ":delayed" }

// enqueueConfig holds resolved enqueue options. A zero readyAt means "now".
type enqueueConfig struct {
	readyAt     time.Time
	priority    int
	prioritySet bool
}

// EnqueueOption customises a single Enqueue call.
type EnqueueOption func(*enqueueConfig)

// WithDelay schedules the job to become claimable after d from now. A d <= 0 is
// equivalent to a plain enqueue.
func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		if d > 0 {
			c.readyAt = time.Now().Add(d)
		}
	}
}

// WithReadyAt schedules the job to become claimable at t. A zero t, or a t at or
// before now, is equivalent to a plain enqueue.
func WithReadyAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) {
		if !t.IsZero() {
			c.readyAt = t
		}
	}
}

// WithPriority sets the job's claim priority for this enqueue (higher is more
// urgent), overriding any value already on the job. It is clamped to
// [0, MaxPriority].
func WithPriority(p int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.priority = p
		c.prioritySet = true
	}
}

// Enqueue makes a job available for workers to claim. With no option it goes
// straight to the ready set; with a future ready-at it goes to the delayed set
// for the promoter to release later. The job hash and the set membership are
// written in one atomic Lua script so a crash can never leave one without the
// other, and the dedup gate added in the next task can slot in before the write
// without an extra round-trip.
func (b *Broker) Enqueue(ctx context.Context, j job.Job, opts ...EnqueueOption) error {
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.prioritySet {
		j.Priority = cfg.priority
	}
	j.Priority = clampPriority(j.Priority)

	now := time.Now()
	var targetKey string
	var score float64
	if cfg.readyAt.After(now) {
		j.State = job.StateDelayed
		targetKey = delayedKey(j.Queue)
		score = float64(cfg.readyAt.UnixMilli())
	} else {
		j.State = job.StateReady
		targetKey = readyKey(j.Queue)
		score = readyScore(j.Priority, now.UnixMilli())
	}

	h := j.ToHash()
	args := make([]any, 0, 2+2*len(h))
	args = append(args, j.ID, score)
	for k, v := range h {
		args = append(args, k, v)
	}

	if err := enqueueScript.Run(ctx, b.rdb, []string{jobKey(j.ID), targetKey}, args...).Err(); err != nil {
		return fmt.Errorf("broker: enqueuing job %s: %w", j.ID, err)
	}
	return nil
}

// Claim atomically takes the highest-priority ready job from the queue, moves it
// into the inflight set under a deadline of now+visibility, bumps its attempt
// count, and returns it. The ok return is false (with a nil error) when the
// queue has nothing ready.
//
// All of the state change happens inside one Lua script (claim.lua); see that
// file for why the atomicity is non-negotiable. `now` is computed here and
// passed in so the script stays deterministic and tests are not at the mercy of
// the Redis server clock.
func (b *Broker) Claim(ctx context.Context, queue string, visibility time.Duration) (job.Job, bool, error) {
	res, err := claimScript.Run(ctx, b.rdb,
		[]string{readyKey(queue), inflightKey(queue)},
		time.Now().UnixMilli(), visibility.Milliseconds(), jobKeyPrefix,
	).Result()
	if errors.Is(err, redis.Nil) {
		return job.Job{}, false, nil // nothing ready to claim
	}
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}

	h, err := hashFromLua(res)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}
	j, err := job.FromHash(h)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("broker: claiming from %q: %w", queue, err)
	}
	return j, true, nil
}

// Ack acknowledges that a claimed job finished successfully: it is removed from
// the inflight set and its hash is deleted. Like the other transitions it runs
// as one Lua script (ack.lua).
func (b *Broker) Ack(ctx context.Context, j job.Job) error {
	if err := ackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue)},
		j.ID, jobKeyPrefix,
	).Err(); err != nil {
		return fmt.Errorf("broker: acking job %s: %w", j.ID, err)
	}
	return nil
}

// Nack reports that a claimed job failed. If the job still has attempts left it
// is requeued to the delayed set with a full-jitter backoff so the retry waits;
// once its retry budget is spent it is moved to the dead-letter queue. The
// decision and the move are atomic in nack.lua; the backoff delay is computed
// here (jitter needs randomness) and passed in as a ready-at timestamp.
func (b *Broker) Nack(ctx context.Context, j job.Job) error {
	b.rndMu.Lock()
	delay := nextBackoff(j.Attempts, b.backoffBase, b.backoffMax, b.rnd)
	b.rndMu.Unlock()
	readyAt := time.Now().Add(delay).UnixMilli()

	if err := nackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), delayedKey(j.Queue), dlqKey(j.Queue)},
		j.ID, jobKeyPrefix, readyAt,
	).Err(); err != nil {
		return fmt.Errorf("broker: nacking job %s: %w", j.ID, err)
	}
	return nil
}

// defaultReapBatch bounds how many jobs a single Reap pass requeues, so one
// call can never block Redis scanning a huge inflight backlog. Callers run Reap
// in a loop until it returns 0 to drain everything.
const defaultReapBatch = 100

// defaultPromoteBatch bounds how many jobs a single Promote pass releases, so
// one call cannot block Redis scanning a huge delayed backlog. Callers loop
// until it returns 0 to drain everything due.
const defaultPromoteBatch = 100

// Reap requeues jobs whose visibility deadline has passed: workers that claimed
// them have crashed or stalled without acking. Each expired job is moved from
// inflight back to ready (atomically, in reaper.lua), which is what makes
// at-least-once delivery survive a worker crash. It returns the number of jobs
// requeued in this pass; a return of defaultReapBatch means more may remain, so
// call again.
func (b *Broker) Reap(ctx context.Context, queue string) (int, error) {
	n, err := reaperScript.Run(ctx, b.rdb,
		[]string{inflightKey(queue), readyKey(queue)},
		time.Now().UnixMilli(), jobKeyPrefix, defaultReapBatch, priorityScale,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("broker: reaping %q: %w", queue, err)
	}
	return n, nil
}

// Promote releases delayed jobs whose ready-at time has passed, moving them from
// the delayed set back to ready (atomically, in promote.lua). It returns the
// number promoted in this pass; a return of defaultPromoteBatch means more may
// remain, so call again.
func (b *Broker) Promote(ctx context.Context, queue string) (int, error) {
	n, err := promoteScript.Run(ctx, b.rdb,
		[]string{delayedKey(queue), readyKey(queue)},
		time.Now().UnixMilli(), jobKeyPrefix, defaultPromoteBatch, priorityScale,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("broker: promoting %q: %w", queue, err)
	}
	return n, nil
}

// Extend pushes a claimed job's visibility deadline to now+visibility, the
// heartbeat a worker sends while a long handler is still running so the reaper
// does not reclaim it. It returns false (without error) when the job is no
// longer in flight — already acked or already reaped — and in that case must not
// re-add it to the inflight set. The check and the re-score are atomic in
// heartbeat.lua.
func (b *Broker) Extend(ctx context.Context, j job.Job, visibility time.Duration) (bool, error) {
	n, err := heartbeatScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue)},
		j.ID, time.Now().UnixMilli(), visibility.Milliseconds(),
	).Int()
	if err != nil {
		return false, fmt.Errorf("broker: extending job %s: %w", j.ID, err)
	}
	return n == 1, nil
}

// hashFromLua converts the flat HGETALL array a script returns (alternating
// field, value, field, value …) into a Go map. go-redis decodes a Lua table as
// []interface{} of strings.
func hashFromLua(v any) (map[string]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected claim reply type %T", v)
	}
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("claim reply has odd field count %d", len(arr))
	}
	h := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		field, fok := arr[i].(string)
		value, vok := arr[i+1].(string)
		if !fok || !vok {
			return nil, fmt.Errorf("non-string field in claim reply at index %d", i)
		}
		h[field] = value
	}
	return h, nil
}
