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
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/job"
)

// Broker talks to a single Redis instance. It is safe for concurrent use: all
// state lives in Redis, and the type itself holds only the client.
type Broker struct {
	rdb *redis.Client
}

// New returns a Broker backed by the given Redis client.
func New(rdb *redis.Client) *Broker {
	return &Broker{rdb: rdb}
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

// Enqueue makes a job available for workers to claim: it persists the job hash
// and adds the id to the queue's ready set. Both writes run in one transaction
// so a crash can never leave a job hash with no ready entry, or vice versa.
//
// The ready-set score is the job's priority; until priorities are wired up every
// job enqueues at score 0.
func (b *Broker) Enqueue(ctx context.Context, j job.Job) error {
	pipe := b.rdb.TxPipeline()
	pipe.HSet(ctx, jobKey(j.ID), j.ToHash())
	pipe.ZAdd(ctx, readyKey(j.Queue), redis.Z{Score: 0, Member: j.ID})
	if _, err := pipe.Exec(ctx); err != nil {
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
// is requeued to ready for another try; once its retry budget is spent it is
// moved to the dead-letter queue. The decision and the moves happen atomically
// in nack.lua.
func (b *Broker) Nack(ctx context.Context, j job.Job) error {
	if err := nackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), readyKey(j.Queue), dlqKey(j.Queue)},
		j.ID, jobKeyPrefix,
	).Err(); err != nil {
		return fmt.Errorf("broker: nacking job %s: %w", j.ID, err)
	}
	return nil
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
