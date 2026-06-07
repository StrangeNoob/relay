// Package job defines Relay's unit of work and how it is encoded for storage.
//
// A Job is a plain value: the broker owns all queue semantics, so this package
// stays free of Redis or any I/O. Keeping it pure makes it trivially testable
// and reusable by every other package (broker, worker, client, api).
package job

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// State is where a job sits in its lifecycle. It is partly redundant with which
// Redis structure currently holds the job (ready/delayed/inflight ZSETs), but
// storing it on the job too makes a job self-describing for inspection and the
// dashboard. More states (ready, inflight, delayed, dead) are added as the
// broker grows to drive those transitions.
type State string

const (
	// StatePending is a job that has been constructed but not yet enqueued.
	StatePending State = "pending"
	// StateInFlight is a job that has been claimed by a worker and is being
	// processed; it sits in the inflight set under a visibility deadline.
	StateInFlight State = "inflight"
	// StateReady is a job waiting in a ready set to be claimed — either freshly
	// enqueued or requeued after a failed attempt.
	StateReady State = "ready"
	// StateDead is a job that exhausted its retry budget and was moved to the
	// dead-letter queue for inspection.
	StateDead State = "dead"
	// StateDelayed is a job waiting in the delayed set for its ready-at time —
	// either scheduled with a delay or waiting out a backoff after a failure.
	StateDelayed State = "delayed"
)

// DefaultMaxRetries is the retry budget a job gets when New is not told
// otherwise: after this many failed attempts the broker sends it to the DLQ.
// Chosen as a small, sane default for a demo queue; the client SDK will later
// allow overriding it per job.
const DefaultMaxRetries = 5

// Job is a single unit of work. The broker persists it as a Redis hash
// (key `job:{id}`); the fields here are the source of truth for that encoding.
type Job struct {
	ID         string    // unique, generated at construction
	Queue      string    // logical queue name this job belongs to
	Payload    []byte    // opaque application data; Relay never interprets it
	State      State     // current lifecycle position
	Attempts   int       // number of delivery attempts so far (bumped on claim)
	MaxRetries int       // attempts allowed before the job is dead-lettered
	CreatedAt  time.Time // wall-clock time the job was constructed

	// IdempotencyKey, when set by the producer, lets the broker drop duplicate
	// enqueues and lets consumers dedup redelivered work. Empty means "no key";
	// it is the mechanism that makes at-least-once delivery safe to build on.
	IdempotencyKey string
}

// New builds a job for the given queue and payload, assigning it a fresh ID and
// creation timestamp. The job is not enqueued — that is the client/broker's job.
func New(queue string, payload []byte) Job {
	return Job{
		ID:         newID(),
		Queue:      queue,
		Payload:    payload,
		State:      StatePending,
		Attempts:   0,
		MaxRetries: DefaultMaxRetries,
		CreatedAt:  time.Now(),
	}
}

// Hash field names. Defined as constants so the encode and decode sides cannot
// drift apart, and so the broker's Lua scripts have a single reference for the
// field layout of a `job:{id}` hash.
const (
	fieldID          = "id"
	fieldQueue       = "queue"
	fieldPayload     = "payload"
	fieldState       = "state"
	fieldAttempts    = "attempts"
	fieldMaxRetries  = "max_retries"
	fieldCreatedAt   = "created_at"
	fieldIdempotency = "idempotency_key"
)

// ToHash renders the job as Redis hash fields (HSET-style flat string map).
// time and integer values are stringified here and parsed back in FromHash;
// the two must stay symmetric.
func (j Job) ToHash() map[string]string {
	return map[string]string{
		fieldID:          j.ID,
		fieldQueue:       j.Queue,
		fieldPayload:     string(j.Payload),
		fieldState:       string(j.State),
		fieldAttempts:    strconv.Itoa(j.Attempts),
		fieldMaxRetries:  strconv.Itoa(j.MaxRetries),
		fieldCreatedAt:   j.CreatedAt.Format(time.RFC3339Nano),
		fieldIdempotency: j.IdempotencyKey,
	}
}

// FromHash reconstructs a Job from the hash produced by ToHash. It returns an
// error (wrapped with %w) if a numeric or time field is malformed, rather than
// silently zeroing it — a corrupt job in Redis is a bug we want surfaced.
func FromHash(h map[string]string) (Job, error) {
	attempts, err := strconv.Atoi(h[fieldAttempts])
	if err != nil {
		return Job{}, fmt.Errorf("job: parsing %s %q: %w", fieldAttempts, h[fieldAttempts], err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, h[fieldCreatedAt])
	if err != nil {
		return Job{}, fmt.Errorf("job: parsing %s %q: %w", fieldCreatedAt, h[fieldCreatedAt], err)
	}
	maxRetries, err := strconv.Atoi(h[fieldMaxRetries])
	if err != nil {
		return Job{}, fmt.Errorf("job: parsing %s %q: %w", fieldMaxRetries, h[fieldMaxRetries], err)
	}
	return Job{
		ID:             h[fieldID],
		Queue:          h[fieldQueue],
		Payload:        []byte(h[fieldPayload]),
		State:          State(h[fieldState]),
		Attempts:       attempts,
		MaxRetries:     maxRetries,
		CreatedAt:      createdAt,
		IdempotencyKey: h[fieldIdempotency],
	}, nil
}

// newID returns a random 128-bit identifier as a 32-char hex string. We use
// crypto/rand rather than a UUID dependency: it is collision-safe in practice
// and keeps the project dependency-free, in line with the "build it ourselves"
// goal. A failure from crypto/rand is treated as fatal via panic — on a healthy
// machine it cannot fail, and a queue with no source of randomness is unusable.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("job: cannot read random bytes for id: " + err.Error())
	}
	return hex.EncodeToString(b)
}
