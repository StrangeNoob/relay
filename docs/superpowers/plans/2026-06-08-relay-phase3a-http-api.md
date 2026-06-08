# Phase 3a HTTP API + Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Relay a JSON HTTP API (`internal/api`) and an always-on server (`cmd/server`) for enqueueing jobs, reading per-queue stats, inspecting the DLQ, and requeueing dead-lettered jobs — plus the broker methods those need.

**Architecture:** `internal/api` is a thin stdlib `net/http` handler over a `*broker.Broker` (parse → call broker → encode JSON/status). `internal/broker` gains read/admin methods (`Stats`, `ListDLQ`, `RequeueDLQ` + atomic `requeue.lua`, `Queues`). `cmd/server` wires Redis + broker (with a Phase 2 metrics `Recorder` so API enqueues are counted) + the API handler + `/metrics` + `/healthz`, with graceful shutdown.

**Tech Stack:** Go, stdlib `net/http` (1.22 `ServeMux` pattern routing), `github.com/redis/go-redis/v9`, `github.com/prometheus/client_golang` (already a dep), real-Redis integration tests.

**Spec:** [`docs/superpowers/specs/2026-06-08-relay-phase3a-http-api-design.md`](../specs/2026-06-08-relay-phase3a-http-api-design.md)

---

## File Structure

- **Modify `internal/broker/broker.go`** — add `Stats`, `ListDLQ`, `RequeueDLQ`, `Queues` methods + the `Stats` struct + DLQ limit constants.
- **Create `internal/broker/scripts/requeue.lua`** — atomic dlq→ready move.
- **Modify `internal/broker/scripts.go`** — embed + register `requeueScript`.
- **Modify `internal/broker/broker_test.go`** — tests for the four new methods (DB 15).
- **Create `internal/api/api.go`** — the `http.Handler`: router, JSON helpers, `jobView`, all five handlers.
- **Create `internal/api/api_test.go`** — `httptest` end-to-end tests against a real broker (DB 12).
- **Create `cmd/server/main.go`** — server wiring.
- **Modify `CLAUDE.md`** — document the API/server, the new Lua script, the new broker methods, and Phase 3a status.

---

## Task 1: Broker `Stats`

**Files:**
- Modify: `internal/broker/broker.go`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/broker_test.go`:

```go
func TestStatsCountsEachState(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	// 3 ready
	for i := 0; i < 3; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("r"))); err != nil {
			t.Fatalf("Enqueue ready: %v", err)
		}
	}
	// 1 delayed (far future so it stays in delayed)
	if err := b.Enqueue(ctx, job.New("emails", []byte("d")), broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}
	// claim 2 -> inflight
	for i := 0; i < 2; i++ {
		if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
			t.Fatalf("Claim: ok=%v err=%v", ok, err)
		}
	}
	// 1 dead-lettered: enqueue with no retry budget, claim, nack -> dlq
	jd := job.New("emails", []byte("x"))
	jd.MaxRetries = 0
	if err := b.Enqueue(ctx, jd); err != nil {
		t.Fatalf("Enqueue dead: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim dead: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	s, err := b.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// After the above: 3 enqueued ready, then 2 of the ready+dead pool were claimed.
	// Careful accounting: 3 ready + 1 dead-candidate = 4 ready before claims; 2 claimed
	// to inflight leaves 2 ready; the dead-candidate may or may not be among the claimed.
	// To keep the assertion deterministic, assert the totals that are unambiguous:
	if s.Delayed != 1 {
		t.Errorf("Delayed = %d, want 1", s.Delayed)
	}
	if s.DLQ != 1 {
		t.Errorf("DLQ = %d, want 1", s.DLQ)
	}
	if s.Ready+s.Inflight < 0 { // structural sanity; refined below
		t.Errorf("unexpected counts: %+v", s)
	}
}
```

NOTE on determinism: the mixed claim ordering above makes Ready/Inflight hard to assert exactly. Replace the final block with a deterministic scenario instead — rewrite the test body so each state is isolated:

```go
func TestStatsCountsEachState(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	// 2 ready
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, job.New("emails", []byte("r"))); err != nil {
			t.Fatalf("Enqueue ready: %v", err)
		}
	}
	// 1 delayed
	if err := b.Enqueue(ctx, job.New("emails", []byte("d")), broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}
	// 1 inflight: enqueue then claim it (claims highest-priority ready; all equal here)
	if err := b.Enqueue(ctx, job.New("emails", []byte("i"))); err != nil {
		t.Fatalf("Enqueue inflight: %v", err)
	}
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	// 1 dlq: push an id directly so the count is unambiguous
	if err := rdb.RPush(ctx, "q:emails:dlq", "deadid").Err(); err != nil {
		t.Fatalf("seed dlq: %v", err)
	}

	s, err := b.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 2 {
		t.Errorf("Ready = %d, want 2", s.Ready)
	}
	if s.Inflight != 1 {
		t.Errorf("Inflight = %d, want 1", s.Inflight)
	}
	if s.Delayed != 1 {
		t.Errorf("Delayed = %d, want 1", s.Delayed)
	}
	if s.DLQ != 1 {
		t.Errorf("DLQ = %d, want 1", s.DLQ)
	}
}
```

Use the second (deterministic) version. (3 ready enqueued, 1 claimed → 2 ready + 1 inflight.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestStatsCountsEachState -v`
Expected: FAIL — `b.Stats` undefined (compile error).

- [ ] **Step 3: Implement `Stats`**

In `internal/broker/broker.go`, add (near the other methods):

```go
// Stats is a point-in-time count of a queue's jobs by state. Each field is the
// cardinality of the corresponding Redis structure for the queue.
type Stats struct {
	Ready    int64 `json:"ready"`
	Inflight int64 `json:"inflight"`
	Delayed  int64 `json:"delayed"`
	DLQ      int64 `json:"dlq"`
}

// Stats returns the current depth of each of a queue's states in one round trip.
// ready/inflight/delayed are ZSETs (ZCARD); the dlq is a list (LLEN).
func (b *Broker) Stats(ctx context.Context, queue string) (Stats, error) {
	pipe := b.rdb.Pipeline()
	ready := pipe.ZCard(ctx, readyKey(queue))
	inflight := pipe.ZCard(ctx, inflightKey(queue))
	delayed := pipe.ZCard(ctx, delayedKey(queue))
	dlq := pipe.LLen(ctx, dlqKey(queue))
	if _, err := pipe.Exec(ctx); err != nil {
		return Stats{}, fmt.Errorf("broker: stats for %q: %w", queue, err)
	}
	return Stats{
		Ready:    ready.Val(),
		Inflight: inflight.Val(),
		Delayed:  delayed.Val(),
		DLQ:      dlq.Val(),
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestStatsCountsEachState -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker Stats: per-queue depth by state"
```

---

## Task 2: Broker `ListDLQ`

**Files:**
- Modify: `internal/broker/broker.go`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/broker_test.go`:

```go
// deadLetter enqueues a job with no retry budget, claims it, and nacks it so it
// lands in the DLQ; it returns the dead-lettered job's id.
func deadLetter(t *testing.T, b *broker.Broker, ctx context.Context, queue, payload string) string {
	t.Helper()
	j := job.New(queue, []byte(payload))
	j.MaxRetries = 0
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, queue, time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	return claimed.ID
}

func TestListDLQReturnsDeadJobs(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	id1 := deadLetter(t, b, ctx, "emails", "a")
	id2 := deadLetter(t, b, ctx, "emails", "b")

	jobs, err := b.ListDLQ(ctx, "emails", 0, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("len = %d, want 2", len(jobs))
	}
	// DLQ is a list pushed with RPUSH, so order is insertion order.
	if jobs[0].ID != id1 || jobs[1].ID != id2 {
		t.Errorf("ids = %s,%s want %s,%s", jobs[0].ID, jobs[1].ID, id1, id2)
	}
	if jobs[0].State != job.StateDead {
		t.Errorf("state = %q, want dead", jobs[0].State)
	}
}

func TestListDLQPaginates(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		deadLetter(t, b, ctx, "emails", "x")
	}
	page, err := b.ListDLQ(ctx, "emails", 2, 1) // limit 2, offset 1 -> items 2 and 3
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(page) != 2 {
		t.Errorf("len = %d, want 2", len(page))
	}
}

func TestListDLQEmpty(t *testing.T) {
	b, _ := newTestBroker(t)
	jobs, err := b.ListDLQ(context.Background(), "emails", 0, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0", len(jobs))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestListDLQ -v`
Expected: FAIL — `b.ListDLQ` undefined.

- [ ] **Step 3: Implement `ListDLQ`**

In `internal/broker/broker.go`, add:

```go
// DLQ listing bounds: an unset/zero limit uses the default; the max caps a single
// page so a huge DLQ cannot be slurped in one request.
const (
	defaultDLQLimit = 50
	maxDLQLimit     = 1000
)

// ListDLQ returns up to limit dead-lettered jobs for a queue, starting at offset
// (0-based) in insertion order. A limit <= 0 uses the default; limits above the
// max are clamped. Job ids whose hash has already been removed are skipped.
func (b *Broker) ListDLQ(ctx context.Context, queue string, limit, offset int64) ([]job.Job, error) {
	if limit <= 0 {
		limit = defaultDLQLimit
	}
	if limit > maxDLQLimit {
		limit = maxDLQLimit
	}
	if offset < 0 {
		offset = 0
	}
	ids, err := b.rdb.LRange(ctx, dlqKey(queue), offset, offset+limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("broker: listing dlq for %q: %w", queue, err)
	}
	jobs := make([]job.Job, 0, len(ids))
	for _, id := range ids {
		h, err := b.rdb.HGetAll(ctx, jobKey(id)).Result()
		if err != nil {
			return nil, fmt.Errorf("broker: loading dlq job %s: %w", id, err)
		}
		if len(h) == 0 {
			continue // hash already cleaned up; skip
		}
		j, err := job.FromHash(h)
		if err != nil {
			return nil, fmt.Errorf("broker: decoding dlq job %s: %w", id, err)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestListDLQ -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker ListDLQ: paged dead-letter inspection"
```

---

## Task 3: `requeue.lua` + Broker `RequeueDLQ`

**Files:**
- Create: `internal/broker/scripts/requeue.lua`
- Modify: `internal/broker/scripts.go`
- Modify: `internal/broker/broker.go`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/broker_test.go`:

```go
func TestRequeueDLQMovesJobBackToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	id := deadLetter(t, b, ctx, "emails", "x")

	ok, err := b.RequeueDLQ(ctx, "emails", id)
	if err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}
	if !ok {
		t.Fatal("RequeueDLQ returned false, want true")
	}

	// gone from dlq
	if n, _ := rdb.LLen(ctx, "q:emails:dlq").Result(); n != 0 {
		t.Errorf("dlq len = %d, want 0", n)
	}
	// back in ready
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready card = %d, want 1", n)
	}
	// state reset to ready, attempts reset to 0
	h, err := rdb.HGetAll(ctx, "job:"+id).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if h["state"] != "ready" {
		t.Errorf("state = %q, want ready", h["state"])
	}
	if h["attempts"] != "0" {
		t.Errorf("attempts = %q, want 0", h["attempts"])
	}

	// and it is claimable again
	if _, ok, err := b.Claim(ctx, "emails", time.Minute); err != nil || !ok {
		t.Fatalf("Claim after requeue: ok=%v err=%v", ok, err)
	}
}

func TestRequeueDLQUnknownIDReturnsFalse(t *testing.T) {
	b, _ := newTestBroker(t)
	ok, err := b.RequeueDLQ(context.Background(), "emails", "nope")
	if err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}
	if ok {
		t.Error("RequeueDLQ returned true for an id not in the DLQ, want false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestRequeueDLQ -v`
Expected: FAIL — `b.RequeueDLQ` undefined.

- [ ] **Step 3: Create the Lua script**

Create `internal/broker/scripts/requeue.lua`:

```lua
-- requeue.lua — move a dead-lettered job back into ready for another run.
--
-- An operator action: a job that exhausted its retry budget is given a fresh
-- start. The remove-from-dlq and add-to-ready must be one atomic step so the job
-- can never be in both or neither. attempts is reset to 0 so the job gets a full
-- retry budget again; the ready score is rebuilt from the job's priority exactly
-- like promote.lua/reaper.lua do, so priority ordering is preserved.
--
-- KEYS[1] = dlq list   q:{name}:dlq
-- KEYS[2] = ready set   q:{name}:ready (ZSET scored by priority)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = now in unix milliseconds
-- ARGV[4] = priority scale (composite ready-score multiplier)
--
-- Returns 1 if the job was requeued, 0 if it was not present in the DLQ.

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
  return 0
end

local job_key = ARGV[2] .. ARGV[1]
local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
redis.call('HSET', job_key, 'state', 'ready', 'attempts', 0)

local score = priority * tonumber(ARGV[4]) - tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], score, ARGV[1])
return 1
```

- [ ] **Step 4: Register the script**

In `internal/broker/scripts.go`, append (after the `enqueueScript` block):

```go
//go:embed scripts/requeue.lua
var requeueSrc string

var requeueScript = redis.NewScript(requeueSrc)
```

- [ ] **Step 5: Implement `RequeueDLQ`**

In `internal/broker/broker.go`, add:

```go
// RequeueDLQ moves a dead-lettered job back into the ready set for another run,
// resetting its attempts to 0 (a deliberate operator retry). The move is atomic
// in requeue.lua. It returns (false, nil) when the id is not in the queue's DLQ.
func (b *Broker) RequeueDLQ(ctx context.Context, queue, id string) (bool, error) {
	n, err := requeueScript.Run(ctx, b.rdb,
		[]string{dlqKey(queue), readyKey(queue)},
		id, jobKeyPrefix, time.Now().UnixMilli(), priorityScale,
	).Int()
	if err != nil {
		return false, fmt.Errorf("broker: requeuing dlq job %s: %w", id, err)
	}
	return n == 1, nil
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestRequeueDLQ -v`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add internal/broker/scripts/requeue.lua internal/broker/scripts.go internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker RequeueDLQ with atomic requeue.lua (dlq -> ready, attempts reset)"
```

---

## Task 4: Broker `Queues` (discovery via SCAN)

**Files:**
- Modify: `internal/broker/broker.go`
- Test: `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/broker/broker_test.go`:

```go
func TestQueuesDiscoversDistinctNames(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("a"))); err != nil {
		t.Fatalf("Enqueue emails: %v", err)
	}
	if err := b.Enqueue(ctx, job.New("sms", []byte("b"))); err != nil {
		t.Fatalf("Enqueue sms: %v", err)
	}
	// a second key family for the same queue must not double-count it
	if err := b.Enqueue(ctx, job.New("emails", []byte("c")), broker.WithDelay(time.Hour)); err != nil {
		t.Fatalf("Enqueue emails delayed: %v", err)
	}

	names, err := b.Queues(ctx)
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(names) != 2 || names[0] != "emails" || names[1] != "sms" {
		t.Errorf("names = %v, want [emails sms]", names)
	}
}

func TestQueuesEmpty(t *testing.T) {
	b, _ := newTestBroker(t)
	names, err := b.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("names = %v, want empty", names)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestQueues -v`
Expected: FAIL — `b.Queues` undefined.

- [ ] **Step 3: Implement `Queues`**

In `internal/broker/broker.go`, add `"sort"` and `"strings"` to the import block if not already present, then add:

```go
// Queues discovers the distinct queue names present in Redis by scanning for the
// per-queue key prefix `q:{name}:...`. It uses a non-blocking SCAN cursor loop,
// dedupes, and returns the names sorted for stable output. On a large keyspace
// this still iterates every key (bounded work per round trip).
func (b *Broker) Queues(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		keys, next, err := b.rdb.Scan(ctx, cursor, "q:*", 200).Result()
		if err != nil {
			return nil, fmt.Errorf("broker: scanning queues: %w", err)
		}
		for _, k := range keys {
			// k is "q:{name}:{suffix...}"; the name is the segment between the
			// leading "q:" and the next ":".
			rest := strings.TrimPrefix(k, "q:")
			i := strings.IndexByte(rest, ':')
			if i <= 0 {
				continue
			}
			seen[rest[:i]] = struct{}{}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestQueues -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Full broker suite under race**

Run: `go test -race ./internal/broker/`
Expected: PASS (all existing + the four new methods).

- [ ] **Step 6: Commit**

```bash
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker Queues: discover queue names via SCAN"
```

---

## Task 5: `internal/api` — router, JSON helpers, and enqueue endpoint

**Files:**
- Create: `internal/api/api.go`
- Create: `internal/api/api_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/api/api_test.go`:

```go
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/api"
	"github.com/StrangeNoob/relay/internal/broker"
)

// apiTestRedisDB is this package's dedicated Redis DB. broker tests use 15,
// worker 14, metrics 13; api claims 12 so parallel `go test ./...` never collides.
const apiTestRedisDB = 12

func newTestAPI(t *testing.T) (http.Handler, *broker.Broker, *redis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: apiTestRedisDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	b := broker.New(rdb)
	h := api.New(b, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h, b, rdb
}

// do issues a request against the handler and returns the recorder.
func do(t *testing.T, h http.Handler, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, target, r)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestEnqueueEndpointCreatesJob(t *testing.T) {
	h, _, _ := newTestAPI(t)

	rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs", map[string]any{
		"payload": "hello",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID == "" {
		t.Error("response id is empty")
	}
	if resp.State != "ready" {
		t.Errorf("state = %q, want ready", resp.State)
	}
}

func TestEnqueueDuplicateReturns409(t *testing.T) {
	h, _, _ := newTestAPI(t)
	body := map[string]any{"payload": "x", "idempotency_key": "k1"}

	if rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs", body); rec.Code != http.StatusCreated {
		t.Fatalf("first enqueue status = %d, want 201", rec.Code)
	}
	rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs", body)
	if rec.Code != http.StatusConflict {
		t.Errorf("duplicate status = %d, want 409", rec.Code)
	}
}

func TestEnqueueBadJSONReturns400(t *testing.T) {
	h, _, _ := newTestAPI(t)
	req := httptest.NewRequest(http.MethodPost, "/api/queues/emails/jobs", bytes.NewReader([]byte("{not json")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestEnqueue -v`
Expected: FAIL — package `internal/api` does not exist / `api.New` undefined.

- [ ] **Step 3: Implement the API scaffold + enqueue**

Create `internal/api/api.go`:

```go
// Package api is Relay's HTTP control surface: a thin JSON layer over the broker.
// Handlers parse and validate the request, call one broker method, and encode the
// result and status code — all queue semantics stay in internal/broker.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

// API holds the dependencies shared by the handlers.
type API struct {
	broker *broker.Broker
	logger *slog.Logger
}

// New returns an http.Handler serving the Relay REST API over the given broker.
// A nil logger falls back to slog.Default(); tests pass a discard logger to stay
// quiet. Routes use stdlib method+path patterns (Go 1.22+).
func New(b *broker.Broker, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	a := &API{broker: b, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/queues/{queue}/jobs", a.enqueue)
	mux.HandleFunc("GET /api/queues/{queue}/stats", a.stats)
	mux.HandleFunc("GET /api/queues/{queue}/dlq", a.listDLQ)
	mux.HandleFunc("POST /api/queues/{queue}/dlq/{id}/requeue", a.requeueDLQ)
	mux.HandleFunc("GET /api/queues", a.queues)
	return mux
}

// writeJSON encodes v as the response body with the given status code.
func (a *API) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		a.logger.Error("api: encoding response", "err", err)
	}
}

// writeError emits a {"error": msg} body with the given status code.
func (a *API) writeError(w http.ResponseWriter, status int, msg string) {
	a.writeJSON(w, status, map[string]string{"error": msg})
}

// jobView is the JSON shape of a job in API responses. Payload is rendered as a
// string (UTF-8); created_at as RFC3339Nano.
type jobView struct {
	ID             string `json:"id"`
	Queue          string `json:"queue"`
	Payload        string `json:"payload"`
	State          string `json:"state"`
	Attempts       int    `json:"attempts"`
	MaxRetries     int    `json:"max_retries"`
	Priority       int    `json:"priority"`
	CreatedAt      string `json:"created_at"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func toJobView(j job.Job) jobView {
	return jobView{
		ID:             j.ID,
		Queue:          j.Queue,
		Payload:        string(j.Payload),
		State:          string(j.State),
		Attempts:       j.Attempts,
		MaxRetries:     j.MaxRetries,
		Priority:       j.Priority,
		CreatedAt:      j.CreatedAt.Format(time.RFC3339Nano),
		IdempotencyKey: j.IdempotencyKey,
	}
}

type enqueueRequest struct {
	Payload        string `json:"payload"`
	DelayMs        int64  `json:"delay_ms"`
	Priority       *int   `json:"priority"`
	IdempotencyKey string `json:"idempotency_key"`
}

type enqueueResponse struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// enqueue handles POST /api/queues/{queue}/jobs.
func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	j := job.New(queue, []byte(req.Payload))
	var opts []broker.EnqueueOption
	if req.DelayMs > 0 {
		opts = append(opts, broker.WithDelay(time.Duration(req.DelayMs)*time.Millisecond))
	}
	if req.Priority != nil {
		opts = append(opts, broker.WithPriority(*req.Priority))
	}
	if req.IdempotencyKey != "" {
		opts = append(opts, broker.WithIdempotencyKey(req.IdempotencyKey))
	}

	if err := a.broker.Enqueue(r.Context(), j, opts...); err != nil {
		if errors.Is(err, broker.ErrDuplicate) {
			a.writeError(w, http.StatusConflict, "duplicate idempotency key")
			return
		}
		a.logger.Error("api: enqueue failed", "queue", queue, "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Enqueue routes to delayed only for a future ready-at; mirror that here for
	// the reported state (Enqueue takes the job by value, so j.State is unchanged).
	state := job.StateReady
	if req.DelayMs > 0 {
		state = job.StateDelayed
	}
	a.writeJSON(w, http.StatusCreated, enqueueResponse{ID: j.ID, State: string(state)})
}

// parseInt64 parses a query value, returning def for an empty string.
func parseInt64(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
```

NOTE: this task adds `stats`, `listDLQ`, `requeueDLQ`, and `queues` to the router but those methods are implemented in Tasks 6–7. To keep the package compiling between tasks, add temporary stubs now and replace them in the next tasks:

```go
func (a *API) stats(w http.ResponseWriter, r *http.Request)      { a.writeError(w, http.StatusNotImplemented, "not implemented") }
func (a *API) listDLQ(w http.ResponseWriter, r *http.Request)    { a.writeError(w, http.StatusNotImplemented, "not implemented") }
func (a *API) requeueDLQ(w http.ResponseWriter, r *http.Request) { a.writeError(w, http.StatusNotImplemented, "not implemented") }
func (a *API) queues(w http.ResponseWriter, r *http.Request)     { a.writeError(w, http.StatusNotImplemented, "not implemented") }
```

(Remove `parseInt64`'s unused warning by leaving it; it is used in Task 6. If the compiler complains about an unused function in this task, that is fine — Go does not error on unused package-level functions, only unused imports/locals. `strconv` is used by `parseInt64`, so the import is fine.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/api/ -run TestEnqueue -v`
Expected: PASS (3 tests). Also `go build ./...` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/api/api.go internal/api/api_test.go
git commit -m "Add internal/api with enqueue endpoint and JSON scaffolding"
```

---

## Task 6: API stats + DLQ-list endpoints

**Files:**
- Modify: `internal/api/api.go` (replace `stats` and `listDLQ` stubs)
- Modify: `internal/api/api_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/api/api_test.go`:

```go
func TestStatsEndpoint(t *testing.T) {
	h, b, _ := newTestAPI(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := b.Enqueue(ctx, mustJob("emails", "x")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	rec := do(t, h, http.MethodGet, "/api/queues/emails/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var s struct {
		Ready    int64 `json:"ready"`
		Inflight int64 `json:"inflight"`
		Delayed  int64 `json:"delayed"`
		DLQ      int64 `json:"dlq"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.Ready != 2 {
		t.Errorf("ready = %d, want 2", s.Ready)
	}
}

func TestDLQListEndpoint(t *testing.T) {
	h, b, rdb := newTestAPI(t)
	ctx := context.Background()
	// seed one dead-lettered job directly: push id + write its hash.
	j := mustJob("emails", "dead")
	j.State = "dead"
	if err := rdb.HSet(ctx, "job:"+j.ID, j.ToHash()).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := rdb.RPush(ctx, "q:emails:dlq", j.ID).Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	_ = b

	rec := do(t, h, http.MethodGet, "/api/queues/emails/dlq", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var jobs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(jobs) != 1 || jobs[0]["id"] != j.ID {
		t.Errorf("jobs = %v, want one with id %s", jobs, j.ID)
	}
}

func TestDLQListBadLimitReturns400(t *testing.T) {
	h, _, _ := newTestAPI(t)
	rec := do(t, h, http.MethodGet, "/api/queues/emails/dlq?limit=abc", nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
```

Also add this helper to `api_test.go` (used above and later):

```go
// mustJob builds a job for tests via the broker's job package.
func mustJob(queue, payload string) job.Job {
	return job.New(queue, []byte(payload))
}
```

And add the import `"github.com/StrangeNoob/relay/internal/job"` to `api_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run 'TestStatsEndpoint|TestDLQList' -v`
Expected: FAIL — stats returns 501 (the stub), so `status = 501, want 200`.

- [ ] **Step 3: Replace the stubs**

In `internal/api/api.go`, replace the `stats` and `listDLQ` stub functions with:

```go
// stats handles GET /api/queues/{queue}/stats.
func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	s, err := a.broker.Stats(r.Context(), queue)
	if err != nil {
		a.logger.Error("api: stats failed", "queue", queue, "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	a.writeJSON(w, http.StatusOK, s)
}

// listDLQ handles GET /api/queues/{queue}/dlq?limit=&offset=.
func (a *API) listDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	limit, err := parseInt64(r.URL.Query().Get("limit"), 0)
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid limit")
		return
	}
	offset, err := parseInt64(r.URL.Query().Get("offset"), 0)
	if err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid offset")
		return
	}
	jobs, err := a.broker.ListDLQ(r.Context(), queue, limit, offset)
	if err != nil {
		a.logger.Error("api: list dlq failed", "queue", queue, "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	views := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		views = append(views, toJobView(j))
	}
	a.writeJSON(w, http.StatusOK, views)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/api/ -run 'TestStatsEndpoint|TestDLQList' -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/api/api.go internal/api/api_test.go
git commit -m "Implement API stats and DLQ-list endpoints"
```

---

## Task 7: API requeue + queues endpoints

**Files:**
- Modify: `internal/api/api.go` (replace `requeueDLQ` and `queues` stubs)
- Modify: `internal/api/api_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/api/api_test.go`:

```go
func TestRequeueEndpointMovesJobBack(t *testing.T) {
	h, _, rdb := newTestAPI(t)
	ctx := context.Background()
	j := mustJob("emails", "dead")
	j.State = "dead"
	if err := rdb.HSet(ctx, "job:"+j.ID, j.ToHash()).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := rdb.RPush(ctx, "q:emails:dlq", j.ID).Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	rec := do(t, h, http.MethodPost, "/api/queues/emails/dlq/"+j.ID+"/requeue", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if n, _ := rdb.ZCard(ctx, "q:emails:ready").Result(); n != 1 {
		t.Errorf("ready card = %d, want 1", n)
	}
	if n, _ := rdb.LLen(ctx, "q:emails:dlq").Result(); n != 0 {
		t.Errorf("dlq len = %d, want 0", n)
	}
}

func TestRequeueUnknownReturns404(t *testing.T) {
	h, _, _ := newTestAPI(t)
	rec := do(t, h, http.MethodPost, "/api/queues/emails/dlq/nope/requeue", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestQueuesEndpointListsNames(t *testing.T) {
	h, b, _ := newTestAPI(t)
	ctx := context.Background()
	if err := b.Enqueue(ctx, mustJob("emails", "a")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Enqueue(ctx, mustJob("sms", "b")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	rec := do(t, h, http.MethodGet, "/api/queues", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var names []string
	if err := json.Unmarshal(rec.Body.Bytes(), &names); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(names) != 2 || names[0] != "emails" || names[1] != "sms" {
		t.Errorf("names = %v, want [emails sms]", names)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run 'TestRequeue|TestQueuesEndpoint' -v`
Expected: FAIL — stubs return 501, so `status = 501, want 200/404`.

- [ ] **Step 3: Replace the stubs**

In `internal/api/api.go`, replace the `requeueDLQ` and `queues` stub functions with:

```go
// requeueDLQ handles POST /api/queues/{queue}/dlq/{id}/requeue.
func (a *API) requeueDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	id := r.PathValue("id")
	ok, err := a.broker.RequeueDLQ(r.Context(), queue, id)
	if err != nil {
		a.logger.Error("api: requeue failed", "queue", queue, "id", id, "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !ok {
		a.writeError(w, http.StatusNotFound, "job not found in dlq")
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]bool{"requeued": true})
}

// queues handles GET /api/queues.
func (a *API) queues(w http.ResponseWriter, r *http.Request) {
	names, err := a.broker.Queues(r.Context())
	if err != nil {
		a.logger.Error("api: queues failed", "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	a.writeJSON(w, http.StatusOK, names)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/api/ -run 'TestRequeue|TestQueuesEndpoint' -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Full api suite under race**

Run: `go test -race ./internal/api/`
Expected: PASS (all api tests).

- [ ] **Step 6: Commit**

```bash
git add internal/api/api.go internal/api/api_test.go
git commit -m "Implement API requeue and queue-discovery endpoints"
```

---

## Task 8: `cmd/server`

**Files:**
- Create: `cmd/server/main.go`

- [ ] **Step 1: Implement the server**

Create `cmd/server/main.go`:

```go
// Command server runs Relay's HTTP control surface: the JSON API plus a
// Prometheus /metrics endpoint and a health check. It is a thin wiring layer;
// all behaviour lives in internal/api, internal/broker, and internal/metrics.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/api"
	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/metrics"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	redisAddr := flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
	queuesFlag := flag.String("queues", "", "comma-separated queues for the /metrics depth collector")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("cannot reach redis", "addr", *redisAddr, "err", err)
		os.Exit(1)
	}

	// A metrics recorder on the broker means API enqueues are counted; a depth
	// collector for the configured queues exposes live gauges from the server.
	rec := metrics.NewRecorder()
	if qs := splitQueues(*queuesFlag); len(qs) > 0 {
		rec.Registry().MustRegister(metrics.NewDepthCollector(rdb, qs...))
	}
	b := broker.New(rdb, broker.WithMetrics(rec))

	mux := http.NewServeMux()
	mux.Handle("/api/", api.New(b, logger))
	mux.Handle("/metrics", promhttp.HandlerFor(rec.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		logger.Info("relay server listening", "addr", *addr, "redis", *redisAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("server shutdown error", "err", err)
	}
	logger.Info("relay server stopped cleanly")
}

// envOr returns the environment value for key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitQueues parses a comma-separated queue list, trimming blanks.
func splitQueues(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
```

- [ ] **Step 2: Build and vet**

Run:
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected: all clean.

- [ ] **Step 3: Smoke check (optional, requires Redis)**

Run (manual, then Ctrl-C):
```bash
go run ./cmd/server -addr :8080 -queues demo &
sleep 1
curl -s localhost:8080/healthz; echo
curl -s -X POST localhost:8080/api/queues/demo/jobs -d '{"payload":"hi"}'; echo
curl -s localhost:8080/api/queues/demo/stats; echo
curl -s localhost:8080/api/queues; echo
curl -s localhost:8080/metrics | grep -c relay_
kill %1
```
Expected: `ok`; a `201` job JSON; stats with `ready:1`; `["demo"]`; some `relay_` metric lines. Skip if no local Redis.

- [ ] **Step 4: Commit**

```bash
git add cmd/server/main.go
git commit -m "Add cmd/server: HTTP API + /metrics + /healthz with graceful shutdown"
```

---

## Task 9: Update CLAUDE.md and final verification

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Make these edits (match the file's exact wording when editing):

1. **Status line** — note Phase 3 is in progress with 3a (HTTP API + server) done. E.g. change the "Phase 3 (API/dashboard) is next" framing to "Phase 3 in progress: 3a HTTP API + server ✅; dashboard/SDK/packaging remain."
2. **"What exists today" list** — add bullets: `internal/api` (JSON REST: enqueue, stats, DLQ list, requeue, queue discovery) and `cmd/server` (API + `/metrics` + `/healthz`). Add `requeue.lua` to the Lua script enumeration(s). Add the new broker methods (`Stats`, `ListDLQ`, `RequeueDLQ`, `Queues`) to the broker description.
3. **Redis data model / lifecycle** — note the DLQ now has an inspect/requeue surface (was "inspect/requeue surface is Phase 3"); requeue moves dlq→ready resetting attempts.
4. **Layout (✅ built · ◻ planned)** — mark `internal/api/` ✅ and `cmd/server/main.go` ✅; leave `internal/client`, `web/`, `deployments/` as ◻ (3b–3d).
5. **Lua script inventory** — wherever the script list appears (e.g. `internal/broker/scripts/*.lua` go:embed line), add `requeue.lua`.
6. **Build order** — Phase 3 line: mark 3a (API/server) done; 3b dashboard, 3c SDK, 3d packaging remain.
7. **Known limitations** — add an API bullet: no auth (demo-grade); UTF-8 string payloads only (base64 future); offset/limit DLQ paging; server depth-gauge `/metrics` covers only `-queues` passed at startup; `Queues` discovery via `SCAN`.
8. **Run commands** — optionally add `go run ./cmd/server -queues demo` to the end-to-end section.

Keep claims accurate to what was built. Do not contradict existing invariants (at-least-once, atomic claim).

- [ ] **Step 2: Full verification**

Run:
```bash
go build ./...
go test -race ./...
go vet ./...
gofmt -l internal/ cmd/
```
Expected: build clean; all tests pass (broker DB 15, worker DB 14, metrics DB 13, api DB 12 — no collisions); vet clean; `gofmt -l` prints nothing. Tests need Redis at localhost:6379; if up, the broker/worker/metrics/api suites must run and pass.

If anything fails, STOP and report — do not paper over a real failure.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Document Phase 3a: HTTP API and server"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** broker `Stats` (Task 1), `ListDLQ` (Task 2), `RequeueDLQ` + `requeue.lua` (Task 3), `Queues` (Task 4); API enqueue/JSON scaffold (Task 5), stats + dlq list (Task 6), requeue + queues (Task 7); `cmd/server` with `/metrics` + `/healthz` + graceful shutdown and a metrics `Recorder` so API enqueues are counted (Task 8); CLAUDE.md + verification (Task 9). All spec sections mapped.
- **Type consistency:** broker methods — `Stats(ctx, queue) (Stats, error)`, `ListDLQ(ctx, queue, limit, offset int64) ([]job.Job, error)`, `RequeueDLQ(ctx, queue, id) (bool, error)`, `Queues(ctx) ([]string, error)` — match their call sites in `internal/api`. `Stats` struct JSON tags (`ready/inflight/delayed/dlq`) match the API test's decode struct. `api.New(b *broker.Broker, logger *slog.Logger) http.Handler` matches `cmd/server` and the api tests. `requeue.lua` ARGV order (id, prefix, now, priorityScale) matches `RequeueDLQ`'s `.Run` call. Test DBs: broker 15, worker 14, metrics 13, api 12.
- **No placeholders:** every code step has complete code. The Task 5 stubs are intentional, replaced in Tasks 6–7, and clearly marked.
- **Known soft spots:** Task 1's first test draft is explicitly discarded in favor of the deterministic version (the instruction says to use the second). Task 5 notes that `parseInt64` is used in Task 6 so it is not dead between tasks (and Go does not error on unused package-level funcs regardless).
