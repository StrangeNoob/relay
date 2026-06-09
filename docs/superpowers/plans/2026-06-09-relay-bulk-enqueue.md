# Bulk Enqueue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bulk enqueue (one request → N jobs) across the broker, API, and SDK, plus a Count field on the dashboard's enqueue form, so a single click produces visible traffic.

**Architecture:** `broker.EnqueueBulk` pipelines the existing `enqueue.lua` for N jobs in one Redis round-trip (no dedup). A new `POST /api/queues/{queue}/jobs/bulk` (`{count,payload,priority?,delay_ms?}`, cap 10000) builds N jobs and calls it. `client.EnqueueBulk` wraps the endpoint. The dashboard form gains a Count input that routes to bulk when >1. No engine/claim changes.

**Tech Stack:** Go (redis/go-redis pipeline, real-Redis tests), React+TS+Vitest. Reuses `enqueue.lua` unchanged.

**Spec:** [`docs/superpowers/specs/2026-06-09-relay-bulk-enqueue-design.md`](../specs/2026-06-09-relay-bulk-enqueue-design.md)

**Conventions:** real-Redis tests skip-not-fail (broker DB 15, api DB 12, client DB 11); errcheck CI lint → use `defer func(){ _ = x.Close() }()`; strict TS; rebuild committed `web/dist`.

---

## File Structure

- **Modify `internal/broker/broker.go`** — add `EnqueueBulk`.
- **Modify `internal/broker/broker_test.go`** — bulk tests.
- **Create `internal/api/bulk.go`** — bulk request/response types + `enqueueBulk` handler (keep `api.go` focused).
- **Modify `internal/api/api.go`** — register the bulk route.
- **Modify `internal/api/api_test.go`** — bulk endpoint tests.
- **Modify `internal/client/client.go`** — `BulkResult` + `EnqueueBulk`.
- **Modify `internal/client/client_test.go`** — hermetic bulk test.
- **Modify `internal/client/roundtrip_test.go`** — wire-compat bulk round-trip.
- **Create `web/src/lib/count.ts`** + **`count.test.ts`** — `clampCount`.
- **Modify `web/src/api.ts`** — `enqueueBulk`.
- **Modify `web/src/components/EnqueueForm.tsx`** — Count field.
- **Rebuild `web/dist`.**
- **Modify `CLAUDE.md`** — document bulk.

---

## Task 1: `broker.EnqueueBulk`

**Files:** `internal/broker/broker.go`, `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing tests** — append to `internal/broker/broker_test.go`:

```go
func TestEnqueueBulkAddsAllToReady(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()
	jobs := make([]job.Job, 50)
	for i := range jobs {
		jobs[i] = job.New("emails", []byte("x"))
	}
	n, err := b.EnqueueBulk(ctx, jobs)
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
	if n != 50 {
		t.Errorf("returned %d, want 50", n)
	}
	if c := rdb.ZCard(ctx, "q:emails:ready").Val(); c != 50 {
		t.Errorf("ready ZCARD = %d, want 50", c)
	}
}

func TestEnqueueBulkWithDelayGoesToDelayed(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()
	jobs := []job.Job{job.New("emails", []byte("a")), job.New("emails", []byte("b"))}
	n, err := b.EnqueueBulk(ctx, jobs, broker.WithDelay(time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("EnqueueBulk: n=%d err=%v", n, err)
	}
	if c := rdb.ZCard(ctx, "q:emails:delayed").Val(); c != 2 {
		t.Errorf("delayed ZCARD = %d, want 2", c)
	}
	if c := rdb.ZCard(ctx, "q:emails:ready").Val(); c != 0 {
		t.Errorf("ready ZCARD = %d, want 0", c)
	}
}

func TestEnqueueBulkEmptyIsNoop(t *testing.T) {
	b, _ := newTestBroker(t)
	n, err := b.EnqueueBulk(context.Background(), nil)
	if err != nil || n != 0 {
		t.Errorf("EnqueueBulk(nil) = (%d, %v), want (0, nil)", n, err)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/leon/WorkSpace/relay-bulk/web 2>/dev/null; cd /Users/leon/WorkSpace/relay-bulk && go test ./internal/broker/ -run TestEnqueueBulk -v`
Expected: FAIL — `b.EnqueueBulk` undefined. (Needs Redis; note if SKIP.)

- [ ] **Step 3: Implement `EnqueueBulk` in `internal/broker/broker.go`** — add after `Enqueue`:

```go
// EnqueueBulk enqueues many jobs (all on their own queue) in a single Redis
// pipeline and returns the number enqueued. The shared delay/priority options
// apply to every job. Unlike Enqueue it does NOT dedup — bulk is for volume, and
// every job.New already has a unique id, so jobs are distinct even with identical
// payloads. It reuses the same atomic enqueue.lua per job.
func (b *Broker) EnqueueBulk(ctx context.Context, jobs []job.Job, opts ...EnqueueOption) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	now := time.Now()

	// Ensure the script is cached so the pipelined EVALSHA calls don't NOSCRIPT
	// (a pipeline can't auto-fallback to EVAL mid-flight).
	if err := enqueueScript.Load(ctx, b.rdb).Err(); err != nil {
		return 0, fmt.Errorf("broker: loading enqueue script: %w", err)
	}

	pipe := b.rdb.Pipeline()
	for i := range jobs {
		j := jobs[i]
		if cfg.prioritySet {
			j.Priority = cfg.priority
		}
		j.Priority = clampPriority(j.Priority)

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

		// Bulk never dedups: useDedup "0", no TTL, empty dedup key.
		h := j.ToHash()
		args := make([]any, 0, 4+2*len(h))
		args = append(args, j.ID, score, 0, "0")
		for k, v := range h {
			args = append(args, k, v)
		}
		enqueueScript.Run(ctx, pipe, []string{jobKey(j.ID), targetKey, ""}, args...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("broker: bulk enqueue: %w", err)
	}
	for i := range jobs {
		b.metrics.IncEnqueued(jobs[i].Queue)
	}
	return len(jobs), nil
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cd /Users/leon/WorkSpace/relay-bulk && go test -race ./internal/broker/ -run TestEnqueueBulk -v` → PASS (3). Then `gofmt -l internal/broker/`, `go build ./...`, `go vet ./internal/broker/` clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/leon/WorkSpace/relay-bulk
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker EnqueueBulk: pipeline N jobs in one round-trip"
```

---

## Task 2: API bulk endpoint

**Files:** Create `internal/api/bulk.go`; modify `internal/api/api.go`, `internal/api/api_test.go`

- [ ] **Step 1: Write the failing tests** — append to `internal/api/api_test.go`:

```go
func TestBulkEnqueue(t *testing.T) {
	h, _, rdb := newTestAPI(t)
	rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs/bulk", map[string]any{"count": 50, "payload": "x"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Enqueued int    `json:"enqueued"`
		State    string `json:"state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Enqueued != 50 || resp.State != "ready" {
		t.Errorf("resp = %+v, want {50 ready}", resp)
	}
	if c := rdb.ZCard(context.Background(), "q:emails:ready").Val(); c != 50 {
		t.Errorf("ready = %d, want 50", c)
	}
}

func TestBulkEnqueueRejectsBadCount(t *testing.T) {
	h, _, _ := newTestAPI(t)
	for _, c := range []int{0, 10001} {
		rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs/bulk", map[string]any{"count": c, "payload": "x"})
		if rec.Code != http.StatusBadRequest {
			t.Errorf("count=%d: status = %d, want 400", c, rec.Code)
		}
	}
}

func TestBulkEnqueueDelayedState(t *testing.T) {
	h, _, _ := newTestAPI(t)
	rec := do(t, h, http.MethodPost, "/api/queues/emails/jobs/bulk", map[string]any{"count": 3, "payload": "x", "delay_ms": 60000})
	var resp struct{ State string `json:"state"` }
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != http.StatusCreated || resp.State != "delayed" {
		t.Errorf("got %d %s, want 201 delayed", rec.Code, resp.State)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/leon/WorkSpace/relay-bulk && go test ./internal/api/ -run TestBulkEnqueue -v`
Expected: FAIL — route not registered (404 → 201 assertion fails). Note Redis availability.

- [ ] **Step 3: Create `internal/api/bulk.go`**

```go
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/job"
)

// maxBulkCount caps how many jobs one bulk request may enqueue.
const maxBulkCount = 10000

type bulkEnqueueRequest struct {
	Count    int    `json:"count"`
	Payload  string `json:"payload"`
	Priority *int   `json:"priority"`
	DelayMs  int64  `json:"delay_ms"`
}

type bulkEnqueueResponse struct {
	Enqueued int    `json:"enqueued"`
	State    string `json:"state"`
}

// enqueueBulk handles POST /api/queues/{queue}/jobs/bulk: it enqueues `count`
// jobs built from one payload template (distinct by id). No idempotency.
func (a *API) enqueueBulk(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	var req bulkEnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Count < 1 || req.Count > maxBulkCount {
		a.writeError(w, http.StatusBadRequest, "count must be between 1 and 10000")
		return
	}

	var opts []broker.EnqueueOption
	if req.DelayMs > 0 {
		opts = append(opts, broker.WithDelay(time.Duration(req.DelayMs)*time.Millisecond))
	}
	if req.Priority != nil {
		opts = append(opts, broker.WithPriority(*req.Priority))
	}

	jobs := make([]job.Job, req.Count)
	for i := range jobs {
		jobs[i] = job.New(queue, []byte(req.Payload))
	}

	n, err := a.broker.EnqueueBulk(r.Context(), jobs, opts...)
	if err != nil {
		a.logger.Error("api: bulk enqueue failed", "queue", queue, "count", req.Count, "err", err)
		a.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	state := job.StateReady
	if req.DelayMs > 0 {
		state = job.StateDelayed
	}
	a.writeJSON(w, http.StatusCreated, bulkEnqueueResponse{Enqueued: n, State: string(state)})
}
```

- [ ] **Step 4: Register the route in `internal/api/api.go`** — add after the single-enqueue route:

```go
	mux.HandleFunc("POST /api/queues/{queue}/jobs/bulk", a.enqueueBulk)
```

(Place it directly below `mux.HandleFunc("POST /api/queues/{queue}/jobs", a.enqueue)`. Go 1.22 routing distinguishes the longer `/bulk` pattern from `/jobs`.)

- [ ] **Step 5: Run to verify they pass**

Run: `cd /Users/leon/WorkSpace/relay-bulk && go test -race ./internal/api/ -run TestBulkEnqueue -v` → PASS (3). Then `gofmt -l internal/api/`, `go build ./...`, `go vet ./internal/api/` clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/leon/WorkSpace/relay-bulk
git add internal/api/bulk.go internal/api/api.go internal/api/api_test.go
git commit -m "Add POST /api/queues/{queue}/jobs/bulk endpoint"
```

---

## Task 3: `client.EnqueueBulk`

**Files:** `internal/client/client.go`, `internal/client/client_test.go`, `internal/client/roundtrip_test.go`

- [ ] **Step 1: Write the failing tests** — append to `internal/client/client_test.go`:

```go
func TestEnqueueBulkSendsCountAndDecodes(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"enqueued":200,"state":"ready"}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	res, err := c.EnqueueBulk(context.Background(), "emails", []byte("hi"), 200, client.WithPriority(5))
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
	if res.Enqueued != 200 || res.State != "ready" {
		t.Errorf("res = %+v, want {200 ready}", res)
	}
	if gotPath != "/api/queues/emails/jobs/bulk" {
		t.Errorf("path = %s", gotPath)
	}
	if gotBody["count"].(float64) != 200 || gotBody["payload"] != "hi" || gotBody["priority"].(float64) != 5 {
		t.Errorf("body = %v", gotBody)
	}
}
```

And append to `internal/client/roundtrip_test.go`:

```go
func TestRoundTripEnqueueBulk(t *testing.T) {
	c, _, _ := newRoundTrip(t)
	ctx := context.Background()
	res, err := c.EnqueueBulk(ctx, "emails", []byte(`{"n":1}`), 25)
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
	if res.Enqueued != 25 {
		t.Errorf("enqueued = %d, want 25", res.Enqueued)
	}
	s, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 25 {
		t.Errorf("ready = %d, want 25", s.Ready)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/leon/WorkSpace/relay-bulk && go test ./internal/client/ -run 'EnqueueBulk' -v`
Expected: FAIL — `c.EnqueueBulk` undefined.

- [ ] **Step 3: Implement in `internal/client/client.go`** — add after `Enqueue`:

```go
// BulkResult is the response from a bulk enqueue.
type BulkResult struct {
	Enqueued int    `json:"enqueued"`
	State    string `json:"state"`
}

// bulkBody is the JSON request for a bulk enqueue.
type bulkBody struct {
	Count          int    `json:"count"`
	Payload        string `json:"payload"`
	DelayMs        int64  `json:"delay_ms,omitempty"`
	Priority       *int   `json:"priority,omitempty"`
	IdempotencyKey string `json:"-"` // bulk has no dedup; ignored
}

// EnqueueBulk enqueues count copies of payload onto a queue in one request.
// WithDelay/WithPriority apply to all jobs; WithIdempotencyKey is ignored (bulk
// has no dedup).
func (c *Client) EnqueueBulk(ctx context.Context, queue string, payload []byte, count int, opts ...EnqueueOption) (BulkResult, error) {
	// Reuse the enqueue options to capture delay/priority, then map to bulkBody.
	var eb enqueueBody
	for _, opt := range opts {
		opt(&eb)
	}
	body := bulkBody{Count: count, Payload: string(payload), DelayMs: eb.DelayMs, Priority: eb.Priority}
	var res BulkResult
	if err := c.do(ctx, http.MethodPost, "/api/queues/"+url.PathEscape(queue)+"/jobs/bulk", body, &res); err != nil {
		return BulkResult{}, err
	}
	return res, nil
}
```

(Note: `EnqueueOption` mutates an `enqueueBody`; we apply the opts to a throwaway `enqueueBody` to extract `DelayMs`/`Priority`, ignoring its payload/idempotency fields. `IdempotencyKey` on `bulkBody` is `json:"-"` so it's never sent.)

- [ ] **Step 4: Run to verify they pass**

Run: `cd /Users/leon/WorkSpace/relay-bulk && go test -race ./internal/client/ -run 'EnqueueBulk|RoundTrip' -v` → PASS. Then `gofmt -l internal/client/`, `go build ./...`, `go vet ./internal/client/` clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/leon/WorkSpace/relay-bulk
git add internal/client/client.go internal/client/client_test.go internal/client/roundtrip_test.go
git commit -m "Add client EnqueueBulk + wire-compat round-trip"
```

---

## Task 4: Dashboard Count field

**Files:** Create `web/src/lib/count.ts`, `web/src/lib/count.test.ts`; modify `web/src/api.ts`, `web/src/components/EnqueueForm.tsx`; rebuild `web/dist`

- [ ] **Step 1: Write the failing test** — `web/src/lib/count.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { clampCount } from "./count";

describe("clampCount", () => {
  it("defaults to 1 for empty/non-numeric", () => {
    expect(clampCount("")).toBe(1);
    expect(clampCount("abc")).toBe(1);
  });
  it("floors to an integer", () => {
    expect(clampCount("12.9")).toBe(12);
  });
  it("clamps to [1, max]", () => {
    expect(clampCount("0")).toBe(1);
    expect(clampCount("-5")).toBe(1);
    expect(clampCount("99999")).toBe(10000);
    expect(clampCount("250")).toBe(250);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/leon/WorkSpace/relay-bulk/web && npx vitest run count`
Expected: FAIL — `./count` not found.

- [ ] **Step 3: Implement `web/src/lib/count.ts`**

```ts
// clampCount parses the enqueue-form count field: floors to an integer and clamps
// to [1, max], defaulting to 1 on empty/non-numeric input.
export function clampCount(raw: string, max = 10000): number {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 1) return 1;
  return n > max ? max : n;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/leon/WorkSpace/relay-bulk/web && npx vitest run count` → PASS. `npm run typecheck` clean.

- [ ] **Step 5: Add `enqueueBulk` to `web/src/api.ts`** — append (next to `enqueue`):

```ts
export interface BulkEnqueueRequest {
  count: number;
  payload: string;
  priority?: number;
  delay_ms?: number;
}

export async function enqueueBulk(queue: string, body: BulkEnqueueRequest): Promise<{ enqueued: number; state: string }> {
  const r = await fetch(`/api/queues/${encodeURIComponent(queue)}/jobs/bulk`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`bulk enqueue: ${r.status}`);
  return r.json();
}
```

- [ ] **Step 6: Add the Count field to `web/src/components/EnqueueForm.tsx`** — replace the file with:

```tsx
import { useState, type FormEvent } from "react";
import { enqueue, enqueueBulk, type EnqueueRequest, type BulkEnqueueRequest } from "../api";
import { clampCount } from "../lib/count";

interface EnqueueFormProps {
  queue: string;
  onClose: () => void;
  onEnqueued: () => void;
}

export function EnqueueForm({ queue, onClose, onEnqueued }: EnqueueFormProps) {
  const [payload, setPayload] = useState('{"hello":"world"}');
  const [priority, setPriority] = useState("");
  const [delayMs, setDelayMs] = useState("");
  const [key, setKey] = useState("");
  const [count, setCount] = useState("1");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setErr("");
    const n = clampCount(count);
    try {
      if (n > 1) {
        const body: BulkEnqueueRequest = { count: n, payload };
        if (priority.trim() !== "") body.priority = Number(priority);
        if (delayMs.trim() !== "") body.delay_ms = Number(delayMs);
        await enqueueBulk(queue, body);
      } else {
        const body: EnqueueRequest = { payload };
        if (priority.trim() !== "") body.priority = Number(priority);
        if (delayMs.trim() !== "") body.delay_ms = Number(delayMs);
        if (key.trim() !== "") body.idempotency_key = key.trim();
        await enqueue(queue, body);
      }
      onEnqueued();
      onClose();
    } catch (e2) {
      setErr(String(e2));
      setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <form className="modal" onClick={(e) => e.stopPropagation()} onSubmit={submit}>
        <h3>Enqueue to <span className="modal-q">{queue}</span></h3>
        <label>Payload<textarea value={payload} onChange={(e) => setPayload(e.target.value)} rows={3} /></label>
        <div className="modal-row">
          <label>Count<input value={count} onChange={(e) => setCount(e.target.value)} placeholder="1" inputMode="numeric" /></label>
          <label>Priority<input value={priority} onChange={(e) => setPriority(e.target.value)} placeholder="0" inputMode="numeric" /></label>
          <label>Delay (ms)<input value={delayMs} onChange={(e) => setDelayMs(e.target.value)} placeholder="0" inputMode="numeric" /></label>
        </div>
        <label>Idempotency key<input value={key} onChange={(e) => setKey(e.target.value)} placeholder="(optional; single only)" /></label>
        {err && <div className="modal-err">{err}</div>}
        <div className="modal-actions">
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
          <button type="submit" className="btn-accent" disabled={busy}>{busy ? "Enqueuing…" : "Enqueue"}</button>
        </div>
      </form>
    </div>
  );
}
```

(The `.modal-row` already lays out its children in a grid; adding a third field reflows fine. `clampCount` guards the count; bulk ignores the idempotency key, so the label notes "single only".)

- [ ] **Step 7: Typecheck, test, rebuild dist**

Run:
```bash
cd /Users/leon/WorkSpace/relay-bulk/web
npm run typecheck
npx vitest run
npm run build
```
Expected: clean; all tests pass; `web/dist` regenerated.

- [ ] **Step 8: Commit**

```bash
cd /Users/leon/WorkSpace/relay-bulk
git add web/src/lib/count.ts web/src/lib/count.test.ts web/src/api.ts web/src/components/EnqueueForm.tsx web/dist
git commit -m "Add Count field to the dashboard enqueue form (bulk burst)"
```

---

## Task 5: CLAUDE.md + final verification

**Files:** `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Make these edits (match the file's wording):
1. **`internal/broker` bullet** — add `EnqueueBulk` (pipelines N enqueues in one round-trip; no dedup) to the listed methods.
2. **`internal/api` bullet** — add `POST /api/queues/{queue}/jobs/bulk` (`{count,payload,priority?,delay_ms?}` → `{enqueued,state}`; count 1–10000; no idempotency) in `internal/api/bulk.go`.
3. **`internal/client` bullet** — add `EnqueueBulk` (+ `BulkResult`).
4. **`web/` bullet** — note the enqueue form has a Count field for bulk bursts.
5. **Known limitations** — add: bulk enqueue returns a count not per-job ids; bulk has no idempotency; cap 10000/request; one queue per request.

- [ ] **Step 2: Full verification**

Run:
```bash
cd /Users/leon/WorkSpace/relay-bulk
go build ./...
go test -race ./...
go vet ./...
gofmt -l internal/ cmd/
( cd web && npm run typecheck && npm run test && npm run build && git diff --exit-code -- dist )
```
Expected: Go build/tests/vet/fmt clean (broker DB 15, worker DB 14, metrics DB 13, api DB 12, client DB 11; needs Redis); frontend typecheck/test/build clean and dist in sync. STOP and report on any failure.

- [ ] **Step 3: Commit**

```bash
cd /Users/leon/WorkSpace/relay-bulk
git add CLAUDE.md
git commit -m "Document bulk enqueue"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** broker `EnqueueBulk` pipeline + no dedup + metric/job (Task 1); `POST .../jobs/bulk` with count cap 10000 + ready/delayed state (Task 2); `client.EnqueueBulk` + `BulkResult` + hermetic + round-trip (Task 3); dashboard Count field + `clampCount` + `enqueueBulk` (Task 4); docs + verification (Task 5). Covers every spec section.
- **Type consistency:** broker `EnqueueBulk(ctx, []job.Job, ...EnqueueOption) (int, error)`; API `bulkEnqueueResponse{Enqueued,State}` (json `enqueued`/`state`) == client `BulkResult{Enqueued,State}` == web `{enqueued,state}`; request `{count,payload,priority?,delay_ms?}` consistent across api `bulkEnqueueRequest`, client `bulkBody`, web `BulkEnqueueRequest`; cap `10000` in api (`maxBulkCount`) and web (`clampCount` max). Test DBs unchanged.
- **No placeholders:** every step has complete code/commands. `EnqueueBulk` reuses `enqueue.lua` via `enqueueScript.Load` + pipelined `Run` (no NOSCRIPT risk); the no-dedup path mirrors single `Enqueue` with `useDedup "0"`, empty dedup key.
- **errcheck:** test handlers use `_, _ = w.Write(...)` / `_ = json.NewDecoder(...).Decode(...)`; no bare deferred Close added. No Go engine/claim changes.
```
