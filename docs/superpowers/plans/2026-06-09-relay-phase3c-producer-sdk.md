# Phase 3c Producer SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A small, self-contained Go HTTP SDK (`internal/client`) for the Relay API — enqueue (delay/priority/idempotency) plus stats/DLQ/requeue/queue-discovery — and a `cmd/demo` refactored to produce through it over HTTP.

**Architecture:** `internal/client` is a thin `net/http` client with functional options, its own DTOs (no `broker`/`job`/Redis import), and typed errors (`ErrDuplicate`, `ErrNotFound`, `*APIError`). A shared `do` helper performs each request and maps non-2xx to errors; per-method wrappers map 409/404 to the sentinels. `cmd/demo` becomes a pure SDK consumer (`-server` replaces `-redis`).

**Tech Stack:** Go stdlib only (`net/http`, `encoding/json`, `net/url`); hermetic `httptest` unit tests + one real-Redis wire-compat round-trip test (DB 11).

**Spec:** [`docs/superpowers/specs/2026-06-09-relay-phase3c-producer-sdk-design.md`](../specs/2026-06-09-relay-phase3c-producer-sdk-design.md)

**Convention reminder:** golangci-lint (errcheck) runs in CI; always write `defer func() { _ = x.Close() }()` for deferred Close calls (a bare `defer resp.Body.Close()` fails the lint).

---

## File Structure

- **Create `internal/client/client.go`** — `Client`, `New`, options, DTOs, errors, the `do` helper, and all five methods.
- **Create `internal/client/client_test.go`** — hermetic `httptest` unit tests (package `client_test`).
- **Create `internal/client/roundtrip_test.go`** — wire-compat test through `api.New(broker)` on real Redis DB 11 (package `client_test`).
- **Modify `cmd/demo/main.go`** — produce through the SDK over HTTP.
- **Modify `CLAUDE.md`** — document 3c.

---

## Task 1: Client core + `Enqueue`

**Files:** Create `internal/client/client.go`, `internal/client/client_test.go`

- [ ] **Step 1: Write the failing tests** — create `internal/client/client_test.go`:

```go
package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/StrangeNoob/relay/internal/client"
)

func TestEnqueueSendsBodyAndDecodesResult(t *testing.T) {
	var gotMethod, gotPath, gotCT string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotCT = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"abc","state":"ready"}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL)
	res, err := c.Enqueue(context.Background(), "emails", []byte("hi"),
		client.WithDelay(2*time.Second), client.WithPriority(5), client.WithIdempotencyKey("k1"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if res.ID != "abc" || res.State != "ready" {
		t.Errorf("res = %+v, want {abc ready}", res)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/queues/emails/jobs" {
		t.Errorf("method/path = %s %s", gotMethod, gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type = %q", gotCT)
	}
	if gotBody["payload"] != "hi" || gotBody["delay_ms"].(float64) != 2000 ||
		gotBody["priority"].(float64) != 5 || gotBody["idempotency_key"] != "k1" {
		t.Errorf("body = %v", gotBody)
	}
}

func TestEnqueueDuplicateReturnsErrDuplicate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"duplicate idempotency key"}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	_, err := c.Enqueue(context.Background(), "emails", []byte("x"), client.WithIdempotencyKey("k"))
	if !errors.Is(err, client.ErrDuplicate) {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}
}

func TestEnqueueOmitsUnsetOptionalFields(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"a","state":"ready"}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	if _, err := c.Enqueue(context.Background(), "emails", []byte("x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok := gotBody["priority"]; ok {
		t.Error("priority should be omitted when unset")
	}
	if _, ok := gotBody["delay_ms"]; ok {
		t.Error("delay_ms should be omitted when unset")
	}
}

func TestEnqueuePriorityZeroIsSent(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"a","state":"ready"}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	if _, err := c.Enqueue(context.Background(), "emails", []byte("x"), client.WithPriority(0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	v, ok := gotBody["priority"]
	if !ok || v.(float64) != 0 {
		t.Errorf("priority = %v ok=%v, want 0 present", v, ok)
	}
}

func TestEnqueueAPIErrorOnServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	_, err := c.Enqueue(context.Background(), "emails", []byte("x"))
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 500 || apiErr.Message != "boom" {
		t.Errorf("err = %v, want APIError{500, boom}", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/client/ -run TestEnqueue -v`
Expected: FAIL — package `internal/client` does not exist.

- [ ] **Step 3: Implement `internal/client/client.go`**

```go
// Package client is a small HTTP SDK for the Relay API. It lets a producer
// enqueue jobs and inspect queues over HTTP without importing the broker or
// talking to Redis directly — it depends only on the standard library.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrDuplicate is returned by Enqueue when the job was rejected as an
// idempotency-key duplicate (HTTP 409).
var ErrDuplicate = errors.New("relay client: duplicate idempotency key")

// ErrNotFound is returned by Requeue when the id is not in the queue's DLQ (404).
var ErrNotFound = errors.New("relay client: not found")

// APIError is returned for any other non-2xx response; it carries the status and
// the message from the server's {"error":...} envelope.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("relay api: %d %s", e.Status, e.Message)
}

// Client talks to a Relay server's HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
}

// Option customises a Client.
type Option func(*Client)

// WithHTTPClient sets the underlying *http.Client (custom transport/timeout).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// WithTimeout sets the request timeout on the client's *http.Client.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.http.Timeout = d }
}

// New builds a client for the given base URL (e.g. "http://localhost:8080").
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// EnqueueResult is the response from a successful enqueue.
type EnqueueResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// enqueueBody is the JSON request for an enqueue. Optional fields use omitempty
// so unset options are not sent; Priority is a pointer so a deliberate 0 is sent
// while an unset priority is omitted (mirrors the API's *int).
type enqueueBody struct {
	Payload        string `json:"payload"`
	DelayMs        int64  `json:"delay_ms,omitempty"`
	Priority       *int   `json:"priority,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// EnqueueOption customises a single Enqueue call.
type EnqueueOption func(*enqueueBody)

// WithDelay schedules the job d into the future.
func WithDelay(d time.Duration) EnqueueOption {
	return func(b *enqueueBody) { b.DelayMs = d.Milliseconds() }
}

// WithPriority sets the job's claim priority (higher is more urgent).
func WithPriority(p int) EnqueueOption {
	return func(b *enqueueBody) { b.Priority = &p }
}

// WithIdempotencyKey sets the idempotency key (a duplicate within the dedup TTL
// is rejected with ErrDuplicate).
func WithIdempotencyKey(k string) EnqueueOption {
	return func(b *enqueueBody) { b.IdempotencyKey = k }
}

// Enqueue submits a job to a queue. A duplicate idempotency key yields
// ErrDuplicate.
func (c *Client) Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (EnqueueResult, error) {
	body := enqueueBody{Payload: string(payload)}
	for _, opt := range opts {
		opt(&body)
	}
	var res EnqueueResult
	err := c.do(ctx, http.MethodPost, "/api/queues/"+url.PathEscape(queue)+"/jobs", body, &res)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
		return EnqueueResult{}, ErrDuplicate
	}
	if err != nil {
		return EnqueueResult{}, err
	}
	return res, nil
}

// do performs a request with an optional JSON body, decoding a 2xx JSON response
// into out (when non-nil). Non-2xx responses become an *APIError carrying the
// status and the {"error":...} message; transport/marshal errors are wrapped.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("relay client: marshal request: %w", err)
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("relay client: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("relay client: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Status: resp.StatusCode, Message: decodeErrMessage(resp.Body)}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("relay client: decode response: %w", err)
		}
	}
	return nil
}

// decodeErrMessage extracts the message from a {"error":"..."} envelope.
func decodeErrMessage(r io.Reader) string {
	var env struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(r).Decode(&env); err == nil && env.Error != "" {
		return env.Error
	}
	return "request failed"
}
```

NOTE: `strconv` is imported here but first used in Task 3 (`ListDLQ`). If the Go compiler complains about an unused import in this task, omit the `strconv` import now and add it in Task 3. (Author it without `strconv` in Task 1, since Task 1 does not use it.)

To keep Task 1 compiling, the import block for Task 1 should be exactly: `bytes`, `context`, `encoding/json`, `errors`, `fmt`, `io`, `net/http`, `net/url`, `strings`, `time` (NO `strconv` yet).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/client/ -run TestEnqueue -v` → PASS (5 tests). Then `gofmt -l internal/client/`, `go build ./...`, `go vet ./internal/client/` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/client/client.go internal/client/client_test.go
git commit -m "Add client SDK core and Enqueue with typed errors"
```

---

## Task 2: `Stats` and `Queues`

**Files:** Modify `internal/client/client.go`, `internal/client/client_test.go`

- [ ] **Step 1: Write the failing tests** — append to `internal/client/client_test.go`:

```go
func TestStatsDecodes(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ready":3,"inflight":1,"delayed":2,"dlq":4}`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	s, err := c.Stats(context.Background(), "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 3 || s.Inflight != 1 || s.Delayed != 2 || s.DLQ != 4 {
		t.Errorf("stats = %+v", s)
	}
	if gotPath != "/api/queues/emails/stats" {
		t.Errorf("path = %s", gotPath)
	}
}

func TestQueuesDecodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/queues" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`["emails","sms"]`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	qs, err := c.Queues(context.Background())
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(qs) != 2 || qs[0] != "emails" || qs[1] != "sms" {
		t.Errorf("queues = %v", qs)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/client/ -run 'TestStats|TestQueues' -v`
Expected: FAIL — `c.Stats` / `c.Queues` undefined.

- [ ] **Step 3: Implement `Stats` and `Queues`** — add to `internal/client/client.go`:

```go
// Stats is a queue's point-in-time depth by state.
type Stats struct {
	Ready    int64 `json:"ready"`
	Inflight int64 `json:"inflight"`
	Delayed  int64 `json:"delayed"`
	DLQ      int64 `json:"dlq"`
}

// Stats returns the current depth of each of a queue's states.
func (c *Client) Stats(ctx context.Context, queue string) (Stats, error) {
	var s Stats
	if err := c.do(ctx, http.MethodGet, "/api/queues/"+url.PathEscape(queue)+"/stats", nil, &s); err != nil {
		return Stats{}, err
	}
	return s, nil
}

// Queues returns the distinct queue names the server knows about.
func (c *Client) Queues(ctx context.Context) ([]string, error) {
	var qs []string
	if err := c.do(ctx, http.MethodGet, "/api/queues", nil, &qs); err != nil {
		return nil, err
	}
	return qs, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/client/ -run 'TestStats|TestQueues' -v` → PASS. Then `gofmt -l internal/client/`, `go build ./...`, `go vet ./internal/client/` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/client/client.go internal/client/client_test.go
git commit -m "Add client Stats and Queues"
```

---

## Task 3: `ListDLQ` and `Requeue`

**Files:** Modify `internal/client/client.go`, `internal/client/client_test.go`

- [ ] **Step 1: Write the failing tests** — append to `internal/client/client_test.go`:

```go
func TestListDLQDecodesAndSendsPaging(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"j1","queue":"emails","payload":"p","state":"dead","attempts":5,"max_retries":5,"priority":0,"created_at":"2026-06-09T00:00:00Z"}]`))
	}))
	defer srv.Close()
	c := client.New(srv.URL)
	jobs, err := c.ListDLQ(context.Background(), "emails", 10, 5)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "j1" || jobs[0].Attempts != 5 || jobs[0].Payload != "p" {
		t.Errorf("jobs = %+v", jobs)
	}
	if gotQuery != "limit=10&offset=5" {
		t.Errorf("query = %q, want limit=10&offset=5", gotQuery)
	}
}

func TestRequeueOKAndNotFound(t *testing.T) {
	// 200 OK
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/queues/emails/dlq/j1/requeue" {
			t.Errorf("method/path = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"requeued":true}`))
	}))
	defer ok.Close()
	if err := client.New(ok.URL).Requeue(context.Background(), "emails", "j1"); err != nil {
		t.Errorf("Requeue ok: %v", err)
	}

	// 404 -> ErrNotFound
	nf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"job not found in dlq"}`))
	}))
	defer nf.Close()
	if err := client.New(nf.URL).Requeue(context.Background(), "emails", "nope"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/client/ -run 'TestListDLQ|TestRequeue' -v`
Expected: FAIL — `c.ListDLQ` / `c.Requeue` undefined.

- [ ] **Step 3: Implement `ListDLQ`, `Requeue`, and the `Job` DTO** — add to `internal/client/client.go`, and add `"strconv"` to the import block:

```go
// Job mirrors the API's job view (payload and created_at are strings as the
// server renders them).
type Job struct {
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

// ListDLQ returns up to limit dead-lettered jobs for a queue, starting at offset.
func (c *Client) ListDLQ(ctx context.Context, queue string, limit, offset int) ([]Job, error) {
	path := "/api/queues/" + url.PathEscape(queue) + "/dlq?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	var jobs []Job
	if err := c.do(ctx, http.MethodGet, path, nil, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// Requeue moves a dead-lettered job back to ready. An id not present in the DLQ
// yields ErrNotFound.
func (c *Client) Requeue(ctx context.Context, queue, id string) error {
	path := "/api/queues/" + url.PathEscape(queue) + "/dlq/" + url.PathEscape(id) + "/requeue"
	err := c.do(ctx, http.MethodPost, path, nil, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
		return ErrNotFound
	}
	return err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/client/ -run 'TestListDLQ|TestRequeue' -v` → PASS. Then the full hermetic suite `go test ./internal/client/`, `gofmt -l internal/client/`, `go build ./...`, `go vet ./internal/client/` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/client/client.go internal/client/client_test.go
git commit -m "Add client ListDLQ and Requeue"
```

---

## Task 4: Wire-compat round-trip test (real Redis, DB 11)

**Files:** Create `internal/client/roundtrip_test.go`

- [ ] **Step 1: Write the test**

```go
package client_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/api"
	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/client"
	"github.com/StrangeNoob/relay/internal/job"
)

// clientTestRedisDB is this package's dedicated Redis DB. broker tests use 15,
// worker 14, metrics 13, api 12; client claims 11 so `go test ./...` never
// collides.
const clientTestRedisDB = 11

// newRoundTrip stands up a real broker + API behind an httptest server and a
// client pointed at it. Skips when Redis is unreachable.
func newRoundTrip(t *testing.T) (*client.Client, *broker.Broker, *redis.Client) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: clientTestRedisDB})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	b := broker.New(rdb)
	srv := httptest.NewServer(api.New(b, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		srv.Close()
		_ = rdb.Close()
	})
	return client.New(srv.URL), b, rdb
}

func TestRoundTripEnqueueStatsQueues(t *testing.T) {
	c, _, _ := newRoundTrip(t)
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, "emails", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	s, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Ready != 1 {
		t.Errorf("ready = %d, want 1", s.Ready)
	}
	qs, err := c.Queues(ctx)
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(qs) != 1 || qs[0] != "emails" {
		t.Errorf("queues = %v, want [emails]", qs)
	}
}

func TestRoundTripDLQListAndRequeue(t *testing.T) {
	c, b, _ := newRoundTrip(t)
	ctx := context.Background()

	// Drive one job to the DLQ via the broker (enqueue maxRetries=0 -> claim -> nack).
	j := job.New("emails", []byte("dead"))
	j.MaxRetries = 0
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	jobs, err := c.ListDLQ(ctx, "emails", 50, 0)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != claimed.ID || jobs[0].State != "dead" {
		t.Fatalf("dlq jobs = %+v", jobs)
	}

	if err := c.Requeue(ctx, "emails", claimed.ID); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	s, err := c.Stats(ctx, "emails")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.DLQ != 0 || s.Ready != 1 {
		t.Errorf("after requeue stats = %+v, want dlq 0 ready 1", s)
	}

	// Requeue of an unknown id -> ErrNotFound.
	if err := c.Requeue(ctx, "emails", "nope"); err == nil {
		t.Error("Requeue unknown id: want error")
	}
}
```

- [ ] **Step 2: Run to verify it passes**

Run: `go test ./internal/client/ -run TestRoundTrip -v`
Expected: PASS (2 tests) when Redis is up; SKIP otherwise. This is a GREEN-only task (it exercises code from Tasks 1–3 against the real server). If it FAILS on a field mismatch, the client DTOs disagree with the server JSON — fix the DTO, not the test.

- [ ] **Step 3: Full client suite under race**

Run: `go test -race ./internal/client/`
Expected: PASS (hermetic + round-trip).

- [ ] **Step 4: Commit**

```bash
git add internal/client/roundtrip_test.go
git commit -m "Add client wire-compat round-trip test against the real API"
```

---

## Task 5: Refactor `cmd/demo` to use the SDK

**Files:** Modify `cmd/demo/main.go`

- [ ] **Step 1: Replace the implementation**

Rewrite `cmd/demo/main.go` to produce through the SDK over HTTP:

```go
// Command demo is a load generator: it enqueues a batch of jobs onto a queue
// through the Relay HTTP API (via internal/client) so a running worker pool has
// something to chew on. It is a pure SDK consumer — it needs cmd/server running.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/StrangeNoob/relay/internal/client"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "Relay server base URL")
	queue := flag.String("queue", "default", "queue to enqueue into")
	count := flag.Int("count", 100, "number of jobs to enqueue")
	delay := flag.Duration("delay", 0, "schedule jobs this far in the future (0 = immediate)")
	priority := flag.Int("priority", 0, "priority for enqueued jobs (higher is more urgent, 0-255)")
	idempotencyKey := flag.String("idempotency-key", "", "idempotency key applied to every enqueued job (empty = none)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()
	c := client.New(*server)

	for i := range *count {
		payload := fmt.Sprintf(`{"n":%d}`, i)
		var opts []client.EnqueueOption
		if *delay > 0 {
			opts = append(opts, client.WithDelay(*delay))
		}
		if *priority != 0 {
			opts = append(opts, client.WithPriority(*priority))
		}
		if *idempotencyKey != "" {
			opts = append(opts, client.WithIdempotencyKey(*idempotencyKey))
		}
		switch _, err := c.Enqueue(ctx, *queue, []byte(payload), opts...); {
		case errors.Is(err, client.ErrDuplicate):
			logger.Info("duplicate dropped", "i", i, "key", *idempotencyKey)
		case err != nil:
			logger.Error("enqueue failed", "i", i, "err", err)
			os.Exit(1)
		}
	}

	logger.Info("enqueued jobs", "count", *count, "queue", *queue, "server", *server)
}
```

(Removes the `broker`/`job`/`redis` imports entirely.)

- [ ] **Step 2: Build and vet**

Run:
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/
```
Expected: all clean.

- [ ] **Step 3: Smoke check (optional; needs server+worker+Redis)**

Run (in separate shells, or skip if not set up):
```bash
go run ./cmd/server -queues demo &        # needs Redis
go run ./cmd/demo -server http://localhost:8080 -queue demo -count 5
curl -s localhost:8080/api/queues/demo/stats; echo   # ready ~5
```
Expected: demo logs "enqueued jobs count=5"; stats shows the jobs. Skip if no local stack.

- [ ] **Step 4: Commit**

```bash
git add cmd/demo/main.go
git commit -m "Switch cmd/demo to enqueue through the client SDK over HTTP"
```

---

## Task 6: Update CLAUDE.md and final verification

**Files:** Modify `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Make these edits (match the file's wording):
1. **Status line** — Phase 3: 3a ✅, 3b ✅, 3c ✅; only 3d (packaging/deploy/README) remains.
2. **"What exists today" list** — add `internal/client` (a stdlib-only HTTP SDK: `Enqueue` with delay/priority/idempotency, plus `Stats`/`ListDLQ`/`Requeue`/`Queues`, typed `ErrDuplicate`/`ErrNotFound`/`APIError`); note `cmd/demo` now produces through the SDK over HTTP (`-server` flag) rather than the broker over Redis.
3. **Layout (✅/◻)** — mark `internal/client/` ✅; update the `cmd/demo` line to note it uses the SDK; leave `deployments/` as ◻.
4. **Build order** — Phase 3: 3a ✅, 3b ✅, 3c ✅; 3d remains.
5. **Known limitations** — add: the client does no retries (one request per call; caller retries); `cmd/demo` now requires a running `cmd/server` (it no longer talks to Redis directly).
6. **Build & dependencies** — note the SDK is stdlib-only (no new dependency); the Go module still depends only on go-redis + prometheus. Update the test-DB note: client tests use DB 11 (broker 15, worker 14, metrics 13, api 12, client 11).
7. **Run commands** — update the demo command to `go run ./cmd/demo -server http://localhost:8080 -queue demo -count 100` and note it needs `cmd/server` running.

- [ ] **Step 2: Full verification**

Run:
```bash
go build ./...
go test -race ./...
go vet ./...
gofmt -l internal/ cmd/
```
Expected: build clean; all tests pass (broker DB 15, worker DB 14, metrics DB 13, api DB 12, client DB 11 — no collisions); vet clean; `gofmt -l` prints nothing. Tests need Redis at localhost:6379.

If anything fails, STOP and report.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Document Phase 3c: producer SDK"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** client core + Enqueue + errors (Task 1); Stats + Queues (Task 2); ListDLQ + Requeue + Job DTO (Task 3); wire-compat round-trip on DB 11 (Task 4); cmd/demo refactor (Task 5); CLAUDE.md + verification (Task 6). Covers every spec section (surface, self-contained DTOs, errors, demo wiring, testing, known limitations).
- **Type consistency:** `EnqueueResult{ID,State}`, `Stats{Ready,Inflight,Delayed,DLQ}`, `Job{...}` JSON tags match the server (`internal/api` jobView/Stats and the enqueue response). `enqueueBody` uses `*int` Priority + omitempty so `WithPriority(0)` sends `priority:0` (mirrors the API). Sentinels `ErrDuplicate`/`ErrNotFound` mapped from 409/404 via `errors.As(*APIError)`. Test DBs: broker 15, worker 14, metrics 13, api 12, client 11.
- **No placeholders:** every step carries complete code. The `strconv` import is deliberately deferred to Task 3 (noted in Task 1) so Task 1 has no unused import.
- **errcheck:** all deferred `Close()` use `defer func() { _ = …Close() }()`; test bodies use `_ = ...Decode()`/`_ = w.Write(...)` where returns are ignored, satisfying the CI linter.
