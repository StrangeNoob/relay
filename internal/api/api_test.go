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
	"github.com/StrangeNoob/relay/internal/job"
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

// mustJob builds a job for tests via the broker's job package.
func mustJob(queue, payload string) job.Job {
	return job.New(queue, []byte(payload))
}

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
