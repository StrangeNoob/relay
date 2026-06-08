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
