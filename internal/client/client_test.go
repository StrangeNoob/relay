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

	nf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"job not found in dlq"}`))
	}))
	defer nf.Close()
	if err := client.New(nf.URL).Requeue(context.Background(), "emails", "nope"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

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
