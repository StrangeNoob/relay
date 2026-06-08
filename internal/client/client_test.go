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
