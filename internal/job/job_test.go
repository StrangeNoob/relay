package job_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/StrangeNoob/relay/internal/job"
)

func TestNewSetsDefaults(t *testing.T) {
	before := time.Now()
	j := job.New("emails", []byte(`{"to":"a@b.com"}`))

	if j.Queue != "emails" {
		t.Errorf("Queue = %q, want %q", j.Queue, "emails")
	}
	if string(j.Payload) != `{"to":"a@b.com"}` {
		t.Errorf("Payload = %q, want %q", j.Payload, `{"to":"a@b.com"}`)
	}
	if j.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", j.Attempts)
	}
	if j.ID == "" {
		t.Error("ID is empty, want a generated id")
	}
	if j.CreatedAt.Before(before) {
		t.Errorf("CreatedAt = %v, want >= %v", j.CreatedAt, before)
	}
}

func TestNewStartsPending(t *testing.T) {
	j := job.New("emails", []byte("x"))

	if j.State != job.StatePending {
		t.Errorf("State = %q, want %q", j.State, job.StatePending)
	}
}

func TestNewUsesDefaultMaxRetries(t *testing.T) {
	j := job.New("emails", []byte("x"))

	if j.MaxRetries != job.DefaultMaxRetries {
		t.Errorf("MaxRetries = %d, want %d", j.MaxRetries, job.DefaultMaxRetries)
	}
}

func TestHashRoundTrip(t *testing.T) {
	want := job.New("emails", []byte(`{"to":"a@b.com"}`))
	want.Attempts = 2
	want.Priority = 4
	want.IdempotencyKey = "user-42-welcome"

	got, err := job.FromHash(want.ToHash())
	if err != nil {
		t.Fatalf("FromHash: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %q, want %q", got.ID, want.ID)
	}
	if got.Queue != want.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, want.Queue)
	}
	if got.State != want.State {
		t.Errorf("State = %q, want %q", got.State, want.State)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
	if got.Attempts != want.Attempts {
		t.Errorf("Attempts = %d, want %d", got.Attempts, want.Attempts)
	}
	if got.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", got.MaxRetries, want.MaxRetries)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority = %d, want %d", got.Priority, want.Priority)
	}
	if got.IdempotencyKey != want.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, want.IdempotencyKey)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestNewDefaultsPriorityZero(t *testing.T) {
	j := job.New("emails", []byte("x"))
	if j.Priority != 0 {
		t.Errorf("Priority = %d, want 0", j.Priority)
	}
}

func TestPriorityRoundTrip(t *testing.T) {
	want := job.New("emails", []byte("x"))
	want.Priority = 7

	got, err := job.FromHash(want.ToHash())
	if err != nil {
		t.Fatalf("FromHash: %v", err)
	}
	if got.Priority != 7 {
		t.Errorf("Priority = %d, want 7", got.Priority)
	}
}

func TestFromHashRejectsMalformedPriority(t *testing.T) {
	h := job.New("emails", []byte("x")).ToHash()
	h["priority"] = "not-a-number"
	if _, err := job.FromHash(h); err == nil {
		t.Error("FromHash with malformed priority = nil error, want error")
	}
}

func TestFromHashRejectsMalformedFields(t *testing.T) {
	valid := job.New("emails", []byte("x")).ToHash()

	cases := map[string]func(map[string]string){
		"non-numeric attempts":    func(h map[string]string) { h["attempts"] = "not-a-number" },
		"non-numeric max_retries": func(h map[string]string) { h["max_retries"] = "lots" },
		"unparseable created_at":  func(h map[string]string) { h["created_at"] = "yesterday" },
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			h := make(map[string]string, len(valid))
			for k, v := range valid {
				h[k] = v
			}
			corrupt(h)

			if _, err := job.FromHash(h); err == nil {
				t.Errorf("FromHash(%v) = nil error, want error", h)
			}
		})
	}
}
