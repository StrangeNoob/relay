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
