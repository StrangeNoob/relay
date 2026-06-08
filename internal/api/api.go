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
