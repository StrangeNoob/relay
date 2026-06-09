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
