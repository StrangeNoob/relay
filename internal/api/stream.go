package api

import (
	"fmt"
	"net/http"
	"time"
)

// streamInterval is how often the SSE hub polls Redis and pushes a fresh snapshot.
const streamInterval = time.Second

// queueSnapshot is one queue's line in an SSE snapshot: point-in-time depths plus
// the cumulative counters the client diffs into throughput.
type queueSnapshot struct {
	Queue          string `json:"queue"`
	Ready          int64  `json:"ready"`
	Inflight       int64  `json:"inflight"`
	Delayed        int64  `json:"delayed"`
	DLQ            int64  `json:"dlq"`
	ProcessedTotal int64  `json:"processed_total"`
	DeadTotal      int64  `json:"dead_total"`
}

// stream handles GET /api/stream: a text/event-stream that subscribes to the
// shared hub and relays each broadcast snapshot to this client until it
// disconnects. All Redis polling happens once in the hub, not per connection.
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		a.writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub := a.hub.subscribe()
	defer a.hub.unsubscribe(sub)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case buf := <-sub.ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", buf); err != nil {
				return // client disconnected
			}
			flusher.Flush()
		}
	}
}
