package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// streamInterval is how often the SSE stream pushes a fresh snapshot.
const streamInterval = time.Second

// queueSnapshot is one queue's line in an SSE snapshot: point-in-time depths plus
// the cumulative counters the client rate-computes into throughput.
type queueSnapshot struct {
	Queue          string `json:"queue"`
	Ready          int64  `json:"ready"`
	Inflight       int64  `json:"inflight"`
	Delayed        int64  `json:"delayed"`
	DLQ            int64  `json:"dlq"`
	ProcessedTotal int64  `json:"processed_total"`
	DeadTotal      int64  `json:"dead_total"`
}

// stream handles GET /api/stream: a text/event-stream that pushes a snapshot of
// every queue immediately and then once per streamInterval until the client
// disconnects. A Redis hiccup skips a tick rather than tearing down the stream.
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		a.writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	// Immediate first snapshot so the UI populates without waiting a tick.
	if !a.writeSnapshot(ctx, w, flusher) {
		return
	}
	ticker := time.NewTicker(streamInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !a.writeSnapshot(ctx, w, flusher) {
				return
			}
		}
	}
}

// writeSnapshot composes and writes one SSE event. It returns false when the
// client connection is gone (write failed), signalling the caller to stop.
func (a *API) writeSnapshot(ctx context.Context, w http.ResponseWriter, flusher http.Flusher) bool {
	queues, err := a.broker.Queues(ctx)
	if err != nil {
		a.logger.Error("api: stream listing queues", "err", err)
		return true // skip this tick, keep the stream open
	}
	snaps := make([]queueSnapshot, 0, len(queues))
	for _, q := range queues {
		st, err := a.broker.Stats(ctx, q)
		if err != nil {
			a.logger.Error("api: stream stats", "queue", q, "err", err)
			continue
		}
		ct, err := a.broker.Counters(ctx, q)
		if err != nil {
			a.logger.Error("api: stream counters", "queue", q, "err", err)
			continue
		}
		snaps = append(snaps, queueSnapshot{
			Queue: q, Ready: st.Ready, Inflight: st.Inflight, Delayed: st.Delayed,
			DLQ: st.DLQ, ProcessedTotal: ct.Processed, DeadTotal: ct.Dead,
		})
	}
	buf, err := json.Marshal(snaps)
	if err != nil {
		a.logger.Error("api: stream marshal", "err", err)
		return true
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", buf); err != nil {
		return false // client disconnected
	}
	flusher.Flush()
	return true
}
