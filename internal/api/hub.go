package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
)

// snapshotSource is the slice of the broker the hub needs to build an SSE
// snapshot. *broker.Broker satisfies it; tests inject a fake so the hub's
// fan-out and lifecycle logic run without a real Redis.
type snapshotSource interface {
	Queues(ctx context.Context) ([]string, error)
	Stats(ctx context.Context, queue string) (broker.Stats, error)
	Counters(ctx context.Context, queue string) (broker.Counters, error)
}

// subscriber is one connected SSE client. ch is buffered with capacity 1 and
// written latest-wins (see send): a slow client only ever holds the newest
// snapshot and never blocks the poller.
type subscriber struct {
	ch chan []byte
}

// hub fans out one Redis poll to every connected SSE subscriber. A single
// background goroutine polls once per interval while at least one subscriber is
// connected (lazy: starts on the first subscribe, stops on the last
// unsubscribe), so Redis load is O(queues)/sec regardless of connection count.
type hub struct {
	src      snapshotSource
	logger   *slog.Logger
	interval time.Duration

	mu     sync.Mutex
	subs   map[*subscriber]struct{}
	last   []byte             // most-recent marshalled snapshot, for instant populate
	cancel context.CancelFunc // non-nil iff the poller goroutine is running
}

// newHub builds an idle hub. It does not poll until the first subscribe.
func newHub(src snapshotSource, logger *slog.Logger, interval time.Duration) *hub {
	return &hub{
		src:      src,
		logger:   logger,
		interval: interval,
		subs:     make(map[*subscriber]struct{}),
	}
}

// subscribe registers a new SSE client, lazily starting the poller when the hub
// was idle. It seeds the new subscriber with the last snapshot (if any) so a
// late joiner's UI populates without waiting for the next tick.
func (h *hub) subscribe() *subscriber {
	s := &subscriber{ch: make(chan []byte, 1)}
	h.mu.Lock()
	if len(h.subs) == 0 {
		ctx, cancel := context.WithCancel(context.Background())
		h.cancel = cancel
		go h.run(ctx)
	}
	h.subs[s] = struct{}{}
	last := h.last
	h.mu.Unlock()

	// Seed from cache without blocking. If the poller already delivered a fresher
	// snapshot between the unlock and here, the channel is full and we skip —
	// never replace fresh with stale, never block.
	if last != nil {
		select {
		case s.ch <- last:
		default:
		}
	}
	return s
}

// unsubscribe removes a client. When the last subscriber leaves, the poller is
// cancelled so an idle server does no Redis work.
func (h *hub) unsubscribe(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, s)
	if len(h.subs) == 0 && h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

// run is the single poller goroutine: an immediate snapshot, then one per
// interval until ctx is cancelled (by the last unsubscribe or server shutdown).
func (h *hub) run(ctx context.Context) {
	h.pollAndBroadcast(ctx)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.pollAndBroadcast(ctx)
		}
	}
}

// pollAndBroadcast reads every queue's depths and counters once, marshals the
// snapshot, caches it, and fans it out. A Redis or marshal error skips this
// tick (the poller and the cached snapshot survive).
func (h *hub) pollAndBroadcast(ctx context.Context) {
	queues, err := h.src.Queues(ctx)
	if err != nil {
		h.logger.Error("api: stream listing queues", "err", err)
		return
	}
	snaps := make([]queueSnapshot, 0, len(queues))
	for _, q := range queues {
		st, err := h.src.Stats(ctx, q)
		if err != nil {
			h.logger.Error("api: stream stats", "queue", q, "err", err)
			continue
		}
		ct, err := h.src.Counters(ctx, q)
		if err != nil {
			h.logger.Error("api: stream counters", "queue", q, "err", err)
			continue
		}
		snaps = append(snaps, queueSnapshot{
			Queue: q, Ready: st.Ready, Inflight: st.Inflight, Delayed: st.Delayed,
			DLQ: st.DLQ, ProcessedTotal: ct.Processed, DeadTotal: ct.Dead,
		})
	}
	buf, err := json.Marshal(snaps)
	if err != nil {
		h.logger.Error("api: stream marshal", "err", err)
		return
	}
	h.mu.Lock()
	h.last = buf
	for s := range h.subs {
		send(s, buf)
	}
	h.mu.Unlock()
}

// send pushes buf to s latest-wins: if the buffer already holds a stale
// snapshot, drop it and enqueue the newest. Never blocks the caller.
func send(s *subscriber, buf []byte) {
	select {
	case s.ch <- buf:
	default:
		select {
		case <-s.ch:
		default:
		}
		select {
		case s.ch <- buf:
		default:
		}
	}
}
