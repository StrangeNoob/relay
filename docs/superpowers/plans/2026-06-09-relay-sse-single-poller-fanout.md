# SSE Single-Poller Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-connection SSE polling with one in-process hub so Redis load is O(queues)/sec regardless of how many dashboards are connected.

**Architecture:** A new `hub` type owns a single background poller that, while ≥1 dashboard is connected, reads every queue's depths+counters once per interval, marshals one snapshot, and fans it out to all subscribers latest-wins. The `stream` handler shrinks to subscribe → relay → unsubscribe. The hub depends on a `snapshotSource` interface (satisfied by `*broker.Broker`) so its fan-out and lifecycle are unit-tested without Redis. Wire format is unchanged, so the committed `web/dist` client is untouched.

**Tech Stack:** Go 1.24 (stdlib `net/http` SSE, `sync.Mutex`, `time.Ticker`, `context`), `go-redis` (only behind the broker), `log/slog`. Tests: stdlib `testing`, run under `-race`.

**Spec:** `docs/superpowers/specs/2026-06-09-relay-sse-single-poller-fanout-design.md`

---

## File Structure

- **Create `internal/api/hub.go`** — the `snapshotSource` interface, `subscriber`, `hub`, and all hub methods (`newHub`, `subscribe`, `unsubscribe`, `run`, `pollAndBroadcast`, `send`). Owns all Redis polling + broadcast. This is where the snapshot-building logic (currently in `stream.go`'s `writeSnapshot`) moves to.
- **Create `internal/api/hub_test.go`** — white-box (`package api`) unit tests with a fake `snapshotSource`; no Redis. Covers lazy start/stop, fan-out, latest-wins, late-joiner cache, and poll-error survival.
- **Modify `internal/api/api.go`** — add a `hub *hub` field to `API`; construct it in `New`.
- **Modify `internal/api/stream.go`** — keep the `streamInterval` const and `queueSnapshot` struct; replace the handler with subscribe/relay/unsubscribe; delete `writeSnapshot`.
- **Modify `CLAUDE.md`** — rewrite the "SSE is per-connection" limitation entry to describe the single-poller fan-out.

The existing real-Redis integration test `TestStreamEmitsSnapshot` (`internal/api/api_test.go:275`) must remain green unchanged — the hub's immediate first poll preserves instant populate.

---

## Task 1: Build the fan-out hub (unit-tested, no Redis)

**Files:**
- Create: `internal/api/hub.go`
- Test: `internal/api/hub_test.go`

This task produces a fully-tested hub that does not yet touch the HTTP handler. It compiles against the existing `queueSnapshot` struct in `stream.go` (same package) and `broker.Stats`/`broker.Counters`.

### Reference: existing types the hub reuses

`queueSnapshot` (already defined in `internal/api/stream.go`, do NOT redefine):

```go
type queueSnapshot struct {
	Queue          string `json:"queue"`
	Ready          int64  `json:"ready"`
	Inflight       int64  `json:"inflight"`
	Delayed        int64  `json:"delayed"`
	DLQ            int64  `json:"dlq"`
	ProcessedTotal int64  `json:"processed_total"`
	DeadTotal      int64  `json:"dead_total"`
}
```

Broker types (`internal/broker/broker.go:428,456`): `broker.Stats{Ready, Inflight, Delayed, DLQ int64}` and `broker.Counters{Processed, Dead int64}`.

- [ ] **Step 1: Write `internal/api/hub.go`**

Create the file with the complete implementation:

```go
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
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/api/`
Expected: success (no output). If `queueSnapshot` is reported undefined, confirm it still exists in `stream.go` — do not redefine it here.

- [ ] **Step 3: Write `internal/api/hub_test.go` (failing tests)**

Create the file. It is white-box (`package api`) to reach the unexported hub. It defines a fake source that signals each poll over a buffered channel, enabling deterministic, sleep-light, `-race`-clean synchronization.

```go
package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/StrangeNoob/relay/internal/broker"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSource is an in-memory snapshotSource. Every Queues call bumps a counter
// and emits a signal on polled (buffered, non-blocking) so tests can observe
// poll cadence without the source ever blocking the poller.
type fakeSource struct {
	mu      sync.Mutex
	queues  []string
	failErr error
	polled  chan struct{}
}

func newFakeSource(queues ...string) *fakeSource {
	return &fakeSource{queues: queues, polled: make(chan struct{}, 1024)}
}

func (f *fakeSource) setErr(err error) {
	f.mu.Lock()
	f.failErr = err
	f.mu.Unlock()
}

func (f *fakeSource) Queues(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	err := f.failErr
	qs := f.queues
	f.mu.Unlock()
	select {
	case f.polled <- struct{}{}:
	default:
	}
	if err != nil {
		return nil, err
	}
	return qs, nil
}

func (f *fakeSource) Stats(ctx context.Context, queue string) (broker.Stats, error) {
	return broker.Stats{Ready: 1}, nil
}

func (f *fakeSource) Counters(ctx context.Context, queue string) (broker.Counters, error) {
	return broker.Counters{Processed: 7}, nil
}

// waitForPoll blocks until the next poll signal or fails the test on timeout.
func waitForPoll(t *testing.T, polled <-chan struct{}) {
	t.Helper()
	select {
	case <-polled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a poll")
	}
}

// assertNoPoll fails if any poll happens within d.
func assertNoPoll(t *testing.T, polled <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-polled:
		t.Fatal("unexpected poll")
	case <-time.After(d):
	}
}

// drain removes any buffered poll signals.
func drain(polled <-chan struct{}) {
	for {
		select {
		case <-polled:
		default:
			return
		}
	}
}

func TestHubLazyStartsOnFirstSubscribe(t *testing.T) {
	f := newFakeSource("emails")
	h := newHub(f, discardLogger(), 20*time.Millisecond)
	assertNoPoll(t, f.polled, 80*time.Millisecond) // idle hub does not poll
	sub := h.subscribe()
	defer h.unsubscribe(sub)
	waitForPoll(t, f.polled) // first subscribe starts the poller
}

func TestHubStopsPollingAfterLastUnsubscribe(t *testing.T) {
	f := newFakeSource("emails")
	h := newHub(f, discardLogger(), 20*time.Millisecond)
	sub := h.subscribe()
	waitForPoll(t, f.polled)
	h.unsubscribe(sub)
	time.Sleep(60 * time.Millisecond) // let the poller observe cancellation and exit
	drain(f.polled)                   // clear the straggler + any buffered signals
	assertNoPoll(t, f.polled, 100*time.Millisecond)
}

func TestHubFansOutToAllSubscribers(t *testing.T) {
	f := newFakeSource("emails")
	h := newHub(f, discardLogger(), 20*time.Millisecond)
	a := h.subscribe()
	defer h.unsubscribe(a)
	b := h.subscribe()
	defer h.unsubscribe(b)
	c := h.subscribe()
	defer h.unsubscribe(c)
	for i, s := range []*subscriber{a, b, c} {
		select {
		case buf := <-s.ch:
			if !bytes.Contains(buf, []byte(`"queue":"emails"`)) {
				t.Fatalf("sub %d: snapshot %q missing queue", i, buf)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sub %d: no snapshot received", i)
		}
	}
}

func TestHubSlowConsumerDoesNotBlockPoller(t *testing.T) {
	f := newFakeSource("emails")
	h := newHub(f, discardLogger(), 10*time.Millisecond)
	slow := h.subscribe() // never reads slow.ch
	defer h.unsubscribe(slow)
	// The poller keeps polling across several ticks even though slow never drains.
	waitForPoll(t, f.polled)
	drain(f.polled)
	waitForPoll(t, f.polled)
	waitForPoll(t, f.polled)
	// Latest-wins: cap-1 channel holds exactly one (the newest) snapshot.
	if got := len(slow.ch); got != 1 {
		t.Fatalf("slow.ch len = %d, want 1 (latest-wins, cap 1)", got)
	}
}

func TestHubLateJoinerGetsCachedSnapshot(t *testing.T) {
	f := newFakeSource("emails")
	// Huge interval: if a late joiner had to wait for a tick it would time out;
	// getting a snapshot promptly proves it came from the cache.
	h := newHub(f, discardLogger(), 10*time.Second)
	first := h.subscribe()
	defer h.unsubscribe(first)
	<-first.ch // the immediate first poll has now produced and cached a snapshot
	late := h.subscribe()
	defer h.unsubscribe(late)
	select {
	case buf := <-late.ch:
		if !bytes.Contains(buf, []byte(`"queue":"emails"`)) {
			t.Fatalf("late joiner got %q", buf)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late joiner did not receive the cached snapshot")
	}
}

func TestHubSurvivesPollError(t *testing.T) {
	f := newFakeSource("emails")
	f.setErr(errors.New("redis down"))
	h := newHub(f, discardLogger(), 20*time.Millisecond)
	sub := h.subscribe()
	defer h.unsubscribe(sub)
	waitForPoll(t, f.polled) // errored poll still ran
	drain(f.polled)
	waitForPoll(t, f.polled) // poller survived the error and polled again
	f.setErr(nil)            // recover
	select {
	case <-sub.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot received after recovery")
	}
}
```

- [ ] **Step 4: Run the tests, expect PASS**

Run: `go test -race ./internal/api/ -run TestHub -v`
Expected: all six `TestHub*` tests PASS. (These are pure in-memory tests; they run even without Redis.)

- [ ] **Step 5: Commit**

```bash
git add internal/api/hub.go internal/api/hub_test.go
git commit -m "Add SSE fan-out hub with lazy single poller"
```

---

## Task 2: Wire the hub into the API and simplify the stream handler

**Files:**
- Modify: `internal/api/api.go:19-41` (API struct + New)
- Modify: `internal/api/stream.go` (handler body; delete writeSnapshot)
- Test: `internal/api/api_test.go:275` (existing `TestStreamEmitsSnapshot`, real Redis — must stay green)

- [ ] **Step 1: Add the hub field and construct it in `New`**

In `internal/api/api.go`, change the `API` struct (currently lines 19-22) to add the field:

```go
// API holds the dependencies shared by the handlers.
type API struct {
	broker *broker.Broker
	logger *slog.Logger
	hub    *hub
}
```

And in `New` (currently line 31), construct the hub after building `a`. The concrete `*broker.Broker` satisfies `snapshotSource`; `streamInterval` is defined in `stream.go` (same package):

```go
	a := &API{broker: b, logger: logger}
	a.hub = newHub(b, logger, streamInterval)
```

Leave the rest of `New` (the mux and route registrations) unchanged.

- [ ] **Step 2: Replace the stream handler and delete writeSnapshot**

In `internal/api/stream.go`, keep the `streamInterval` const (line 12) and the `queueSnapshot` struct (lines 16-24). Replace the `stream` method and DELETE the entire `writeSnapshot` method. The file becomes:

```go
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
```

Note the import block dropped `context` and `encoding/json` (now used only in `hub.go`) and kept `fmt`, `net/http`, `time`.

- [ ] **Step 3: Verify the package builds and vets clean**

Run: `go build ./... && go vet ./internal/api/`
Expected: success, no output. (Confirms no leftover references to `writeSnapshot` and no unused imports.)

- [ ] **Step 4: Run the full api test suite (needs Redis on :6379)**

Run: `go test -race ./internal/api/ -v`
Expected: all tests PASS, including `TestHub*` (Task 1) and the real-Redis `TestStreamEmitsSnapshot` — the latter still receives its first snapshot because the hub polls immediately on the first subscribe.
Note: if Redis is not running, the real-Redis tests SKIP (not fail) and the `TestHub*` tests still PASS. To exercise everything, ensure Redis is reachable (e.g., `docker run -p 6379:6379 redis:7`).

- [ ] **Step 5: Commit**

```bash
git add internal/api/api.go internal/api/stream.go
git commit -m "Route /api/stream through the fan-out hub"
```

---

## Task 3: Update CLAUDE.md limitation note

**Files:**
- Modify: `CLAUDE.md` (the "SSE is per-connection" bullet under "Known limitations", currently line 119)

- [ ] **Step 1: Rewrite the SSE limitation entry**

In `CLAUDE.md`, replace this line:

```
- **SSE is per-connection.** Each open dashboard tab runs its own server-side ticker goroutine reading Redis every ~1 s. This is fine for a demo; a production deployment would fan-out from a single poller.
```

with:

```
- **SSE fan-out is per-process, single-poller.** While ≥1 dashboard is connected, one background goroutine per server process polls Redis every ~1 s, builds the snapshot, and broadcasts it to every connected dashboard (latest-wins per client, so a slow tab never blocks the poller; a late joiner is seeded from the last cached snapshot). Redis load is O(queues)/sec, independent of connection count; an idle server (no dashboards) does no polling. The poller is owned by the `hub` in `internal/api/hub.go` and is lazily started/stopped by subscriber count. Each server replica runs its own poller — there is no cross-replica fan-out (Redis Pub/Sub remains future work).
```

- [ ] **Step 2: Verify the edit**

Run: `grep -n "SSE fan-out is per-process" CLAUDE.md`
Expected: one matching line.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Document single-poller SSE fan-out in CLAUDE.md"
```

---

## Final verification (after all tasks)

- [ ] `go build ./...` — succeeds.
- [ ] `go test -race ./...` — all packages pass (Redis-backed suites need Redis on :6379, else they skip).
- [ ] `golangci-lint run` — clean (note the project's errcheck convention: any `defer x.Close()` must be `defer func() { _ = x.Close() }()`; the hub adds no such defers, but keep it in mind for any incidental change).
- [ ] No `web/` change and no `web/dist` rebuild — confirm `git status` shows nothing under `web/` (wire format is unchanged by design).
- [ ] Grep confirms the old path is gone: `grep -rn "writeSnapshot" internal/` returns nothing.

---

## Self-Review (against the spec)

**Spec coverage:**
- In-process hub + single poller → Task 1 (`hub.go`).
- Lazy lifecycle (start on first subscribe, stop on last unsubscribe, zero idle polling) → Task 1 Step 1 (`subscribe`/`unsubscribe`) + tests `TestHubLazyStartsOnFirstSubscribe`, `TestHubStopsPollingAfterLastUnsubscribe`.
- `snapshotSource` interface for Redis-free unit tests → Task 1.
- Immediate first snapshot / late-joiner cache → `run`'s immediate poll + `subscribe` seeding; tests `TestHubLateJoinerGetsCachedSnapshot`, and integration `TestStreamEmitsSnapshot`.
- Latest-wins, poller never blocked → `send`; test `TestHubSlowConsumerDoesNotBlockPoller`.
- Error handling (skip tick, survive) → `pollAndBroadcast`; test `TestHubSurvivesPollError`.
- Simplified handler + delete `writeSnapshot` → Task 2.
- Wire-format unchanged, no web rebuild → Task 2 (same `data: %s\n\n` + `queueSnapshot`) + Final verification.
- Docs updated → Task 3.
- Benign restart overlap → documented in spec; implementation tolerates it (old goroutine exits on cancelled ctx without touching `h.cancel`). No dedicated test (timing-dependent, structurally safe).

**Placeholder scan:** none — every code step shows full code; every run step shows the command and expected result.

**Type consistency:** `snapshotSource` methods match `*broker.Broker`'s real signatures (`Queues(ctx) ([]string, error)`, `Stats(ctx, string) (broker.Stats, error)`, `Counters(ctx, string) (broker.Counters, error)`). `subscriber.ch` is `chan []byte` everywhere. `hub.last` is `[]byte`. `newHub(src, logger, interval)` call in Task 2 matches its Task 1 definition. `streamInterval`/`queueSnapshot` are defined once (in `stream.go`) and reused by `hub.go`.
