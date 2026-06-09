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
