package broker

import "testing"

func TestReadyScoreOrdering(t *testing.T) {
	const now = int64(1_700_000_000_000)

	// Higher priority scores higher (ZPOPMAX claims it first).
	if readyScore(5, now) <= readyScore(1, now) {
		t.Error("higher priority should score higher")
	}
	// Within a priority, the older job (smaller now) scores higher → FIFO.
	if readyScore(3, now) <= readyScore(3, now+1000) {
		t.Error("older job should score higher within a priority")
	}
	// Priority dominates time: priority 1 far in the future still beats priority 0 now.
	if readyScore(1, now+1_000_000_000) <= readyScore(0, now) {
		t.Error("priority must dominate the time tiebreak")
	}
}

func TestClampPriority(t *testing.T) {
	cases := map[int]int{-5: 0, 0: 0, 100: 100, 255: 255, 300: 255}
	for in, want := range cases {
		if got := clampPriority(in); got != want {
			t.Errorf("clampPriority(%d) = %d, want %d", in, got, want)
		}
	}
}
