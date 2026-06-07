package broker

// MaxPriority is the largest accepted priority; higher is more urgent. The range
// is bounded so the composite ready score stays an exact float64 integer
// (MaxPriority*priorityScale stays below 2^53).
const MaxPriority = 255

// priorityScale weights priority above ready-time in the composite ready score.
// It exceeds any realistic now-in-milliseconds (~1.7e12 today, ~2.6e12 in 30
// years), so a higher priority always outranks a lower one regardless of time.
const priorityScale = 10_000_000_000_000 // 1e13

// clampPriority bounds p into [0, MaxPriority].
func clampPriority(p int) int {
	if p < 0 {
		return 0
	}
	if p > MaxPriority {
		return MaxPriority
	}
	return p
}

// readyScore is the ZSET score for a job entering the ready set. Priority
// dominates (descending, so ZPOPMAX claims the most urgent first); subtracting
// the readiness time makes the oldest job of a given priority win the tie (FIFO).
// priority must already be clamped to [0, MaxPriority].
func readyScore(priority int, nowMs int64) float64 {
	return float64(priority)*priorityScale - float64(nowMs)
}
