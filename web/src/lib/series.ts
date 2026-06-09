// pushSample appends v to a rolling window, keeping at most `cap` newest values.
export function pushSample(window: number[], v: number, cap: number): number[] {
  const next = [...window, v];
  return next.length > cap ? next.slice(next.length - cap) : next;
}

// processedDelta returns the jobs processed between two consecutive cumulative
// snapshots. The server pushes one snapshot per ~1s, so the delta IS the
// per-second throughput — and unlike a wall-clock rate it is immune to SSE
// arrival jitter/bunching (a backgrounded tab or a reconnect can flush several
// snapshots milliseconds apart; dividing by that near-zero interval would
// produce huge spikes that crush the chart's auto-scale). A counter reset or
// flush (cur < prev) yields 0, never negative.
export function processedDelta(prev: number, cur: number): number {
  return cur >= prev ? cur - prev : 0;
}
