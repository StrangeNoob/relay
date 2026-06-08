// A timestamped cumulative-counter sample.
export interface Sample {
  value: number;
  t: number; // unix ms
}

// ratePerSecond returns the per-second delta between two cumulative samples.
// Non-positive intervals and counter resets (decreases) yield 0, never negative.
export function ratePerSecond(prev: Sample, cur: Sample): number {
  const dt = (cur.t - prev.t) / 1000;
  if (dt <= 0) return 0;
  const dv = cur.value - prev.value;
  if (dv < 0) return 0;
  return dv / dt;
}

// pushSample appends v to a rolling window, keeping at most `cap` newest values.
export function pushSample(window: number[], v: number, cap: number): number[] {
  const next = [...window, v];
  return next.length > cap ? next.slice(next.length - cap) : next;
}
