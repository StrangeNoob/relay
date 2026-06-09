import { describe, it, expect } from "vitest";
import { processedDelta, pushSample } from "./series";

describe("processedDelta", () => {
  it("returns the per-snapshot delta", () => {
    expect(processedDelta(100, 130)).toBe(30);
  });
  it("is zero when unchanged (idle tick)", () => {
    expect(processedDelta(130, 130)).toBe(0);
  });
  it("never returns negative (counter reset / flush)", () => {
    expect(processedDelta(100, 5)).toBe(0);
  });
  it("ignores wall-clock timing — bunched arrivals still report the true delta", () => {
    // The whole point of the fix: no time term, so two snapshots arriving 1ms
    // apart (backgrounded tab / reconnect flush) report their real delta, not a
    // huge spike from dividing by a near-zero interval.
    expect(processedDelta(1000, 1028)).toBe(28);
  });
});

describe("pushSample", () => {
  it("appends and caps the window length", () => {
    let s: number[] = [];
    for (let i = 0; i < 5; i++) s = pushSample(s, i, 3);
    expect(s).toEqual([2, 3, 4]);
  });
});
