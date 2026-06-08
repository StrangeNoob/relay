import { describe, it, expect } from "vitest";
import { ratePerSecond, pushSample } from "./series";

describe("ratePerSecond", () => {
  it("computes delta over elapsed seconds", () => {
    const prev = { value: 100, t: 1000 };
    const cur = { value: 130, t: 4000 }; // +30 over 3s
    expect(ratePerSecond(prev, cur)).toBe(10);
  });
  it("returns 0 for a non-positive interval", () => {
    expect(ratePerSecond({ value: 1, t: 5 }, { value: 9, t: 5 })).toBe(0);
  });
  it("never returns negative (counter reset / flush)", () => {
    expect(ratePerSecond({ value: 100, t: 0 }, { value: 5, t: 1000 })).toBe(0);
  });
});

describe("pushSample", () => {
  it("appends and caps the window length", () => {
    let s: number[] = [];
    for (let i = 0; i < 5; i++) s = pushSample(s, i, 3);
    expect(s).toEqual([2, 3, 4]);
  });
});
