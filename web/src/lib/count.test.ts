import { describe, it, expect } from "vitest";
import { clampCount } from "./count";

describe("clampCount", () => {
  it("defaults to 1 for empty/non-numeric", () => {
    expect(clampCount("")).toBe(1);
    expect(clampCount("abc")).toBe(1);
  });
  it("floors to an integer", () => {
    expect(clampCount("12.9")).toBe(12);
  });
  it("clamps to [1, max]", () => {
    expect(clampCount("0")).toBe(1);
    expect(clampCount("-5")).toBe(1);
    expect(clampCount("99999")).toBe(10000);
    expect(clampCount("250")).toBe(250);
  });
});
