import { describe, it, expect } from "vitest";
import { formatCount, formatAge } from "./format";

describe("formatCount", () => {
  it("passes small numbers through", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(942)).toBe("942");
  });
  it("abbreviates thousands and millions", () => {
    expect(formatCount(1240)).toBe("1.2k");
    expect(formatCount(2_500_000)).toBe("2.5M");
  });
});

describe("formatAge", () => {
  it("renders seconds, minutes, hours", () => {
    expect(formatAge(5_000)).toBe("5s");
    expect(formatAge(90_000)).toBe("1m");
    expect(formatAge(3_660_000)).toBe("1h");
  });
});
