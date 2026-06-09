import { describe, it, expect } from "vitest";
import { shouldAutoOpen, markSeen, HELP_SEEN_KEY } from "./help";

function fakeStorage(initial: Record<string, string> = {}) {
  const m = new Map(Object.entries(initial));
  return {
    store: m,
    getItem: (k: string) => (m.has(k) ? m.get(k)! : null),
    setItem: (k: string, v: string) => void m.set(k, v),
  };
}

describe("shouldAutoOpen", () => {
  it("is true when the seen key is absent", () => {
    expect(shouldAutoOpen(fakeStorage())).toBe(true);
  });
  it("is false when the seen key is set", () => {
    expect(shouldAutoOpen(fakeStorage({ [HELP_SEEN_KEY]: "1" }))).toBe(false);
  });
  it("defaults to true when getItem throws (e.g. privacy mode)", () => {
    const throwing = { getItem: () => { throw new Error("blocked"); } };
    expect(shouldAutoOpen(throwing)).toBe(true);
  });
});

describe("markSeen", () => {
  it("writes the seen flag", () => {
    const s = fakeStorage();
    markSeen(s);
    expect(s.getItem(HELP_SEEN_KEY)).toBe("1");
  });
  it("swallows a throwing setItem", () => {
    const throwing = { setItem: () => { throw new Error("blocked"); } };
    expect(() => markSeen(throwing)).not.toThrow();
  });
});
