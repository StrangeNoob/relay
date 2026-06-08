import { describe, it, expect } from "vitest";
import { indexByQueue, type QueueSnapshot } from "./snapshot";

const snap = (queue: string, ready: number): QueueSnapshot => ({
  queue, ready, inflight: 0, delayed: 0, dlq: 0, processed_total: 0, dead_total: 0,
});

describe("indexByQueue", () => {
  it("maps a snapshot array by queue name", () => {
    const m = indexByQueue([snap("emails", 2), snap("sms", 5)]);
    expect(m.emails.ready).toBe(2);
    expect(m.sms.ready).toBe(5);
  });
});
