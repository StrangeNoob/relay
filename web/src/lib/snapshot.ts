// QueueSnapshot is one queue's line in an /api/stream event (matches the Go
// queueSnapshot JSON).
export interface QueueSnapshot {
  queue: string;
  ready: number;
  inflight: number;
  delayed: number;
  dlq: number;
  processed_total: number;
  dead_total: number;
}

// indexByQueue turns a snapshot array into a name->snapshot map.
export function indexByQueue(snaps: QueueSnapshot[]): Record<string, QueueSnapshot> {
  const out: Record<string, QueueSnapshot> = {};
  for (const s of snaps) out[s.queue] = s;
  return out;
}
