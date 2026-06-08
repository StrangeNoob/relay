import { useEffect, useState } from "react";
import { indexByQueue, type QueueSnapshot } from "../lib/snapshot";

export interface StreamState {
  byQueue: Record<string, QueueSnapshot>;
  queues: string[];
  connected: boolean;
}

// useStream subscribes to /api/stream (SSE) and exposes the latest per-queue
// snapshot. EventSource reconnects automatically.
export function useStream(): StreamState {
  const [state, setState] = useState<StreamState>({ byQueue: {}, queues: [], connected: false });

  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.onopen = () => setState((s) => ({ ...s, connected: true }));
    es.onerror = () => setState((s) => ({ ...s, connected: false }));
    es.onmessage = (e) => {
      const snaps = JSON.parse(e.data as string) as QueueSnapshot[];
      setState({ byQueue: indexByQueue(snaps), queues: snaps.map((s) => s.queue), connected: true });
    };
    return () => es.close();
  }, []);

  return state;
}
