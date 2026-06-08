// REST helpers for the Relay API. The dashboard is served by the same origin as
// the API, so all paths are relative.

export interface DlqJob {
  id: string;
  queue: string;
  payload: string;
  state: string;
  attempts: number;
  max_retries: number;
  priority: number;
  created_at: string;
  idempotency_key?: string;
}

export interface EnqueueRequest {
  payload: string;
  delay_ms?: number;
  priority?: number;
  idempotency_key?: string;
}

export async function listDlq(queue: string, limit = 50, offset = 0): Promise<DlqJob[]> {
  const r = await fetch(`/api/queues/${encodeURIComponent(queue)}/dlq?limit=${limit}&offset=${offset}`);
  if (!r.ok) throw new Error(`list dlq: ${r.status}`);
  return r.json() as Promise<DlqJob[]>;
}

export async function requeue(queue: string, id: string): Promise<void> {
  const r = await fetch(`/api/queues/${encodeURIComponent(queue)}/dlq/${encodeURIComponent(id)}/requeue`, {
    method: "POST",
  });
  if (!r.ok) throw new Error(`requeue: ${r.status}`);
}

export async function enqueue(queue: string, body: EnqueueRequest): Promise<{ id: string; state: string }> {
  const r = await fetch(`/api/queues/${encodeURIComponent(queue)}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`enqueue: ${r.status}`);
  return r.json() as Promise<{ id: string; state: string }>;
}
