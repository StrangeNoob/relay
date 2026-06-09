import { useState, type FormEvent } from "react";
import { enqueue, enqueueBulk, type EnqueueRequest, type BulkEnqueueRequest } from "../api";
import { clampCount } from "../lib/count";

interface EnqueueFormProps {
  queue: string;
  onClose: () => void;
  onEnqueued: () => void;
}

export function EnqueueForm({ queue, onClose, onEnqueued }: EnqueueFormProps) {
  const [payload, setPayload] = useState('{"hello":"world"}');
  const [priority, setPriority] = useState("");
  const [delayMs, setDelayMs] = useState("");
  const [key, setKey] = useState("");
  const [count, setCount] = useState("1");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setErr("");
    const n = clampCount(count);
    try {
      if (n > 1) {
        const body: BulkEnqueueRequest = { count: n, payload };
        if (priority.trim() !== "") body.priority = Number(priority);
        if (delayMs.trim() !== "") body.delay_ms = Number(delayMs);
        await enqueueBulk(queue, body);
      } else {
        const body: EnqueueRequest = { payload };
        if (priority.trim() !== "") body.priority = Number(priority);
        if (delayMs.trim() !== "") body.delay_ms = Number(delayMs);
        if (key.trim() !== "") body.idempotency_key = key.trim();
        await enqueue(queue, body);
      }
      onEnqueued();
      onClose();
    } catch (e2) {
      setErr(String(e2));
      setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <form className="modal" onClick={(e) => e.stopPropagation()} onSubmit={submit}>
        <h3>Enqueue to <span className="modal-q">{queue}</span></h3>
        <label>Payload<textarea value={payload} onChange={(e) => setPayload(e.target.value)} rows={3} /></label>
        <div className="modal-row">
          <label>Count<input value={count} onChange={(e) => setCount(e.target.value)} placeholder="1" inputMode="numeric" /></label>
          <label>Priority<input value={priority} onChange={(e) => setPriority(e.target.value)} placeholder="0" inputMode="numeric" /></label>
          <label>Delay (ms)<input value={delayMs} onChange={(e) => setDelayMs(e.target.value)} placeholder="0" inputMode="numeric" /></label>
        </div>
        <label>Idempotency key<input value={key} onChange={(e) => setKey(e.target.value)} placeholder="(optional; single only)" /></label>
        {err && <div className="modal-err">{err}</div>}
        <div className="modal-actions">
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
          <button type="submit" className="btn-accent" disabled={busy}>{busy ? "Enqueuing…" : "Enqueue"}</button>
        </div>
      </form>
    </div>
  );
}
