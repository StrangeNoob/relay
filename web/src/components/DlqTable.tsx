import { type DlqJob } from "../api";
import { formatAge } from "../lib/format";

interface DlqTableProps { jobs: DlqJob[]; onRequeue: (id: string) => void; }

function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 4) + "…" + id.slice(-4) : id;
}
function preview(payload: string): string {
  return payload.length > 48 ? payload.slice(0, 48) + "…" : payload;
}

export function DlqTable({ jobs, onRequeue }: DlqTableProps) {
  return (
    <div className="dlq">
      <div className="cap">
        <h3>Dead-letter queue</h3>
        <span className="sub">{jobs.length} job{jobs.length === 1 ? "" : "s"} · exhausted retries</span>
      </div>
      <table>
        <thead><tr><th>Job ID</th><th>Attempts</th><th>Payload</th><th>Age</th><th></th></tr></thead>
        <tbody>
          {jobs.length === 0 && (<tr><td colSpan={5} className="empty">No dead-lettered jobs</td></tr>)}
          {jobs.map((j) => (
            <tr key={j.id}>
              <td className="id">{shortId(j.id)}</td>
              <td className="att">{j.attempts}/{j.max_retries}</td>
              <td>{preview(j.payload)}</td>
              <td className="age">{formatAge(Date.now() - Date.parse(j.created_at))}</td>
              <td><button className="requeue" onClick={() => onRequeue(j.id)}>Requeue</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
