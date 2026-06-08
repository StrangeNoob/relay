import { type QueueSnapshot } from "../lib/snapshot";
import { formatCount } from "../lib/format";

interface StatTilesProps { snap?: QueueSnapshot; }

const tiles = [
  { key: "ready", label: "Ready", desc: "claimable now", alert: false },
  { key: "inflight", label: "In-flight", desc: "being processed", alert: false },
  { key: "delayed", label: "Delayed", desc: "scheduled / backoff", alert: false },
  { key: "dlq", label: "Dead-letter", desc: "needs attention", alert: true },
] as const;

export function StatTiles({ snap }: StatTilesProps) {
  return (
    <div className="tiles">
      {tiles.map((t) => (
        <div key={t.key} className={"tile" + (t.alert ? " alert" : "")}>
          <div className="k">{t.label}</div>
          <div className="v">{formatCount(snap ? snap[t.key] : 0)}</div>
          <div className="d">{t.desc}</div>
        </div>
      ))}
    </div>
  );
}
