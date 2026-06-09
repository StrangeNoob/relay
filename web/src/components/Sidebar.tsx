import { type QueueSnapshot } from "../lib/snapshot";
import { formatCount } from "../lib/format";

interface SidebarProps {
  queues: string[];
  byQueue: Record<string, QueueSnapshot>;
  selected: string;
  connected: boolean;
  onSelect: (q: string) => void;
  onEnqueueClick: () => void;
  onHelpClick: () => void;
}

export function Sidebar({ queues, byQueue, selected, connected, onSelect, onEnqueueClick, onHelpClick }: SidebarProps) {
  return (
    <aside className="side">
      <div>
        <div className="brand">Relay<span className="dotacc">.</span></div>
        <div className="brand-sub">task&nbsp;queue</div>
      </div>
      <div className="qlabel">Queues</div>
      {queues.length === 0 && <div className="q-empty">no queues yet</div>}
      {queues.map((q) => (
        <div key={q} className={"q" + (q === selected ? " active" : "")} onClick={() => onSelect(q)}>
          <span className="nm">{q}</span>
          <span className="ct">{formatCount(byQueue[q]?.ready ?? 0)}</span>
        </div>
      ))}
      <div className="side-foot">
        <button className="enq" onClick={onEnqueueClick}>+ Enqueue a job</button>
        <div className="conn">
          <span className={"live" + (connected ? "" : " off")} /> {connected ? "live · 1s" : "offline"}
          <button className="help-q" onClick={onHelpClick} aria-label="What is Relay?" title="What is Relay?">?</button>
        </div>
      </div>
    </aside>
  );
}
