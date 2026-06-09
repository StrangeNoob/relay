import { useCallback, useEffect, useRef, useState } from "react";
import { useStream } from "./hooks/useStream";
import { processedDelta, pushSample } from "./lib/series";
import { listDlq, requeue, type DlqJob } from "./api";
import { Sidebar } from "./components/Sidebar";
import { StatTiles } from "./components/StatTiles";
import { Charts } from "./components/Charts";
import { DlqTable } from "./components/DlqTable";
import { EnqueueForm } from "./components/EnqueueForm";
import { HelpOverlay } from "./components/HelpOverlay";
import { shouldAutoOpen, markSeen } from "./lib/help";

const WINDOW = 60;

export function App() {
  const { byQueue, queues, connected } = useStream();
  const [selected, setSelected] = useState("");
  const [depth, setDepth] = useState<number[]>([]);
  const [throughput, setThroughput] = useState<number[]>([]);
  const [dlq, setDlq] = useState<DlqJob[]>([]);
  const [showEnqueue, setShowEnqueue] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const prevProcessed = useRef<number | null>(null);

  // Auto-open the help overlay on a visitor's first load.
  useEffect(() => {
    if (shouldAutoOpen(localStorage)) setHelpOpen(true);
  }, []);

  // Default the selection to the first queue once queues arrive.
  useEffect(() => {
    if (selected === "" && queues.length > 0) setSelected(queues[0]);
  }, [queues, selected]);

  // Reset the rolling windows when switching queues (defined BEFORE the append
  // effect so on a queue-change render it clears before the new sample is added).
  useEffect(() => {
    setDepth([]);
    setThroughput([]);
    prevProcessed.current = null;
  }, [selected]);

  const snap = selected ? byQueue[selected] : undefined;

  // On each new snapshot for the selected queue, extend the rolling windows.
  useEffect(() => {
    if (!snap) return;
    setDepth((d) => pushSample(d, snap.ready, WINDOW));
    // Throughput is the per-snapshot processed delta (server pushes ~1/s), not a
    // wall-clock rate — see processedDelta: immune to SSE arrival jitter/bunching.
    if (prevProcessed.current !== null) {
      const delta = processedDelta(prevProcessed.current, snap.processed_total);
      setThroughput((t) => pushSample(t, delta, WINDOW));
    }
    prevProcessed.current = snap.processed_total;
  }, [snap]);

  // Load the DLQ for the selected queue, refreshed on a slow timer.
  useEffect(() => {
    if (!selected) return;
    let alive = true;
    const refresh = () => {
      listDlq(selected).then((j) => { if (alive) setDlq(j); }).catch(() => { if (alive) setDlq([]); });
    };
    refresh();
    const id = setInterval(refresh, 5000);
    return () => { alive = false; clearInterval(id); };
  }, [selected]);

  const onRequeue = async (id: string) => {
    if (!selected) return;
    await requeue(selected, id);
    listDlq(selected).then(setDlq).catch(() => {});
  };

  const onEnqueued = () => {
    if (selected) listDlq(selected).then(setDlq).catch(() => {});
  };

  const closeHelp = useCallback(() => {
    markSeen(localStorage);
    setHelpOpen(false);
  }, []);

  return (
    <div className="app">
      <Sidebar
        queues={queues}
        byQueue={byQueue}
        selected={selected}
        connected={connected}
        onSelect={setSelected}
        onEnqueueClick={() => setShowEnqueue(true)}
        onHelpClick={() => setHelpOpen(true)}
      />
      <main className="main">
        <div className="crumb">queue</div>
        <div className="h1">{selected || "—"}</div>
        <div className="updated">{connected ? "live · auto every 1s" : "reconnecting…"}</div>
        <div className="hr" />
        <StatTiles snap={snap} />
        <Charts depth={depth} throughput={throughput} />
        <DlqTable jobs={dlq} onRequeue={onRequeue} />
      </main>
      {showEnqueue && selected && (
        <EnqueueForm queue={selected} onClose={() => setShowEnqueue(false)} onEnqueued={onEnqueued} />
      )}
      <HelpOverlay open={helpOpen} onClose={closeHelp} />
    </div>
  );
}
