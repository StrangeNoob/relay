import { useEffect } from "react";

interface HelpOverlayProps {
  open: boolean;
  onClose: () => void;
}

// HelpOverlay is the first-visit explainer modal: what Relay is + a CSS-animated
// job lifecycle (Ready -> In-flight -> done, with retry/Delayed and Dead-letter
// branches). Closing is handled by the parent (which records "seen").
export function HelpOverlay({ open, onClose }: HelpOverlayProps) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="help-backdrop" onClick={onClose}>
      <div
        className="help-modal"
        role="dialog"
        aria-modal="true"
        aria-label="What is Relay?"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="help-head">
          <div className="help-kicker">▚ Relay</div>
          <h2 className="help-title">A task queue, built from scratch on Redis</h2>
          <p className="help-lede">
            Producers enqueue jobs; a pool of workers each <em>atomically claim</em> one, run it,
            and acknowledge. Failures retry with backoff; a crashed worker&rsquo;s job is
            auto-recovered. Here&rsquo;s the lifecycle every job follows:
          </p>
        </div>

        <div className="help-stage">
          <svg className="help-wires" viewBox="0 0 720 248" preserveAspectRatio="none">
            <path className="help-wire" d="M144,117 L286,117" markerEnd="url(#help-a)" />
            <path className="help-wire" d="M406,117 L598,117" markerEnd="url(#help-a)" />
            <path className="help-wire help-wire-retry" d="M346,142 L346,182" markerEnd="url(#help-g)" />
            <path className="help-wire help-wire-retry" d="M286,202 C150,202 92,202 92,144" markerEnd="url(#help-g)" />
            <path className="help-wire help-wire-dead" d="M406,200 L566,200" markerEnd="url(#help-r)" />
            <defs>
              <marker id="help-a" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto">
                <path d="M0,0 L6,3 L0,6 Z" fill="#6f6757" />
              </marker>
              <marker id="help-g" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto">
                <path d="M0,0 L6,3 L0,6 Z" fill="#cbb48e" />
              </marker>
              <marker id="help-r" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto">
                <path d="M0,0 L6,3 L0,6 Z" fill="#d2603f" />
              </marker>
            </defs>
          </svg>

          <div className="help-end help-enq">enqueue ▸</div>
          <div className="help-node help-ready"><span className="help-nm">Ready</span><span className="help-sub">claimable</span></div>
          <div className="help-node help-inflight"><span className="help-nm">In-flight</span><span className="help-sub">claimed · processing</span></div>
          <div className="help-node help-delayed"><span className="help-nm">Delayed</span><span className="help-sub">backoff</span></div>
          <div className="help-node help-dlq"><span className="help-nm">Dead-letter</span><span className="help-sub">requeue-able</span></div>
          <div className="help-end help-done">done ✓</div>
          <div className="help-wlabel help-wl-retry">retry · full-jitter backoff</div>
          <div className="help-wlabel help-wl-dead">retries exhausted</div>

          <div className="help-tok help-h1" />
          <div className="help-tok help-h2" />
          <div className="help-tok help-h3" />
          <div className="help-tok help-rt" />
          <div className="help-tok help-dd" />
        </div>

        <div className="help-caps">
          <div className="help-cap">
            <h4>The atomic claim</h4>
            <p>Workers compete for jobs; a single Redis Lua script guarantees no two ever claim the same one.</p>
          </div>
          <div className="help-cap">
            <h4>Retry with backoff</h4>
            <p>A failed job waits in <i>delayed</i> with full-jitter backoff, then returns to ready — until its budget is spent.</p>
          </div>
          <div className="help-cap">
            <h4>Crash-safe</h4>
            <p>If a worker dies mid-job, its visibility deadline lapses and the reaper requeues it. At-least-once.</p>
          </div>
        </div>

        <div className="help-actions">
          <button type="button" className="help-skip" onClick={onClose}>Don&rsquo;t show again</button>
          <button type="button" className="help-btn" onClick={onClose}>Got it — explore the dashboard →</button>
        </div>
      </div>
    </div>
  );
}
