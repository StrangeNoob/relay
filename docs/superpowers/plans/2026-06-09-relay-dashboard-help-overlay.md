# Dashboard Help Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an animated first-visit help overlay to the Relay dashboard that explains the project and the job lifecycle, with an always-visible "?" to reopen it.

**Architecture:** A pure-CSS-animated `HelpOverlay` React component (job-lifecycle diagram with moving dot tokens), gated by a tested `lib/help.ts` localStorage helper (`shouldAutoOpen`/`markSeen`). `App.tsx` auto-opens it on first visit and renders it; `Sidebar.tsx` gains a "?" button to reopen. No new dependency, no backend change; rebuild the committed `web/dist`.

**Tech Stack:** React 18 + TypeScript + Vite + Vitest; pure CSS keyframes (no motion lib). All under `web/`.

**Spec:** [`docs/superpowers/specs/2026-06-09-relay-dashboard-help-overlay-design.md`](../specs/2026-06-09-relay-dashboard-help-overlay-design.md)

**Conventions:** strict TS must pass; use explicit `vitest` imports; the committed `web/dist` must be rebuilt (CI checks it). No Go changes.

---

## File Structure

- **Create `web/src/lib/help.ts`** — `HELP_SEEN_KEY`, `shouldAutoOpen`, `markSeen` (pure).
- **Create `web/src/lib/help.test.ts`** — vitest unit tests.
- **Create `web/src/components/HelpOverlay.tsx`** — the animated overlay modal.
- **Modify `web/src/theme.css`** — append the `help-*` styles + keyframes.
- **Modify `web/src/components/Sidebar.tsx`** — add the "?" reopen button (`onHelpClick` prop).
- **Modify `web/src/App.tsx`** — wire auto-open + render overlay + pass `onHelpClick`.
- **Rebuild `web/dist`** — commit the updated bundle.

---

## Task 1: `lib/help.ts` (localStorage gate) with tests

**Files:** Create `web/src/lib/help.ts`, `web/src/lib/help.test.ts`

- [ ] **Step 1: Write the failing tests** — `web/src/lib/help.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { shouldAutoOpen, markSeen, HELP_SEEN_KEY } from "./help";

function fakeStorage(initial: Record<string, string> = {}) {
  const m = new Map(Object.entries(initial));
  return {
    store: m,
    getItem: (k: string) => (m.has(k) ? m.get(k)! : null),
    setItem: (k: string, v: string) => void m.set(k, v),
  };
}

describe("shouldAutoOpen", () => {
  it("is true when the seen key is absent", () => {
    expect(shouldAutoOpen(fakeStorage())).toBe(true);
  });
  it("is false when the seen key is set", () => {
    expect(shouldAutoOpen(fakeStorage({ [HELP_SEEN_KEY]: "1" }))).toBe(false);
  });
  it("defaults to true when getItem throws (e.g. privacy mode)", () => {
    const throwing = { getItem: () => { throw new Error("blocked"); } };
    expect(shouldAutoOpen(throwing)).toBe(true);
  });
});

describe("markSeen", () => {
  it("writes the seen flag", () => {
    const s = fakeStorage();
    markSeen(s);
    expect(s.getItem(HELP_SEEN_KEY)).toBe("1");
  });
  it("swallows a throwing setItem", () => {
    const throwing = { setItem: () => { throw new Error("blocked"); } };
    expect(() => markSeen(throwing)).not.toThrow();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/leon/WorkSpace/relay-help-overlay/web && npx vitest run help`
Expected: FAIL — `./help` module not found.

- [ ] **Step 3: Implement `web/src/lib/help.ts`**

```ts
// Persistence gate for the first-visit help overlay. Kept pure (takes the storage
// as a parameter) so it is trivially unit-testable and never throws on a blocked
// localStorage (private mode).

export const HELP_SEEN_KEY = "relay.help.seen";

// shouldAutoOpen reports whether the overlay should auto-open. It opens when the
// "seen" flag is absent; if storage access throws (privacy mode), it defaults to
// showing the overlay rather than failing.
export function shouldAutoOpen(storage: Pick<Storage, "getItem">): boolean {
  try {
    return storage.getItem(HELP_SEEN_KEY) === null;
  } catch {
    return true;
  }
}

// markSeen records that the visitor has dismissed the overlay. A blocked storage
// is swallowed so dismissing never throws.
export function markSeen(storage: Pick<Storage, "setItem">): void {
  try {
    storage.setItem(HELP_SEEN_KEY, "1");
  } catch {
    // ignore (private mode / storage disabled)
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/leon/WorkSpace/relay-help-overlay/web && npx vitest run help` → PASS (5). Then `npm run typecheck` clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/leon/WorkSpace/relay-help-overlay
git add web/src/lib/help.ts web/src/lib/help.test.ts
git commit -m "Add help-overlay localStorage gate (shouldAutoOpen/markSeen) with tests"
```

---

## Task 2: `HelpOverlay.tsx` + styles

**Files:** Create `web/src/components/HelpOverlay.tsx`, modify `web/src/theme.css`

- [ ] **Step 1: Create `web/src/components/HelpOverlay.tsx`**

```tsx
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
```

- [ ] **Step 2: Append the styles to `web/src/theme.css`**

Append this block at the end of `web/src/theme.css` (uses the existing tokens; `#cbb48e` is the throughput-chart gold used as a literal):

```css
/* help overlay */
.help-backdrop { position: fixed; inset: 0; background: rgba(0, 0, 0, .62); display: flex; align-items: center; justify-content: center; padding: 20px; z-index: 50; }
.help-modal { width: 760px; max-width: 96vw; max-height: 92vh; overflow: auto; background: var(--panel-2); border: 1px solid var(--line); border-radius: 16px; box-shadow: 0 30px 80px rgba(0, 0, 0, .6); }
.help-head { padding: 22px 26px 6px; }
.help-kicker { font-family: var(--mono); font-size: 10px; letter-spacing: .22em; text-transform: uppercase; color: var(--accent); }
.help-title { font-family: var(--serif); font-weight: 600; font-size: 27px; letter-spacing: -.01em; margin: 6px 0 4px; }
.help-lede { color: var(--muted); font-size: 13.5px; line-height: 1.55; max-width: 62ch; margin: 0; }
.help-stage { position: relative; height: 248px; margin: 6px 18px 0; }
.help-wires { position: absolute; inset: 0; width: 100%; height: 100%; overflow: visible; }
.help-wire { stroke: var(--line); stroke-width: 1.5; fill: none; }
.help-wire-retry { stroke: #cbb48e; stroke-dasharray: 4 4; opacity: .7; }
.help-wire-dead { stroke: rgba(210, 96, 63, .5); }
.help-node { position: absolute; border: 1px solid var(--line); background: var(--panel); border-radius: 10px; display: flex; flex-direction: column; justify-content: center; align-items: center; }
.help-nm { font-family: var(--mono); font-size: 10px; letter-spacing: .12em; text-transform: uppercase; color: var(--ink); }
.help-sub { font-family: var(--mono); font-size: 8.5px; color: var(--faint); margin-top: 2px; }
.help-ready { left: 40px; top: 92px; width: 104px; height: 50px; }
.help-inflight { left: 286px; top: 92px; width: 120px; height: 50px; }
.help-delayed { left: 286px; top: 182px; width: 120px; height: 40px; border-color: rgba(203, 180, 142, .4); }
.help-dlq { left: 566px; top: 178px; width: 120px; height: 44px; border-color: rgba(210, 96, 63, .45); background: linear-gradient(var(--accent-soft), transparent), var(--panel); }
.help-dlq .help-nm { color: var(--accent); }
.help-end { position: absolute; font-family: var(--mono); font-size: 10px; color: var(--muted); }
.help-enq { left: 0; top: 62px; color: #cbb48e; }
.help-done { left: 600px; top: 108px; color: #9fd17a; }
.help-wlabel { position: absolute; font-family: var(--mono); font-size: 8.5px; color: var(--faint); }
.help-wl-retry { left: 150px; top: 214px; }
.help-wl-dead { left: 430px; top: 184px; color: #a8584a; }
.help-tok { position: absolute; width: 11px; height: 11px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 9px var(--accent); }
.help-h1 { animation: help-happy 5s linear infinite; }
.help-h2 { animation: help-happy 5s linear infinite 1.7s; }
.help-h3 { animation: help-happy 5s linear infinite 3.3s; }
.help-rt { background: #cbb48e; box-shadow: 0 0 9px #cbb48e; animation: help-retry 6.5s ease-in-out infinite 1s; }
.help-dd { animation: help-dead 8s ease-in-out infinite 2.5s; }
@keyframes help-happy {
  0% { left: 8px; top: 112px; opacity: 0; }
  7% { opacity: 1; }
  24% { left: 86px; top: 112px; }
  34% { left: 120px; top: 112px; }
  52% { left: 330px; top: 112px; }
  86% { left: 600px; top: 112px; opacity: 1; }
  95% { left: 632px; top: 112px; opacity: 0; }
  100% { opacity: 0; }
}
@keyframes help-retry {
  0% { left: 92px; top: 112px; opacity: 0; }
  8% { opacity: 1; }
  26% { left: 330px; top: 112px; }
  38% { left: 336px; top: 196px; }
  58% { left: 336px; top: 196px; }
  80% { left: 96px; top: 196px; }
  92% { left: 92px; top: 112px; }
  100% { left: 92px; top: 112px; opacity: 1; }
}
@keyframes help-dead {
  0% { left: 92px; top: 112px; opacity: 0; }
  10% { opacity: 1; }
  28% { left: 330px; top: 112px; }
  46% { left: 336px; top: 196px; }
  70% { left: 612px; top: 196px; }
  100% { left: 612px; top: 196px; opacity: 1; }
}
.help-caps { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; padding: 8px 26px 4px; }
.help-cap { border-top: 1px solid var(--line); padding-top: 10px; }
.help-cap h4 { font-family: var(--serif); font-weight: 500; font-size: 13.5px; margin: 0 0 3px; }
.help-cap p { margin: 0; color: var(--muted); font-size: 11.5px; line-height: 1.45; }
.help-actions { display: flex; justify-content: space-between; align-items: center; padding: 14px 26px 22px; }
.help-skip { font-family: var(--mono); font-size: 11px; color: var(--faint); background: none; border: none; cursor: pointer; }
.help-skip:hover { color: var(--muted); }
.help-btn { background: var(--accent); color: #1b120e; border: none; border-radius: 9px; padding: 9px 18px; font-family: var(--sans); font-weight: 600; font-size: 13px; cursor: pointer; }
@media (prefers-reduced-motion: reduce) {
  .help-tok { animation: none !important; }
  .help-h2, .help-h3, .help-rt, .help-dd { opacity: 1; }
  .help-h1 { left: 120px; top: 112px; }
  .help-h2 { left: 330px; top: 112px; }
  .help-h3 { left: 600px; top: 112px; }
  .help-rt { left: 336px; top: 196px; }
  .help-dd { left: 612px; top: 196px; }
}
```

- [ ] **Step 3: Typecheck**

Run: `cd /Users/leon/WorkSpace/relay-help-overlay/web && npm run typecheck`
Expected: clean (the component compiles; it is not yet rendered — that's Task 4).

- [ ] **Step 4: Commit**

```bash
cd /Users/leon/WorkSpace/relay-help-overlay
git add web/src/components/HelpOverlay.tsx web/src/theme.css
git commit -m "Add animated HelpOverlay component and styles"
```

---

## Task 3: Sidebar "?" reopen button

**Files:** Modify `web/src/components/Sidebar.tsx`, `web/src/theme.css`

- [ ] **Step 1: Add the prop + button** — replace the full contents of `web/src/components/Sidebar.tsx` with:

```tsx
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
```

- [ ] **Step 2: Add the "?" button style** — append to `web/src/theme.css`:

```css
.conn .help-q { margin-left: auto; width: 18px; height: 18px; border-radius: 50%; border: 1px solid var(--line); background: transparent; color: var(--muted); font-family: var(--sans); font-size: 11px; line-height: 1; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; }
.conn .help-q:hover { border-color: var(--accent); color: var(--accent); }
```

(The `.conn` rule already uses `display: flex; align-items: center; gap: 7px;`, so `margin-left: auto` pushes the "?" to the right edge.)

- [ ] **Step 3: Typecheck**

Run: `cd /Users/leon/WorkSpace/relay-help-overlay/web && npm run typecheck`
Expected: FAIL — `App.tsx` calls `<Sidebar .../>` without the new required `onHelpClick` prop. That is expected and fixed in Task 4. (If you prefer a clean typecheck here, proceed directly to Task 4 before running it; either way Task 4 makes it pass.)

- [ ] **Step 4: Commit**

```bash
cd /Users/leon/WorkSpace/relay-help-overlay
git add web/src/components/Sidebar.tsx web/src/theme.css
git commit -m "Add sidebar help (?) reopen button"
```

---

## Task 4: Wire the overlay into `App.tsx`

**Files:** Modify `web/src/App.tsx`

- [ ] **Step 1: Add imports** — add these alongside the existing imports in `web/src/App.tsx`:

```tsx
import { HelpOverlay } from "./components/HelpOverlay";
import { shouldAutoOpen, markSeen } from "./lib/help";
```

- [ ] **Step 2: Add state, auto-open effect, and close handler** — inside the `App` component, add a `helpOpen` state alongside the other `useState` calls:

```tsx
  const [helpOpen, setHelpOpen] = useState(false);
```

Add this effect (place it near the other mount effects):

```tsx
  // Auto-open the help overlay on a visitor's first load.
  useEffect(() => {
    if (shouldAutoOpen(localStorage)) setHelpOpen(true);
  }, []);
```

And a close handler (place it next to the other handlers like `onRequeue`):

```tsx
  const closeHelp = () => {
    markSeen(localStorage);
    setHelpOpen(false);
  };
```

- [ ] **Step 3: Pass `onHelpClick` to Sidebar and render the overlay** — in the returned JSX, update the `<Sidebar .../>` usage to add the prop:

```tsx
        onEnqueueClick={() => setShowEnqueue(true)}
        onHelpClick={() => setHelpOpen(true)}
```

(Add the `onHelpClick` line immediately after the existing `onEnqueueClick` prop on the `Sidebar` element.)

Then render the overlay near the existing `EnqueueForm` conditional (just before the closing `</div>` of the `.app` container):

```tsx
      <HelpOverlay open={helpOpen} onClose={closeHelp} />
```

- [ ] **Step 4: Typecheck + tests + dev sanity**

Run:
```bash
cd /Users/leon/WorkSpace/relay-help-overlay/web
npm run typecheck
npx vitest run
```
Expected: typecheck clean; all vitest tests pass (help + existing format/series/snapshot). If you have a local stack, `npm run dev` and confirm: overlay auto-opens with cleared storage, dots animate, "Got it"/"Don't show again"/Esc/backdrop close it, reload doesn't re-open, the sidebar "?" reopens it.

- [ ] **Step 5: Commit**

```bash
cd /Users/leon/WorkSpace/relay-help-overlay
git add web/src/App.tsx
git commit -m "Wire help overlay: auto-open on first visit, reopen via sidebar"
```

---

## Task 5: Rebuild `web/dist` and verify

**Files:** `web/dist` (rebuild)

- [ ] **Step 1: Build**

Run:
```bash
cd /Users/leon/WorkSpace/relay-help-overlay/web
npm run typecheck
npm run test
npm run build
```
Expected: typecheck + tests clean; `vite build` regenerates `web/dist` including the overlay.

- [ ] **Step 2: Confirm Go build still embeds cleanly**

Run:
```bash
cd /Users/leon/WorkSpace/relay-help-overlay
go build ./...
go test ./web/ -run TestHandler
```
Expected: clean; `web.Handler` tests still pass (index/SPA fallback unaffected).

- [ ] **Step 3: Commit the rebuilt dist**

```bash
cd /Users/leon/WorkSpace/relay-help-overlay
git add web/dist
git commit -m "Rebuild web/dist with the help overlay"
```

- [ ] **Step 4: Full verification**

Run:
```bash
cd /Users/leon/WorkSpace/relay-help-overlay
( cd web && npm run typecheck && npm run test && npm run build && git diff --exit-code -- dist )
go build ./... && go vet ./... && gofmt -l web/
```
Expected: frontend typecheck/test/build clean and `dist` in sync (no diff); Go build/vet/fmt clean. If anything fails, STOP and report.

---

## Self-Review (completed during planning)

- **Spec coverage:** localStorage gate + tests (Task 1); animated `HelpOverlay` + styles incl. `prefers-reduced-motion` and Esc/backdrop close (Task 2); always-visible sidebar "?" (Task 3); first-visit auto-open + render + reopen wiring (Task 4); dist rebuild + verification (Task 5). Covers every spec section (form, animation, persistence, placement, a11y, testing, rollout).
- **Type consistency:** `shouldAutoOpen(Pick<Storage,"getItem">)` / `markSeen(Pick<Storage,"setItem">)` / `HELP_SEEN_KEY` match across help.ts, its test, and App.tsx. `HelpOverlay` props `{open, onClose}` match the App render. `Sidebar` gains required `onHelpClick: () => void`, supplied in Task 4 (Task 3's standalone typecheck failure is called out explicitly and resolved in Task 4). CSS classes are `help-*`-prefixed to avoid collisions; the `.conn .help-q` selector relies on the existing `.conn` flex rule.
- **No placeholders:** every file's full content/edit is given with exact code; the gold `#cbb48e` literal matches the existing throughput-chart color (no new token required).
- **No Go changes:** frontend-only; the Go suite and `web.Handler` are unaffected (dist embed only).
```
