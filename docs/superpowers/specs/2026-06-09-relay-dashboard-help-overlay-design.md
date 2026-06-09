# Relay — Dashboard Help Overlay (animated onboarding)

**Status:** Approved design · **Date:** 2026-06-09
**Parent:** Phase 3b dashboard ([`2026-06-08-relay-phase3b-dashboard-design.md`](2026-06-08-relay-phase3b-dashboard-design.md))
**Scope:** A post-3 enhancement to the embedded dashboard (`web/`). Deploys live via the Railway `main`-branch auto-deploy once merged.

## Purpose

First-time visitors landing on the live dashboard (https://server-production-840f.up.railway.app)
should immediately understand what Relay is. Add an **animated first-visit help overlay** that
explains, in one screen, what Relay does and the lifecycle every job follows — with a persistent
**?** control to reopen it anytime.

## Scope

In scope:

- A `HelpOverlay` React component: a modal that auto-opens on a visitor's first load, explains Relay
  in a line, shows a **CSS-animated job-lifecycle** (Ready → In-flight → done, with retry→Delayed
  and Dead-letter branches), three concept captions, and dismiss controls.
- A persistent **?** button (always visible in the sidebar) that opens the overlay on demand.
- "Seen" persistence in `localStorage` so the overlay auto-opens only on the first visit.
- Pure-logic helper for the auto-open decision, unit-tested.

Out of scope: a guided spotlight tour; multi-page docs; any backend/API change; a new npm dependency
(animation is pure CSS, consistent with the dashboard's no-extra-deps stance); i18n.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Form | **First-visit modal overlay + always-visible `?`** | Chosen by the user: grabs newcomers, stays out of the way on return visits, reopenable anytime. |
| Animation | **Pure CSS keyframes** (no library) | Matches the dashboard's dependency discipline (hand-rolled SVG sparklines, no chart/motion lib). The lifecycle is a small set of absolutely-positioned nodes + keyframe-animated dot tokens. |
| Persistence | **`localStorage["relay.help.seen"]`** | Auto-open only when unset; any dismiss path sets it; the `?` ignores it and always opens. |
| Placement of `?` | **Sidebar footer**, next to the live indicator | Always on screen regardless of selected queue; unobtrusive. |
| Accessibility | **Esc to close, focus trap-lite, `prefers-reduced-motion` respected** | Keyboard-dismissible; animations pause/!animate for reduced-motion users. |

## Components & changes (all under `web/`)

### `src/lib/help.ts` (new, pure, tested)

- `HELP_SEEN_KEY = "relay.help.seen"`.
- `shouldAutoOpen(storage: Pick<Storage,"getItem">): boolean` — returns `true` when the key is unset
  (or storage throws, e.g. privacy mode → default to showing once). Pure; unit-tested with a fake
  storage object (no real `localStorage` needed in tests).
- `markSeen(storage: Pick<Storage,"setItem">): void` — sets the key to `"1"`, swallowing any
  storage exception (private mode) so a dismiss never throws.

### `src/components/HelpOverlay.tsx` (new)

Props: `{ open: boolean; onClose: () => void }`. Renders nothing when `!open`. When open:

- A backdrop (click → `onClose`) and a centered modal (`role="dialog"`, `aria-modal`,
  `aria-label="What is Relay?"`, click-stop on the modal).
- Header: kicker `▚ Relay`, Fraunces title "A task queue, built from scratch on Redis", a one-line
  lede.
- **The animated stage** (the centerpiece): absolutely-positioned nodes — `Ready`, `In-flight`,
  `Delayed`, `Dead-letter` — plus `enqueue ▸` and `done ✓` end labels, SVG connector wires (ready→
  inflight→done; inflight→delayed; delayed→ready loop labeled "retry · full-jitter backoff";
  delayed→dlq labeled "retries exhausted"), and CSS-animated dot tokens: three terracotta
  happy-path dots (staggered), one gold retry dot (loops through Delayed back to Ready), one dot
  that ends in Dead-letter.
- Three concept captions: **The atomic claim**, **Retry with backoff**, **Crash-safe** (copy as in
  the approved mock — accurate to the engine: single Lua claim, full-jitter backoff via the delayed
  set, reaper visibility-deadline recovery / at-least-once).
- Actions: **"Got it — explore the dashboard →"** (primary) and **"Don't show again"** (ghost) —
  both call `onClose`. `Esc` also closes.
- A `useEffect` adds/removes a `keydown` listener for `Esc` while open.

Styling lives in `theme.css` (reusing the existing design tokens — `--accent`, `--line`, `--panel`,
`--muted`, `--faint`, etc.). The one extra hue, the retry-dot gold `#cbb48e`, is the same value the
throughput sparkline already uses as a literal in `Charts.tsx`; the overlay may use it as a literal
too or promote it to a `--gold` token — no genuinely new colors.
All keyframe animations are wrapped so that under `@media (prefers-reduced-motion: reduce)` the dots
are shown static (animation disabled) — the diagram still reads.

### `src/App.tsx` (modify)

- Add `const [helpOpen, setHelpOpen] = useState(false)`.
- On mount, `useEffect(() => { if (shouldAutoOpen(localStorage)) setHelpOpen(true); }, [])`.
- `onClose` for the overlay: `markSeen(localStorage); setHelpOpen(false)`.
- Render `<HelpOverlay open={helpOpen} onClose={closeHelp} />` alongside the existing
  `EnqueueForm` modal.
- Pass an `onHelpClick={() => setHelpOpen(true)}` handler to `Sidebar`.

### `src/components/Sidebar.tsx` (modify)

- Add a small **?** button in the sidebar footer (near the `live · 1s` indicator) wired to
  `onHelpClick`. Styled as a subtle circular/ghost control (`aria-label="What is Relay?"`).

### `web/dist` (rebuild)

Rebuild and commit `web/dist` (the server embeds it). CI's dist-in-sync check enforces this.

## Behavior

- **First visit:** `relay.help.seen` unset → overlay auto-opens. User reads it, clicks "Got it" or
  "Don't show again" (or Esc / backdrop) → `markSeen` writes the flag → it won't auto-open again.
- **Return visit:** flag set → no auto-open. The **?** in the sidebar opens it on demand any time.
- **Private-mode / storage blocked:** `shouldAutoOpen` defaults to `true` and `markSeen` no-ops
  safely; worst case the overlay shows each visit but never errors.

## Testing

- **`src/lib/help.test.ts` (vitest):** `shouldAutoOpen` → true when key absent, false when `"1"`,
  true when `getItem` throws; `markSeen` writes `"1"` and swallows a throwing `setItem`.
- **Gates:** `tsc --noEmit` (strict), `vitest run`, `vite build`, and the committed-`dist`-in-sync
  check all pass. No Go changes, so the Go suite is unaffected.
- Manual: load with cleared storage → overlay appears and animates; dismiss → gone; reload → not
  shown; click **?** → reopens; `prefers-reduced-motion` → static diagram.

## Rollout

Normal flow: implement on a branch → PR → CI green → merge to `main`. Railway's `server` service
auto-rebuilds from `main` and the new overlay is live. No infra change.

## Known limitations

- The lifecycle animation is **illustrative**, not driven by live data (it always shows the same
  representative flow). The real-time numbers remain in the dashboard's tiles/charts.
- "Seen" is per-browser (localStorage), not per-user. Clearing storage re-triggers the first-visit
  overlay.
