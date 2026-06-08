# Relay — Phase 3b: Live Dashboard

**Status:** Approved design · **Date:** 2026-06-08
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Depends on:** [`2026-06-08-relay-phase3a-http-api-design.md`](2026-06-08-relay-phase3a-http-api-design.md)
**Phase:** 3 (polish) — second sub-project (3a HTTP API ✅; 3b this; 3c producer SDK; 3d packaging/deploy/README).

## Purpose

Give Relay a live, visual control surface: a single-page dashboard that shows each queue's depth
(ready/inflight/delayed/dlq), throughput over time, and the dead-letter queue with one-click
requeue, plus a small enqueue form for driving the demo. It consumes the Phase 3a HTTP API and a
new server-sent-events stream, and ships embedded in `cmd/server` so the whole thing is one Go
binary.

## Scope

In scope:

- A Vite + React + TypeScript app in `web/`, built to `web/dist` (committed) and embedded into
  `cmd/server` via `go:embed`, served at `/`.
- A dark-editorial visual design (fonts/colors/layout locked below).
- An SSE endpoint (`GET /api/stream`) that pushes per-queue depth + cumulative counters every ~1s.
- Cluster-wide throughput: additive `INCR` counters in `ack.lua` (processed) and `nack.lua` (dead),
  read by the server and streamed; the client derives the rate.
- DLQ inspection + requeue and an enqueue form, both over the existing 3a REST endpoints.

Out of scope: authentication; historical persistence (charts are in-memory rolling windows that
reset on reload); per-job drill-down beyond the DLQ payload preview; Grafana/Prometheus dashboards
(that is the 3d stack); a counter-reset endpoint (the new Redis counters are monotonic); the
producer SDK (3c) and packaging/deploy (3d).

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Frontend stack | **Vite + React + TypeScript**, static build embedded via `go:embed` | Component model + types + good DX for maintainability, while keeping the single Go-binary deploy. No SSR/server features needed (the Go server is the backend), so Next.js would be used only as a static exporter — Vite fits better. |
| Aesthetic | **Dark editorial** (Fraunces serif + IBM Plex, terracotta accent) | Chosen by the user from three directions; distinctive and high-craft, not a generic dev-tool look. |
| Layout | **Left sidebar (queue list) + main panel** | Chosen by the user; scales to many queues, control-room feel. |
| Charts | **Hand-rolled SVG sparklines** | Dependency-free, matches the mockup, keeps frontend deps to just React (no chart lib, router, or state library). |
| Realtime | **SSE (`GET /api/stream`)**, server composes snapshots | Lower latency than polling and the server already has Redis; the client just listens and renders. |
| Throughput source | **Redis-resident processed/dead counters** (`INCR` in ack/nack), rate derived client-side | `relay_jobs_processed_total` lives only on each worker's `/metrics`; the dashboard server has no processed count. A shared Redis counter is cluster-wide, atomic (one script each), and readable by the server. Client derives Δ/Δt from the counter stream so the server stays stateless per tick. |
| Build/embed | **Commit `web/dist`, `go:embed` it** | `go build ./...` stays self-contained (no Node needed for the Go build/CI/contributors); CI rebuilds the frontend to verify it is in sync. |

## Visual design (locked)

Reference mockup: `.superpowers/brainstorm/*/content/dark-editorial.html` (this session). Design tokens:

```
--bg:#15120e        warm espresso near-black
--panel:#1c1813     surface
--panel-2:#211c16   raised surface
--line:#2e271e      hairline border
--ink:#ece3d4       primary text (warm cream)
--muted:#9a8f7c     secondary text
--faint:#6f6757     tertiary / labels
--accent:#d2603f    terracotta (single accent; DLQ + active markers + primary actions)
fonts: Fraunces (display: wordmark, stat numbers, headings),
       IBM Plex Sans (UI text), IBM Plex Mono (labels, job IDs, counts)
```

- **Sidebar:** `Relay.` wordmark (Fraunces, the `.` in accent) + `task queue` mono sub-label; a
  `Queues` section listing each queue (name in Fraunces, count in mono) with the active one marked
  by an inset accent bar; a `+ Enqueue a job` button and a `host · live 1s` footer with a pulsing
  accent dot.
- **Main:** breadcrumb + queue name (Fraunces, large) + `updated …` line; a hairline rule; four
  stat tiles (Ready / In-flight / Delayed / Dead-letter, the DLQ tile in the accent); two chart
  panels (Queue depth, Throughput) with hairline framing and SVG sparklines; the Dead-letter table
  (Job ID mono, Attempts, Payload preview, Age, Requeue button).
- Fonts loaded locally (self-host the woff2 in `web/` or via a build-time font step) so the embedded
  binary has no external font dependency at runtime; a Google Fonts `<link>` is acceptable for the
  mockup but the shipped app should vendor the fonts to keep `/` self-contained offline. (Implementer
  may use `@fontsource/*` packages, which Vite bundles.)

## Architecture

```
browser (React SPA)
  ├── EventSource("/api/stream")  ──► live depth + counters every ~1s (all queues)
  ├── GET  /api/queues/{q}/dlq    ──► DLQ table (on select / refresh)
  ├── POST /api/queues/{q}/jobs   ──► enqueue form
  └── POST /api/queues/{q}/dlq/{id}/requeue ──► requeue button
cmd/server (Go)
  ├── /            ──► embedded SPA (web/dist via go:embed), index.html fallback
  ├── /api/        ──► api.New(broker)  (3a endpoints + new /api/stream)
  ├── /metrics     ──► promhttp (unchanged)
  └── /healthz     ──► 200 (unchanged)
redis
  └── q:{name}:processed / q:{name}:dead  (new monotonic counters, INCR by ack/nack)
```

## Components & changes

### `internal/broker/scripts/ack.lua`

Add one line after the inflight removal / job delete: `redis.call('INCR', KEYS[?] /* processed key */)`.
The processed counter key `q:{name}:processed` is passed in as an extra `KEYS`/`ARGV` entry from Go
(the script does not derive key names). Still one atomic script; the increment only happens on a
successful ack.

### `internal/broker/scripts/nack.lua`

On the **dead** branch only (`return 'dead'`), add `redis.call('INCR', /* dead key */)` before
returning. The dead counter key `q:{name}:dead` is passed from Go. Retry branch is unchanged.

### `internal/broker` (`broker.go`)

- `Ack` passes the processed-counter key to `ack.lua`; `Nack` passes the dead-counter key to
  `nack.lua`. Add key helpers `processedKey(queue)` → `"q:"+queue+":processed"` and
  `deadKey(queue)` → `"q:"+queue+":dead"`.
- `type Counters struct { Processed, Dead int64 }` and
  `Counters(ctx, queue) (Counters, error)` — `GET` both keys (missing → 0), in one pipeline.
- These are additive; `Ack`/`Nack` signatures and the existing metric instrumentation are unchanged.

### `internal/api` — `GET /api/stream` (SSE)

- Sets `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`;
  flushes after each event (requires `http.Flusher`).
- Loops on a `time.Ticker` (~1s) until `r.Context().Done()`: calls `broker.Queues`, then for each
  queue `Stats` + `Counters`, and writes one event
  `data: [{"queue":…,"ready":…,"inflight":…,"delayed":…,"dlq":…,"processed_total":…,"dead_total":…}, …]\n\n`.
  An initial snapshot is sent immediately (not after the first tick) so the UI populates at once.
- On a Redis error for a tick it skips that tick (logs) rather than tearing down the stream.

### `web/embed.go` (embed package)

`go:embed` cannot reach across directories (no `../`), so the embed lives next to the assets: a
tiny `package web` file with `//go:embed all:dist` exposing `var Assets embed.FS` (and a helper
returning an `fs.FS` rooted at `dist`). `cmd/server` imports this package. (`all:` so dotfiles/
nested assets are included.)

### `cmd/server`

- Import `web` and serve `web.Assets` at `/` via `http.FileServerFS`, with an SPA fallback (any
  non-`/api`, non-`/metrics`, non-`/healthz` path that isn't a real asset returns `index.html`).
  Keep `/api/`, `/metrics`, `/healthz`. The `/api/stream` route is registered by `api.New`.

### `web/` (new Vite + React + TS app)

- Deps: `react`, `react-dom`, `@fontsource/fraunces`, `@fontsource/ibm-plex-sans`,
  `@fontsource/ibm-plex-mono`; dev: `vite`, `typescript`, `@vitejs/plugin-react`, `vitest`. No chart,
  router, or state-management libraries.
- Structure (focused files): `main.tsx`, `App.tsx` (layout + selected-queue state), `api.ts` (REST
  calls + types), `useStream.ts` (EventSource hook → snapshot state), `lib/series.ts` (rolling
  window + throughput rate, pure + unit-tested), `lib/format.ts` (age/number/bytes, pure +
  tested), and components `Sidebar.tsx`, `StatTiles.tsx`, `Sparkline.tsx`, `Charts.tsx`,
  `DlqTable.tsx`, `EnqueueForm.tsx`, plus `theme.css` with the tokens above.
- Vite `base: './'` and `build.outDir: 'dist'`, so the build writes `web/dist/` (committed and
  embedded by `web/embed.go`) with relative asset URLs.

## Data model additions

| Key | Type | Written by | Read by |
|---|---|---|---|
| `q:{name}:processed` | string counter | `ack.lua` (`INCR` on ack) | `broker.Counters` → SSE throughput |
| `q:{name}:dead` | string counter | `nack.lua` (`INCR` on dead) | `broker.Counters` → SSE |

Both are monotonic and cluster-wide (every worker increments the same key). No TTL, no reset.

## Testing

### Go (real Redis where needed; skip-not-fail)

- **broker (DB 15):** `ack` increments `q:{name}:processed`; `nack`→dead increments `q:{name}:dead`
  while `nack`→retry does **not**; `Counters` returns the values (and 0 for an untouched queue).
  Existing ack/nack tests still pass (increment is additive).
- **api (DB 12):** `GET /api/stream` returns `text/event-stream`, emits at least one parseable
  `data:` snapshot containing a seeded queue with correct fields, then returns promptly when the
  request context is cancelled. The SPA fallback handler serves `index.html` for an unknown path.
- **cmd/server:** build/vet only.

### Frontend

- `vitest` unit tests for the pure logic: throughput rate from successive cumulative samples
  (`lib/series.ts`), rolling-window cap, and `lib/format.ts` (age, counts). The SSE-snapshot reducer
  (merge snapshot → per-queue state) is unit-tested with sample payloads.
- Gates: `tsc --noEmit` (strict) and `vite build` must pass. No browser/E2E tests.

### CI

- New frontend job: Node setup → `npm ci` (in `web/`) → `tsc --noEmit` → `vitest run` →
  `vite build`. The job also fails if `vite build` produces a `web/dist` that differs from the
  committed one (keeps the embedded bundle in sync).
- The existing Go job is unchanged and builds against the committed `web/dist`.

## Invariants preserved

- At-least-once delivery — the new counters are observational `INCR`s; no job movement changes.
- The atomic claim is sacred — `claim.lua` is untouched; `ack.lua`/`nack.lua` each remain a single
  atomic script, now with one additive `INCR`.
- Crash safety via the reaper — untouched.
- Build the queue from scratch on Redis primitives — the dashboard is a separate `web/` workspace; it
  adds no Go queue dependency. The Go module still depends only on go-redis + prometheus.

## Known limitations

- **Charts are in-memory.** Depth/throughput history is a client-side rolling window; a reload starts
  fresh. Long-term history is Prometheus/Grafana's job (3d).
- **Counters are monotonic and never reset.** `processed`/`dead` grow forever (until the Redis DB is
  flushed). Throughput is a rate over deltas, so this is fine; absolute totals just keep climbing.
- **SSE fan-out is per-connection.** Each open dashboard runs its own ticker reading Redis; fine for
  a handful of viewers, not tuned for many concurrent dashboards.
- **No auth.** Same as 3a — demo-grade.
- **Committed `web/dist`.** The built bundle is in git; it must be rebuilt and committed when the UI
  changes (CI verifies it matches source).
