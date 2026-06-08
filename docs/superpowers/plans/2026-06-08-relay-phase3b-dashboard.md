# Phase 3b Live Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a live, embedded dark-editorial dashboard for Relay: per-queue depth + throughput over SSE, a dead-letter table with one-click requeue, and an enqueue form — a Vite+React+TS app served from `cmd/server` via `go:embed`.

**Architecture:** Backend adds cluster-wide `processed`/`dead` Redis counters (additive `INCR` in `ack.lua`/`nack.lua`), a `broker.Counters` reader, and an SSE endpoint (`GET /api/stream`) that pushes per-queue depth + counters every ~1s. The frontend (`web/`, Vite+React+TS) listens via `EventSource`, derives throughput client-side, draws hand-rolled SVG sparklines, and calls the existing 3a REST endpoints for DLQ/requeue/enqueue. The built bundle (`web/dist`) is committed and embedded by a `web` package the server serves at `/`.

**Tech Stack:** Go (stdlib `net/http`, `go:embed`), `redis/go-redis/v9`, real-Redis integration tests; Vite + React 18 + TypeScript + Vitest; no chart/router/state libraries.

**Spec:** [`docs/superpowers/specs/2026-06-08-relay-phase3b-dashboard-design.md`](../specs/2026-06-08-relay-phase3b-dashboard-design.md)

**Execution note:** Frontend tasks need Node 20+ and network access for `npm`. If `npm` or the registry is unavailable in the sandbox, report BLOCKED rather than faking a build.

---

## File Structure

- **Modify `internal/broker/scripts/ack.lua`** — `INCR` the processed counter (key passed from Go).
- **Modify `internal/broker/scripts/nack.lua`** — `INCR` the dead counter on the dead branch (key passed from Go).
- **Modify `internal/broker/broker.go`** — `processedKey`/`deadKey` helpers; pass the counter keys into `ack`/`nack`; add `Counters` struct + method.
- **Modify `internal/broker/broker_test.go`** — counter increment + `Counters` tests.
- **Create `internal/api/stream.go`** — SSE handler + snapshot type (keep `api.go` focused).
- **Modify `internal/api/api.go`** — register `GET /api/stream`.
- **Modify `internal/api/api_test.go`** — SSE test.
- **Create `web/`** — Vite+React+TS app: `package.json`, `vite.config.ts`, `tsconfig.json`, `index.html`, `src/` (`main.tsx`, `App.tsx`, `theme.css`, `api.ts`, `hooks/useStream.ts`, `lib/format.ts`, `lib/series.ts`, `components/*`), and the committed build output `web/dist/`.
- **Create `web/embed.go`** — `package web`, `//go:embed all:dist`, `Handler()` with SPA fallback.
- **Create `web/handler_test.go`** — serves index.html for `/` and client routes.
- **Modify `cmd/server/main.go`** — serve `web.Handler()` at `/`.
- **Modify `.github/workflows/ci.yml`** — add a frontend job.
- **Modify `CLAUDE.md`** — document 3b.

---

## Task 1: `ack.lua` processed counter

**Files:** `internal/broker/scripts/ack.lua`, `internal/broker/broker.go`, `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/broker/broker_test.go`:

```go
func TestAckIncrementsProcessedCounter(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	if err := b.Enqueue(ctx, job.New("emails", []byte("x"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	j, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Ack(ctx, j); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, _ := rdb.Get(ctx, "q:emails:processed").Int64(); n != 1 {
		t.Errorf("q:emails:processed = %d, want 1", n)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestAckIncrementsProcessedCounter -v`
Expected: FAIL — `q:emails:processed = 0, want 1`. (Needs Redis; note if it SKIPs.)

- [ ] **Step 3: Edit `ack.lua`** — add the processed key as `KEYS[2]` and increment it:

```lua
-- ack.lua — acknowledge that a job was processed successfully.
--
-- KEYS[1] = inflight set      q:{name}:inflight
-- KEYS[2] = processed counter q:{name}:processed (cluster-wide throughput counter)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")

local id = ARGV[1]
redis.call('ZREM', KEYS[1], id)
redis.call('DEL', ARGV[2] .. id)
redis.call('INCR', KEYS[2])
return 1
```

- [ ] **Step 4: Update `Ack` in `broker.go`** — add the key helper and pass it. Add near the other key helpers:

```go
// processedKey is the Redis key for a queue's cumulative processed counter:
// `q:{name}:processed`, INCR'd by ack.lua. Read by the dashboard for throughput.
func processedKey(queue string) string { return "q:" + queue + ":processed" }
```

Change the `ackScript.Run` KEYS slice to include it:

```go
	if err := ackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), processedKey(j.Queue)},
		j.ID, jobKeyPrefix,
	).Err(); err != nil {
		return fmt.Errorf("broker: acking job %s: %w", j.ID, err)
	}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestAck' -v`
Expected: PASS (the new test plus the existing `TestAckRecordsProcessedAndLatency` — the increment is additive). Then `gofmt -l internal/broker/`, `go build ./...`, `go vet ./internal/broker/` clean.

- [ ] **Step 6: Commit**

```bash
git add internal/broker/scripts/ack.lua internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Increment a cluster-wide processed counter on ack"
```

---

## Task 2: `nack.lua` dead counter

**Files:** `internal/broker/scripts/nack.lua`, `internal/broker/broker.go`, `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/broker/broker_test.go` (reuses the `deadLetter`/`nackTestJob` helpers added in Phase 3a / Phase 2):

```go
func TestNackDeadIncrementsDeadCounter(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	_ = deadLetter(t, b, ctx, "emails", "x") // enqueue maxRetries=0 -> claim -> nack -> dead
	if n, _ := rdb.Get(ctx, "q:emails:dead").Int64(); n != 1 {
		t.Errorf("q:emails:dead = %d, want 1", n)
	}
}

func TestNackRetryDoesNotIncrementDeadCounter(t *testing.T) {
	b, rdb := newTestBroker(t)
	ctx := context.Background()

	j := job.New("emails", []byte("x")) // default MaxRetries=5 -> first nack retries
	if err := b.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := b.Nack(ctx, claimed); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if n, _ := rdb.Get(ctx, "q:emails:dead").Int64(); n != 0 {
		t.Errorf("q:emails:dead = %d, want 0 (retry must not increment)", n)
	}
}
```

(If `deadLetter` is not present in this file, define it as in the Phase 3a plan: enqueue a job with `MaxRetries = 0`, claim it, `Nack` it, return the id.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run 'TestNackDead|TestNackRetryDoesNot' -v`
Expected: FAIL — `q:emails:dead = 0, want 1` for the dead case.

- [ ] **Step 3: Edit `nack.lua`** — add the dead-counter key as `KEYS[4]`, INCR only on the dead branch:

```lua
-- nack.lua — handle a failed delivery.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- KEYS[2] = delayed set  q:{name}:delayed
-- KEYS[3] = dead-letter  q:{name}:dlq
-- KEYS[4] = dead counter q:{name}:dead (cluster-wide; INCR only when dead-lettered)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = retry ready-at in unix milliseconds (precomputed backoff)
--
-- Returns 'retry' or 'dead'.

local id = ARGV[1]
local job_key = ARGV[2] .. id
local ready_at = tonumber(ARGV[3])

redis.call('ZREM', KEYS[1], id)

local attempts = tonumber(redis.call('HGET', job_key, 'attempts')) or 0
local max_retries = tonumber(redis.call('HGET', job_key, 'max_retries')) or 0

if attempts < max_retries then
  redis.call('HSET', job_key, 'state', 'delayed')
  redis.call('ZADD', KEYS[2], ready_at, id)
  return 'retry'
end

redis.call('HSET', job_key, 'state', 'dead')
redis.call('RPUSH', KEYS[3], id)
redis.call('INCR', KEYS[4])
return 'dead'
```

- [ ] **Step 4: Update `Nack` in `broker.go`** — add the `deadKey` helper and pass it:

```go
// deadKey is the Redis key for a queue's cumulative dead-letter counter:
// `q:{name}:dead`, INCR'd by nack.lua on the dead branch.
func deadKey(queue string) string { return "q:" + queue + ":dead" }
```

Change the `nackScript.Run` KEYS slice:

```go
	outcome, err := nackScript.Run(ctx, b.rdb,
		[]string{inflightKey(j.Queue), delayedKey(j.Queue), dlqKey(j.Queue), deadKey(j.Queue)},
		j.ID, jobKeyPrefix, readyAt,
	).Text()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/broker/ -run 'TestNack' -v`
Expected: PASS (new tests + existing nack tests). Then `gofmt -l internal/broker/`, `go build ./...`, `go vet ./internal/broker/` clean.

- [ ] **Step 6: Commit**

```bash
git add internal/broker/scripts/nack.lua internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Increment a cluster-wide dead counter when a job is dead-lettered"
```

---

## Task 3: `broker.Counters`

**Files:** `internal/broker/broker.go`, `internal/broker/broker_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/broker/broker_test.go`:

```go
func TestCountersReadsProcessedAndDead(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	// process one (ack) and dead-letter one
	if err := b.Enqueue(ctx, job.New("emails", []byte("ok"))); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	j, ok, err := b.Claim(ctx, "emails", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim: %v %v", ok, err)
	}
	if err := b.Ack(ctx, j); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	_ = deadLetter(t, b, ctx, "emails", "bad")

	c, err := b.Counters(ctx, "emails")
	if err != nil {
		t.Fatalf("Counters: %v", err)
	}
	if c.Processed != 1 || c.Dead != 1 {
		t.Errorf("Counters = %+v, want {Processed:1 Dead:1}", c)
	}
}

func TestCountersUntouchedQueueIsZero(t *testing.T) {
	b, _ := newTestBroker(t)
	c, err := b.Counters(context.Background(), "emails")
	if err != nil {
		t.Fatalf("Counters: %v", err)
	}
	if c.Processed != 0 || c.Dead != 0 {
		t.Errorf("Counters = %+v, want zeros", c)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/broker/ -run TestCounters -v`
Expected: FAIL — `b.Counters` undefined.

- [ ] **Step 3: Implement `Counters` in `broker.go`** (`errors` and `redis` are already imported):

```go
// Counters is a queue's cumulative, monotonic lifetime totals — distinct from the
// point-in-time depths in Stats. They back the dashboard's throughput rate.
type Counters struct {
	Processed int64 `json:"processed_total"`
	Dead      int64 `json:"dead_total"`
}

// Counters reads a queue's processed/dead counters in one pipeline. A missing
// key (queue never acked/dead-lettered) reads as 0, not an error.
func (b *Broker) Counters(ctx context.Context, queue string) (Counters, error) {
	pipe := b.rdb.Pipeline()
	pCmd := pipe.Get(ctx, processedKey(queue))
	dCmd := pipe.Get(ctx, deadKey(queue))
	// A GET on a missing key yields redis.Nil, which Exec surfaces as an error;
	// that is expected here, so only a non-Nil error is a real failure.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return Counters{}, fmt.Errorf("broker: counters for %q: %w", queue, err)
	}
	processed, err := getInt64OrZero(pCmd)
	if err != nil {
		return Counters{}, fmt.Errorf("broker: counters for %q: %w", queue, err)
	}
	dead, err := getInt64OrZero(dCmd)
	if err != nil {
		return Counters{}, fmt.Errorf("broker: counters for %q: %w", queue, err)
	}
	return Counters{Processed: processed, Dead: dead}, nil
}

// getInt64OrZero reads a GET result as int64, treating a missing key as 0.
func getInt64OrZero(cmd *redis.StringCmd) (int64, error) {
	v, err := cmd.Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/broker/ -run TestCounters -v` → PASS (2). Then full suite under race: `go test -race ./internal/broker/`. `gofmt -l internal/broker/`, `go build ./...`, `go vet ./internal/broker/` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/broker/broker.go internal/broker/broker_test.go
git commit -m "Add broker Counters: cumulative processed/dead per queue"
```

---

## Task 4: SSE stream endpoint

**Files:** `internal/api/stream.go` (new), `internal/api/api.go`, `internal/api/api_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/api/api_test.go`. Add imports `"bufio"` and `"strings"` to the test file if missing:

```go
func TestStreamEmitsSnapshot(t *testing.T) {
	h, b, _ := newTestAPI(t)
	if err := b.Enqueue(context.Background(), mustJob("emails", "x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/api/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Read until the first "data: " line (the immediate initial snapshot).
	reader := bufio.NewReader(resp.Body)
	var payload string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("reading stream: %v", err)
		}
		if strings.HasPrefix(line, "data: ") {
			payload = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			break
		}
	}
	cancel() // stop the stream server-side

	var snaps []map[string]any
	if err := json.Unmarshal([]byte(payload), &snaps); err != nil {
		t.Fatalf("decode snapshot %q: %v", payload, err)
	}
	if len(snaps) != 1 || snaps[0]["queue"] != "emails" {
		t.Fatalf("snaps = %v, want one for emails", snaps)
	}
	if snaps[0]["ready"].(float64) != 1 {
		t.Errorf("ready = %v, want 1", snaps[0]["ready"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestStreamEmitsSnapshot -v`
Expected: FAIL — `/api/stream` route not registered (the request 404s, so Content-Type assertion fails). (Needs Redis; note if SKIP.)

- [ ] **Step 3: Create `internal/api/stream.go`**:

```go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// streamInterval is how often the SSE stream pushes a fresh snapshot.
const streamInterval = time.Second

// queueSnapshot is one queue's line in an SSE snapshot: point-in-time depths plus
// the cumulative counters the client rate-computes into throughput.
type queueSnapshot struct {
	Queue          string `json:"queue"`
	Ready          int64  `json:"ready"`
	Inflight       int64  `json:"inflight"`
	Delayed        int64  `json:"delayed"`
	DLQ            int64  `json:"dlq"`
	ProcessedTotal int64  `json:"processed_total"`
	DeadTotal      int64  `json:"dead_total"`
}

// stream handles GET /api/stream: a text/event-stream that pushes a snapshot of
// every queue immediately and then once per streamInterval until the client
// disconnects. A Redis hiccup skips a tick rather than tearing down the stream.
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		a.writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	// Immediate first snapshot so the UI populates without waiting a tick.
	if !a.writeSnapshot(ctx, w, flusher) {
		return
	}
	ticker := time.NewTicker(streamInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !a.writeSnapshot(ctx, w, flusher) {
				return
			}
		}
	}
}

// writeSnapshot composes and writes one SSE event. It returns false when the
// client connection is gone (write failed), signalling the caller to stop.
func (a *API) writeSnapshot(ctx context.Context, w http.ResponseWriter, flusher http.Flusher) bool {
	queues, err := a.broker.Queues(ctx)
	if err != nil {
		a.logger.Error("api: stream listing queues", "err", err)
		return true // skip this tick, keep the stream open
	}
	snaps := make([]queueSnapshot, 0, len(queues))
	for _, q := range queues {
		st, err := a.broker.Stats(ctx, q)
		if err != nil {
			a.logger.Error("api: stream stats", "queue", q, "err", err)
			continue
		}
		ct, err := a.broker.Counters(ctx, q)
		if err != nil {
			a.logger.Error("api: stream counters", "queue", q, "err", err)
			continue
		}
		snaps = append(snaps, queueSnapshot{
			Queue: q, Ready: st.Ready, Inflight: st.Inflight, Delayed: st.Delayed,
			DLQ: st.DLQ, ProcessedTotal: ct.Processed, DeadTotal: ct.Dead,
		})
	}
	buf, err := json.Marshal(snaps)
	if err != nil {
		a.logger.Error("api: stream marshal", "err", err)
		return true
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", buf); err != nil {
		return false // client disconnected
	}
	flusher.Flush()
	return true
}
```

- [ ] **Step 4: Register the route in `api.go`** — add to the `New` mux:

```go
	mux.HandleFunc("GET /api/stream", a.stream)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/api/ -run TestStreamEmitsSnapshot -v` → PASS. Then `go test -race ./internal/api/`, `gofmt -l internal/api/`, `go build ./...`, `go vet ./internal/api/` clean.

- [ ] **Step 6: Commit**

```bash
git add internal/api/stream.go internal/api/api.go internal/api/api_test.go
git commit -m "Add SSE /api/stream pushing per-queue depth and counters"
```

---

## Task 5: Scaffold the Vite + React + TS app

**Files:** `web/package.json`, `web/vite.config.ts`, `web/tsconfig.json`, `web/tsconfig.node.json`, `web/index.html`, `web/src/main.tsx`, `web/src/vite-env.d.ts`, plus the committed build `web/dist/`.

> Requires Node 20+ and npm registry access. If unavailable, report BLOCKED.

- [ ] **Step 1: Create `web/package.json`**:

```json
{
  "name": "relay-dashboard",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "typecheck": "tsc --noEmit",
    "test": "vitest run"
  },
  "dependencies": {
    "@fontsource/fraunces": "^5.0.0",
    "@fontsource/ibm-plex-mono": "^5.0.0",
    "@fontsource/ibm-plex-sans": "^5.0.0",
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@types/react": "^18.3.0",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "typescript": "^5.5.0",
    "vite": "^5.4.0",
    "vitest": "^2.1.0"
  }
}
```

- [ ] **Step 2: Create `web/vite.config.ts`**:

```ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base: "./" keeps asset URLs relative so the bundle works under go:embed.
// outDir: "dist" is committed and embedded by web/embed.go.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: { outDir: "dist", emptyOutDir: true },
});
```

- [ ] **Step 3: Create `web/tsconfig.json`** and `web/tsconfig.node.json`:

`web/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "types": ["vitest/globals"]
  },
  "include": ["src"],
  "references": [{ "path": "./tsconfig.node.json" }]
}
```

`web/tsconfig.node.json`:
```json
{
  "compilerOptions": {
    "composite": true,
    "skipLibCheck": true,
    "module": "ESNext",
    "moduleResolution": "bundler",
    "allowSyntheticDefaultImports": true,
    "strict": true,
    "noEmit": true
  },
  "include": ["vite.config.ts"]
}
```

- [ ] **Step 4: Create `web/index.html`**:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Relay</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 5: Create `web/src/vite-env.d.ts`** and a minimal `web/src/main.tsx`:

`web/src/vite-env.d.ts`:
```ts
/// <reference types="vite/client" />
```

`web/src/main.tsx` (minimal placeholder; the real App lands in Task 8):
```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <div>Relay dashboard</div>
  </StrictMode>,
);
```

- [ ] **Step 6: Install, build, verify**

Run:
```bash
cd web
npm install
npm run typecheck
npm run build
ls dist/index.html
```
Expected: `npm install` writes `web/package-lock.json`; typecheck clean; `vite build` writes `web/dist/` containing `index.html` and `assets/`. (If `npm` is unavailable, report BLOCKED.)

- [ ] **Step 7: Commit (including the lockfile and built dist)**

```bash
cd ..
git add web/package.json web/package-lock.json web/vite.config.ts web/tsconfig.json web/tsconfig.node.json web/index.html web/src web/dist
git commit -m "Scaffold Vite+React+TS dashboard app (builds to committed web/dist)"
```

(Do NOT add `web/node_modules`. Add a `web/.gitignore` containing `node_modules/` in this commit.)

---

## Task 6: Frontend pure logic (`format.ts`, `series.ts`) with Vitest

**Files:** `web/src/lib/format.ts`, `web/src/lib/series.ts`, `web/src/lib/format.test.ts`, `web/src/lib/series.test.ts`

- [ ] **Step 1: Write the failing tests**

`web/src/lib/format.test.ts`:
```ts
import { describe, it, expect } from "vitest";
import { formatCount, formatAge } from "./format";

describe("formatCount", () => {
  it("passes small numbers through", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(942)).toBe("942");
  });
  it("abbreviates thousands and millions", () => {
    expect(formatCount(1240)).toBe("1.2k");
    expect(formatCount(2_500_000)).toBe("2.5M");
  });
});

describe("formatAge", () => {
  it("renders seconds, minutes, hours", () => {
    expect(formatAge(5_000)).toBe("5s");
    expect(formatAge(90_000)).toBe("1m");
    expect(formatAge(3_660_000)).toBe("1h");
  });
});
```

`web/src/lib/series.test.ts`:
```ts
import { describe, it, expect } from "vitest";
import { ratePerSecond, pushSample } from "./series";

describe("ratePerSecond", () => {
  it("computes delta over elapsed seconds", () => {
    const prev = { value: 100, t: 1000 };
    const cur = { value: 130, t: 4000 }; // +30 over 3s
    expect(ratePerSecond(prev, cur)).toBe(10);
  });
  it("returns 0 for a non-positive interval", () => {
    expect(ratePerSecond({ value: 1, t: 5 }, { value: 9, t: 5 })).toBe(0);
  });
  it("never returns negative (counter reset / flush)", () => {
    expect(ratePerSecond({ value: 100, t: 0 }, { value: 5, t: 1000 })).toBe(0);
  });
});

describe("pushSample", () => {
  it("appends and caps the window length", () => {
    let s: number[] = [];
    for (let i = 0; i < 5; i++) s = pushSample(s, i, 3);
    expect(s).toEqual([2, 3, 4]);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd web && npx vitest run`
Expected: FAIL — `./format` and `./series` modules not found.

- [ ] **Step 3: Implement the modules**

`web/src/lib/format.ts`:
```ts
// formatCount abbreviates large counts (1240 -> "1.2k", 2_500_000 -> "2.5M").
export function formatCount(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return trim(n / 1000) + "k";
  return trim(n / 1_000_000) + "M";
}

function trim(x: number): string {
  return x.toFixed(1).replace(/\.0$/, "");
}

// formatAge renders an elapsed duration in ms as a coarse age ("5s", "1m", "1h").
export function formatAge(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h`;
}
```

`web/src/lib/series.ts`:
```ts
// A timestamped cumulative-counter sample.
export interface Sample {
  value: number;
  t: number; // unix ms
}

// ratePerSecond returns the per-second delta between two cumulative samples.
// Non-positive intervals and counter resets (decreases) yield 0, never negative.
export function ratePerSecond(prev: Sample, cur: Sample): number {
  const dt = (cur.t - prev.t) / 1000;
  if (dt <= 0) return 0;
  const dv = cur.value - prev.value;
  if (dv < 0) return 0;
  return dv / dt;
}

// pushSample appends v to a rolling window, keeping at most `cap` newest values.
export function pushSample(window: number[], v: number, cap: number): number[] {
  const next = [...window, v];
  return next.length > cap ? next.slice(next.length - cap) : next;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd web && npx vitest run` → all pass. `npm run typecheck` clean.

- [ ] **Step 5: Commit**

```bash
cd ..
git add web/src/lib
git commit -m "Add dashboard pure logic (format, series) with unit tests"
```

---

## Task 7: Data layer — `api.ts` types/calls and `useStream` hook

**Files:** `web/src/api.ts`, `web/src/hooks/useStream.ts`, `web/src/lib/snapshot.ts`, `web/src/lib/snapshot.test.ts`

- [ ] **Step 1: Write the failing test** — `web/src/lib/snapshot.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { indexByQueue, type QueueSnapshot } from "./snapshot";

const snap = (queue: string, ready: number): QueueSnapshot => ({
  queue, ready, inflight: 0, delayed: 0, dlq: 0, processed_total: 0, dead_total: 0,
});

describe("indexByQueue", () => {
  it("maps a snapshot array by queue name", () => {
    const m = indexByQueue([snap("emails", 2), snap("sms", 5)]);
    expect(m.emails.ready).toBe(2);
    expect(m.sms.ready).toBe(5);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd web && npx vitest run snapshot`
Expected: FAIL — `./snapshot` not found.

- [ ] **Step 3: Implement the data layer**

`web/src/lib/snapshot.ts`:
```ts
// QueueSnapshot is one queue's line in an /api/stream event (matches the Go
// queueSnapshot JSON).
export interface QueueSnapshot {
  queue: string;
  ready: number;
  inflight: number;
  delayed: number;
  dlq: number;
  processed_total: number;
  dead_total: number;
}

// indexByQueue turns a snapshot array into a name->snapshot map.
export function indexByQueue(snaps: QueueSnapshot[]): Record<string, QueueSnapshot> {
  const out: Record<string, QueueSnapshot> = {};
  for (const s of snaps) out[s.queue] = s;
  return out;
}
```

`web/src/api.ts`:
```ts
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
  return r.json();
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
  return r.json();
}
```

`web/src/hooks/useStream.ts`:
```ts
import { useEffect, useState } from "react";
import { indexByQueue, type QueueSnapshot } from "../lib/snapshot";

export interface StreamState {
  byQueue: Record<string, QueueSnapshot>;
  queues: string[];
  connected: boolean;
}

// useStream subscribes to /api/stream (SSE) and exposes the latest per-queue
// snapshot. It reconnects automatically (EventSource does this for us).
export function useStream(): StreamState {
  const [state, setState] = useState<StreamState>({ byQueue: {}, queues: [], connected: false });

  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.onopen = () => setState((s) => ({ ...s, connected: true }));
    es.onerror = () => setState((s) => ({ ...s, connected: false }));
    es.onmessage = (e) => {
      const snaps = JSON.parse(e.data) as QueueSnapshot[];
      setState({ byQueue: indexByQueue(snaps), queues: snaps.map((s) => s.queue), connected: true });
    };
    return () => es.close();
  }, []);

  return state;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd web && npx vitest run snapshot` → PASS. `npm run typecheck` clean.

- [ ] **Step 5: Commit**

```bash
cd ..
git add web/src/api.ts web/src/hooks web/src/lib/snapshot.ts web/src/lib/snapshot.test.ts
git commit -m "Add dashboard data layer: REST client and SSE stream hook"
```

---

## Task 8: Dashboard UI (dark editorial) + build

**Files:** `web/src/theme.css`, `web/src/App.tsx`, `web/src/main.tsx` (update), and `web/src/components/`: `Sidebar.tsx`, `StatTiles.tsx`, `Sparkline.tsx`, `Charts.tsx`, `DlqTable.tsx`, `EnqueueForm.tsx`. Rebuild `web/dist`.

This task is a UI translation of the approved dark-editorial mockup. There is no Go-style RED/GREEN; the gate is `npm run typecheck` + `npm run build` + visual correctness against the tokens below.

- [ ] **Step 1: Create `web/src/theme.css`** (the locked design tokens + base styles):

```css
@import "@fontsource/fraunces/400.css";
@import "@fontsource/fraunces/500.css";
@import "@fontsource/fraunces/600.css";
@import "@fontsource/ibm-plex-sans/400.css";
@import "@fontsource/ibm-plex-sans/500.css";
@import "@fontsource/ibm-plex-sans/600.css";
@import "@fontsource/ibm-plex-mono/400.css";
@import "@fontsource/ibm-plex-mono/500.css";

:root {
  color-scheme: dark;
  --bg: #15120e;
  --panel: #1c1813;
  --panel-2: #211c16;
  --line: #2e271e;
  --ink: #ece3d4;
  --muted: #9a8f7c;
  --faint: #6f6757;
  --accent: #d2603f;
  --accent-soft: rgba(210, 96, 63, 0.14);
  --serif: "Fraunces", Georgia, serif;
  --sans: "IBM Plex Sans", system-ui, sans-serif;
  --mono: "IBM Plex Mono", monospace;
}

* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); font-family: var(--sans); font-size: 14px; }
.app { display: grid; grid-template-columns: 236px 1fr; min-height: 100vh; max-width: 1180px; margin: 0 auto; }
/* (Carry over the sidebar/main/tile/panel/table/button rules from the approved
   mockup; match the token names above. Keep them in this single theme.css.) */
```

Translate the full mockup styling (sidebar, stat tiles, chart panels, DLQ table, requeue button, enqueue form) into `theme.css` using these exact tokens. The mockup's structure: a `.app` grid (236px sidebar + main), hairline (`--line`) borders, Fraunces for the wordmark/stat numbers/section headings, mono for labels/IDs/counts, terracotta (`--accent`) for the active queue marker, the DLQ tile, and primary actions. Match colors precisely.

- [ ] **Step 2: Implement `Sparkline.tsx`** (dependency-free SVG):

```tsx
interface SparklineProps {
  data: number[];
  stroke: string;
  fill?: string;
  height?: number;
}

// Sparkline draws a normalized polyline (and optional area) from data points.
export function Sparkline({ data, stroke, fill, height = 86 }: SparklineProps) {
  const w = 320;
  if (data.length < 2) {
    return <svg className="spark" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none" />;
  }
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const span = max - min || 1;
  const stepX = w / (data.length - 1);
  const pts = data.map((v, i) => {
    const x = i * stepX;
    const y = height - ((v - min) / span) * (height - 6) - 3;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const line = pts.join(" ");
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none">
      {fill && <polyline fill={fill} stroke="none" points={`0,${height} ${line} ${w},${height}`} />}
      <polyline fill="none" stroke={stroke} strokeWidth={2} points={line} />
    </svg>
  );
}
```

- [ ] **Step 3: Implement the remaining components and `App.tsx`**

Build these components (each one focused; props match the data layer):
- `Sidebar.tsx` — props `{ queues: string[]; byQueue: Record<string, QueueSnapshot>; selected: string; onSelect(q): void; onEnqueueClick(): void; connected: boolean }`. Renders the `Relay.` wordmark, the queue list (name + `formatCount(ready)`, active marker on `selected`), the `+ Enqueue a job` button, and the live footer.
- `StatTiles.tsx` — props `{ snap?: QueueSnapshot }`. Four tiles (Ready/In-flight/Delayed/Dead-letter); DLQ tile uses the accent style. Numbers via `formatCount`.
- `Charts.tsx` — props `{ depth: number[]; throughput: number[] }`. Two panels ("Queue depth", "Throughput") each wrapping a `Sparkline` (depth uses accent stroke + soft fill; throughput uses a muted gold stroke `#cbb48e`).
- `DlqTable.tsx` — props `{ jobs: DlqJob[]; onRequeue(id): void }`. Columns: Job ID (mono, shortened), Attempts (`{attempts}/{max_retries}`), Payload (preview), Age (`formatAge(Date.now() - Date.parse(created_at))`), and a Requeue button per row.
- `EnqueueForm.tsx` — props `{ queue: string; onClose(): void; onEnqueued(): void }`. A small modal/inline form: payload (textarea), optional priority (number), delay_ms (number), idempotency_key (text); submits via `enqueue(...)`.

`App.tsx` wiring:
- `const stream = useStream();` derive `queues = stream.queues`.
- Local state: `selected` (default first queue), `depthWindow`/`throughputWindow` (`number[]`, capped via `pushSample`, e.g. cap 60), `prevSample` (for `ratePerSecond` on `processed_total`), `dlqJobs`, `showEnqueue`.
- On each new snapshot for `selected`: push `ready` into the depth window; compute throughput via `ratePerSecond(prevSample, {value: processed_total, t: Date.now()})` and push into the throughput window; update `prevSample`.
- Fetch the DLQ list (`listDlq(selected)`) when `selected` changes and after a requeue/enqueue, and on a slow timer (e.g. every 5s).
- Render `.app` → `<Sidebar/>` + main (`<StatTiles/>`, `<Charts/>`, `<DlqTable/>`), plus `<EnqueueForm/>` when `showEnqueue`.

Update `web/src/main.tsx` to import `./theme.css` and render `<App/>`:
```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./theme.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

- [ ] **Step 4: Typecheck, build, rebuild dist**

Run:
```bash
cd web
npm run typecheck
npm run build
```
Expected: typecheck clean; `web/dist` regenerated with the real UI. Optionally `npm run dev` and eyeball it against a running `cmd/server` (Task 10) if a local Redis is up.

- [ ] **Step 5: Commit**

```bash
cd ..
git add web/src web/dist
git commit -m "Implement dark-editorial dashboard UI and rebuild dist"
```

---

## Task 9: Embed package (`web/embed.go`) + serving handler

**Files:** `web/embed.go`, `web/handler_test.go`

- [ ] **Step 1: Write the failing test** — `web/handler_test.go`:

```go
package web_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/StrangeNoob/relay/web"
)

func TestHandlerServesIndex(t *testing.T) {
	srv := httptest.NewServer(web.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
}

func TestHandlerSpaFallback(t *testing.T) {
	srv := httptest.NewServer(web.Handler())
	defer srv.Close()

	// A client-side route that is not a real asset must still return index.html (200).
	resp, err := http.Get(srv.URL + "/queues/emails")
	if err != nil {
		t.Fatalf("GET /queues/emails: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (SPA fallback)", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./web/ -run TestHandler -v`
Expected: FAIL — package `web` has no `Handler` (and no `dist` embed yet). (`web/dist` must exist from Task 5/8; if it does not, complete those first.)

- [ ] **Step 3: Create `web/embed.go`**:

```go
// Package web embeds the built dashboard (web/dist) and serves it with an SPA
// fallback. The Vite build output is committed so `go build` needs no Node step.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
)

//go:embed all:dist
var dist embed.FS

// assets returns the embedded files rooted at dist/.
func assets() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic("web: embed dist subtree: " + err.Error())
	}
	return sub
}

// Handler serves the dashboard. Real asset paths are served directly; any other
// path falls back to index.html so client-side routing works (single-page app).
func Handler() http.Handler {
	root := assets()
	fileServer := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if clean == "." || clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(root, clean); err != nil {
			// Not a real asset — serve the SPA shell.
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
```

Add `"strings"` to the import block (used by `Handler`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./web/ -run TestHandler -v` → PASS (2). `gofmt -l web/`, `go build ./...`, `go vet ./web/` clean.

- [ ] **Step 5: Commit**

```bash
git add web/embed.go web/handler_test.go
git commit -m "Embed and serve the dashboard SPA with index.html fallback"
```

---

## Task 10: Serve the dashboard from `cmd/server`

**Files:** `cmd/server/main.go`

- [ ] **Step 1: Wire the SPA route** — add the import and the `/` handler. Add to imports:

```go
	"github.com/StrangeNoob/relay/web"
```

After the `/healthz` registration, add:

```go
	// Serve the embedded dashboard at / (SPA fallback). Registered last and at the
	// root, so the more specific /api/, /metrics, /healthz patterns take priority.
	mux.Handle("/", web.Handler())
```

- [ ] **Step 2: Build, vet, format**

Run:
```bash
go build ./...
go vet ./...
gofmt -l cmd/ internal/ web/
```
Expected: all clean.

- [ ] **Step 3: Smoke check (optional, needs local Redis)**

Run, then Ctrl-C:
```bash
go run ./cmd/server -addr :8080 &
sleep 1
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/                 # 200 (index.html)
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/queues/emails    # 200 (SPA fallback)
curl -s localhost:8080/healthz; echo                                     # ok
curl -s -N localhost:8080/api/stream & sleep 2; kill %2                  # streams "data: [...]"
kill %1
```
Expected: `200`, `200`, `ok`, and at least one `data:` SSE line. Skip if no Redis.

- [ ] **Step 4: Commit**

```bash
git add cmd/server/main.go
git commit -m "Serve the embedded dashboard from cmd/server at /"
```

---

## Task 11: CI frontend job, CLAUDE.md, final verification

**Files:** `.github/workflows/ci.yml`, `CLAUDE.md`

- [ ] **Step 1: Add a frontend CI job** — append to `.github/workflows/ci.yml` under `jobs:`:

```yaml
  web:
    name: dashboard (build & test)
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: web
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
          cache-dependency-path: web/package-lock.json

      - name: Install
        run: npm ci

      - name: Typecheck
        run: npm run typecheck

      - name: Test
        run: npm run test

      - name: Build
        run: npm run build

      # Fail if the committed web/dist is stale vs a fresh build of the source.
      - name: Verify committed dist is in sync
        run: git diff --exit-code -- dist
```

- [ ] **Step 2: Update `CLAUDE.md`**

Make these edits (match the file's wording):
1. **Status line** — note 3b (dashboard) is done: Phase 3 in progress — 3a HTTP API ✅, 3b dashboard ✅; 3c SDK, 3d packaging remain.
2. **"What exists today" list** — add: `web/` (Vite+React+TS dark-editorial dashboard, embedded via `web/embed.go`, served at `/` by `cmd/server`); the SSE endpoint `GET /api/stream`; the new broker `Counters` method; and note `ack.lua`/`nack.lua` now `INCR` the `processed`/`dead` counters.
3. **Redis data model table** — add rows `q:{name}:processed` (string counter, INCR on ack) and `q:{name}:dead` (string counter, INCR on dead-letter); note they back dashboard throughput.
4. **Layout (✅/◻)** — mark `web/` ✅ and add `web/embed.go`; leave `internal/client`, `deployments/` as ◻.
5. **Build order** — Phase 3: 3a ✅, 3b ✅; 3c SDK, 3d packaging remain.
6. **Known limitations** — add: dashboard charts are in-memory (reset on reload); `processed`/`dead` counters are monotonic (no reset); SSE is per-connection; committed `web/dist` must be rebuilt on UI change (CI verifies).
7. **Build & dependencies** — note the `web/` workspace builds with Node/Vite but the Go module gains no dependency; `go build ./...` uses the committed `web/dist`.
8. **Run commands** — add `go run ./cmd/server` then open `http://localhost:8080`.

- [ ] **Step 3: Full verification**

Run:
```bash
go build ./...
go test -race ./...
go vet ./...
gofmt -l internal/ cmd/ web/
( cd web && npm run typecheck && npm run test && npm run build && git diff --exit-code -- dist )
```
Expected: Go build/tests/vet/fmt clean (broker DB 15, worker DB 14, metrics DB 13, api DB 12, web no-Redis — all pass); frontend typecheck/test/build clean and dist in sync. Tests need Redis at localhost:6379.

If anything fails, STOP and report.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml CLAUDE.md
git commit -m "Document Phase 3b and add the dashboard CI job"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** processed counter (Task 1), dead counter (Task 2), `Counters` (Task 3), SSE `/api/stream` (Task 4), Vite+React scaffold + committed dist (Task 5), pure logic + tests (Task 6), REST client + SSE hook + snapshot reducer (Task 7), dark-editorial UI + sparklines (Task 8), `web/embed.go` + SPA fallback (Task 9), `cmd/server` serving (Task 10), CI frontend job + CLAUDE.md (Task 11). Maps to every spec section (frontend stack, aesthetic, SSE, throughput-via-Redis-counters, embed, testing, CI, data model, known limitations).
- **Type consistency:** Go `Counters{Processed,Dead int64}` (json `processed_total`/`dead_total`) matches the SSE `queueSnapshot` fields and the TS `QueueSnapshot` interface. `processedKey`/`deadKey` match the Lua `KEYS` they are passed into (ack KEYS[2]; nack KEYS[4]). `web.Handler()` matches `cmd/server` and `web/handler_test.go`. Test DBs unchanged (broker 15, worker 14, metrics 13, api 12; web needs no Redis).
- **No placeholders:** Go and pure-logic steps carry complete code. Task 8 (UI) is an explicit translation of the locked tokens/mockup with full component contracts, `Sparkline`/`theme.css`/`main.tsx` code given and the remaining small components specified by props + behavior — appropriate for a mockup-driven UI build.
- **Known soft spots:** frontend tasks require Node + npm registry (flagged BLOCKED-if-unavailable). `http.FileServerFS`/`http.FileServerFS` and `fs.Sub` require Go 1.22+ (the module is on 1.24/1.25). The dist-in-sync CI check assumes a deterministic Vite build; if hashing differs across environments, relax the check to building (not diffing) and note it.
