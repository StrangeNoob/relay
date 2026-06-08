# Relay — Phase 3d: Packaging, Deploy & README

**Status:** Approved design · **Date:** 2026-06-09
**Parent spec:** [`2026-06-07-relay-distributed-task-queue-design.md`](2026-06-07-relay-distributed-task-queue-design.md)
**Depends on:** 3a HTTP API ✅, 3b dashboard ✅, 3c producer SDK ✅
**Phase:** 3 (polish) — fourth and final sub-project. Completing it closes the project's planned scope.

## Purpose

Make Relay runnable and presentable in one step: a multi-stage Dockerfile, a docker-compose stack
that runs the whole system end-to-end (Redis + server + workers + a load-generating demo), and the
portfolio README — the front door that explains what Relay is, shows its architecture, and tells a
visitor how to run it. The compose stack is the runnable "demo"; live hosting is documented but
remains the operator's manual step.

## Scope

In scope:

- `Dockerfile` (multi-stage) building all three binaries into one minimal image.
- `.dockerignore` to keep the build context lean.
- `deployments/docker-compose.yml` running redis + server + worker(s) + demo end-to-end.
- `README.md`: the project front page, including a mermaid architecture diagram, quickstart, local
  dev, feature list, invariants, a deploy section, and a link to the design docs.
- A CI job that builds the Docker image (so the Dockerfile cannot rot).

Out of scope: actually deploying to a live host (documented, but the operator runs it — no
credentials here); a LICENSE file (the author's IP choice); Kubernetes manifests; TLS/auth (the API
is demo-grade); pushing images to a registry.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Image | **One multi-stage Dockerfile, shared image** | All three binaries from one build; compose services run the right one via `command`. The server embeds the committed `web/dist`, so no Node step is needed. |
| Runtime base | **`gcr.io/distroless/static:nonroot`** | Tiny, no shell/package surface, ideal for `CGO_ENABLED=0` static Go binaries; runs as non-root. |
| Demo | **docker-compose stack is the demo** | `docker compose up` brings up the full system and the demo container generates load; the dashboard shows it live. Reproducible, no secrets. |
| Deploy | **Documented in README (Fly.io-style); operator runs it** | A live URL needs a hosting account/credentials not available here; the image + compose make deployment straightforward, and the README gives concrete steps. |
| Diagram | **Mermaid in the README** | Renders natively on GitHub, lives as diffable text, no binary asset to drift. |
| Dockerfile CI | **Add a `docker build` job** | Keeps the Dockerfile working as the code evolves; build only (no push), so no registry creds. |

## Components & changes

### `Dockerfile` (repo root, multi-stage)

```dockerfile
# Builder
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# web/dist is committed, so the server embeds the dashboard with no Node step.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo   ./cmd/demo

# Runtime
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/demo   /usr/local/bin/demo
EXPOSE 8080
# No fixed entrypoint binary: each compose service sets `command`. Default to the server.
CMD ["/usr/local/bin/server"]
```

The exact Go base tag should match `go.mod` (`go 1.24` / `toolchain go1.25.11`); use a `golang:1.25`
tag that resolves the pinned toolchain (the plan verifies the build works against the available
image).

### `.dockerignore`

Excludes at least: `.git`, `web/node_modules`, `.superpowers`, `docs`, `*.md` is **not** excluded
(README is fine to copy, it is small), local build artifacts. The key win is excluding
`web/node_modules` (large) and `.git`.

### `deployments/docker-compose.yml`

Services (all but redis built from the root `Dockerfile`):

- **redis** — `redis:7`, healthcheck `redis-cli ping` (interval/timeout/retries), so dependents wait
  for readiness.
- **server** — `command: ["/usr/local/bin/server", "-addr", ":8080", "-redis", "redis:6379",
  "-queues", "demo"]`; `ports: ["8080:8080"]`; `depends_on: redis (service_healthy)`.
- **worker** — `command: ["/usr/local/bin/worker", "-redis", "redis:6379", "-queue", "demo",
  "-concurrency", "4", "-fail-rate", "0.1"]`; `depends_on: redis (service_healthy)`. Scalable with
  `docker compose up --scale worker=N` (the worker is a competing consumer; multiple are safe).
- **demo** — `command: ["/usr/local/bin/demo", "-server", "http://server:8080", "-queue", "demo",
  "-count", "200"]`; `depends_on: server (service_started)`; `restart: "no"` (one-shot load).

Topology matches the system: the demo (SDK over HTTP) and server talk to the API; the server and
workers talk to Redis; workers are the competing consumers plus the reaper/promoter loops. The 10%
fail-rate produces retries and DLQ entries so the dashboard's DLQ + requeue are demonstrable.

### `README.md`

Sections, in order:

1. **Title + tagline + CI badge** — "Relay — a distributed task queue built from scratch on Redis,
   in Go." Badge points at the CI workflow.
2. **What it is** — portfolio/back-end showcase; the point is proving queue internals, not wrapping a
   library.
3. **Architecture** — a mermaid diagram: producer / SDK / demo → server (HTTP API + embedded
   dashboard + `/metrics`); server and workers ↔ Redis; workers run the claim loop + reaper +
   promoter. A short prose walk-through follows.
4. **Delivery semantics & invariants** — at-least-once (never exactly-once); the atomic claim is
   sacred (one Lua script); crash safety via the reaper (visibility deadline); build-from-scratch on
   Redis primitives.
5. **Features** — competing consumers, priority, delayed/scheduled, retries with full-jitter backoff,
   DLQ + inspect/requeue, visibility timeout + reaper, idempotency keys, per-queue rate limiting,
   Prometheus metrics, live dashboard, producer SDK.
6. **Quickstart** — `docker compose -f deployments/docker-compose.yml up --build`, then open
   `http://localhost:8080`; what you'll see (depth/throughput, DLQ to requeue); scale workers.
7. **Local development** — `go run ./cmd/server` / `./cmd/worker` / `./cmd/demo`; tests need Redis on
   `:6379`, run `go test -race ./...`; the dashboard is `web/` (Vite) with `npm` for changes.
8. **Project layout** — brief map of `internal/{job,broker,worker,metrics,api,client}`, `cmd/*`,
   `web/`, `deployments/`.
9. **Deploy** — container-host instructions (Fly.io-style: build/push the image, set `REDIS_ADDR`,
   run server + worker; point at a managed Redis). Noted as the operator's step.
10. **Design docs** — pointer to `docs/superpowers/specs/` (the authoritative designs) and a note
    that the spec is the source of truth.

### CI (`.github/workflows/ci.yml`)

Add a `docker` job: checkout → `docker build -t relay:ci .` (build only, no push). Keeps the
Dockerfile honest. The existing `test`, `lint`, and `web` jobs are unchanged.

## Testing / validation

Docker is available in the dev environment, so validation is real (not just review):

- `docker build -t relay:ci .` succeeds and produces all three binaries in the image.
- `docker compose -f deployments/docker-compose.yml config` validates the compose file.
- `docker compose ... up` brings the stack up: redis healthy → server up → workers consuming → demo
  enqueues 200 jobs. Then assert: `curl localhost:8080/healthz` → `ok`;
  `curl localhost:8080/api/queues` includes `demo`; `curl localhost:8080/api/queues/demo/stats`
  shows non-zero activity; `curl localhost:8080/` returns the dashboard HTML;
  `curl localhost:8080/metrics` has `relay_*` lines. Tear down with `docker compose down -v`.
- README: prose reviewed; mermaid block syntax sanity-checked (fenced ```mermaid```); links resolve.
- `go build ./...` and `go test -race ./...` remain green (no Go source changes in this phase beyond
  none expected; if the demo/server need a tweak for container DNS it is minimal).

## Invariants preserved

- No queue logic changes — this phase is packaging and docs. At-least-once, the atomic claim, and
  reaper crash-safety are untouched.
- No new Go dependency. The Docker image is built from the existing module; the README/compose add no
  code dependencies.

## Known limitations

- **No live hosting in-repo.** The compose stack is the demo; deploying to a public URL is the
  operator's step (documented).
- **Demo image runs as a one-shot.** The `demo` service exits after enqueuing; re-run with
  `docker compose run --rm demo ...` to generate more load.
- **distroless runtime has no shell.** Debugging inside the container needs `docker compose exec`
  alternatives (or a temporary alpine base); chosen deliberately for a minimal image.
- **Single Redis, no persistence tuning.** The compose Redis is ephemeral (no volume by default);
  fine for a demo, not a production data store.
