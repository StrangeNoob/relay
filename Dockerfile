# syntax=docker/dockerfile:1

# --- builder ---
FROM golang:1.25 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Build all three binaries. web/dist is committed, so the server embeds the
# dashboard with no Node step.
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo   ./cmd/demo

# --- runtime ---
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/demo   /usr/local/bin/demo
EXPOSE 8080
# Each compose service overrides `command`; default to the server.
CMD ["/usr/local/bin/server"]
