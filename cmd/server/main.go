// Command server runs Relay's HTTP control surface: the JSON API plus a
// Prometheus /metrics endpoint and a health check. It is a thin wiring layer;
// all behaviour lives in internal/api, internal/broker, and internal/metrics.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/StrangeNoob/relay/internal/api"
	"github.com/StrangeNoob/relay/internal/broker"
	"github.com/StrangeNoob/relay/internal/metrics"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	redisAddr := flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
	queuesFlag := flag.String("queues", "", "comma-separated queues for the /metrics depth collector")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Error("cannot reach redis", "addr", *redisAddr, "err", err)
		os.Exit(1)
	}

	// A metrics recorder on the broker means API enqueues are counted; a depth
	// collector for the configured queues exposes live gauges from the server.
	rec := metrics.NewRecorder()
	if qs := splitQueues(*queuesFlag); len(qs) > 0 {
		rec.Registry().MustRegister(metrics.NewDepthCollector(rdb, qs...))
	}
	b := broker.New(rdb, broker.WithMetrics(rec))

	mux := http.NewServeMux()
	mux.Handle("/api/", api.New(b, logger))
	mux.Handle("/metrics", promhttp.HandlerFor(rec.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		logger.Info("relay server listening", "addr", *addr, "redis", *redisAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("server shutdown error", "err", err)
	}
	logger.Info("relay server stopped cleanly")
}

// envOr returns the environment value for key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitQueues parses a comma-separated queue list, trimming blanks.
func splitQueues(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
