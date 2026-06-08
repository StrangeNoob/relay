package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// scrapeTimeout bounds the Redis work done during one Prometheus scrape so a
// slow or unreachable Redis cannot hang the /metrics handler.
const scrapeTimeout = 5 * time.Second

// DepthCollector reports per-queue depth gauges by reading Redis at scrape time,
// so the values can never go stale between scrapes. It implements
// prometheus.Collector and is registered on the Recorder's registry.
type DepthCollector struct {
	rdb    *redis.Client
	queues []string
	desc   *prometheus.Desc
}

// NewDepthCollector builds a collector that reports depths for the given queues.
func NewDepthCollector(rdb *redis.Client, queues ...string) *DepthCollector {
	return &DepthCollector{
		rdb:    rdb,
		queues: queues,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "queue_depth"),
			"Number of jobs in a queue, by state.",
			[]string{"queue", "state"}, nil,
		),
	}
}

// Describe sends the single gauge descriptor. (Required by prometheus.Collector.)
func (c *DepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect reads ready/inflight/delayed (ZCARD) and dlq (LLEN) for each queue and
// emits one gauge sample per (queue, state). On a Redis error for a given query
// it skips that sample rather than reporting a misleading zero.
func (c *DepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	for _, q := range c.queues {
		c.emit(ch, q, "ready", c.rdb.ZCard(ctx, "q:"+q+":ready"))
		c.emit(ch, q, "inflight", c.rdb.ZCard(ctx, "q:"+q+":inflight"))
		c.emit(ch, q, "delayed", c.rdb.ZCard(ctx, "q:"+q+":delayed"))
		c.emit(ch, q, "dlq", c.rdb.LLen(ctx, "q:"+q+":dlq"))
	}
}

// emit turns one IntCmd result into a gauge sample, skipping it on error.
func (c *DepthCollector) emit(ch chan<- prometheus.Metric, queue, state string, cmd *redis.IntCmd) {
	n, err := cmd.Result()
	if err != nil {
		return // skip this sample; do not report a stale/zero depth
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), queue, state)
}
