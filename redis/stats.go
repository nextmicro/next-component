package redis

import (
	"context"
	"strings"
	"time"

	prom "github.com/go-kratos/kratos/contrib/metrics/prometheus/v2"
	"github.com/go-kratos/kratos/v2/metrics"
	"github.com/nextmicro/logger"
	m "github.com/nextmicro/next/pkg/metrics"
	"github.com/redis/go-redis/v9"
)

type Stat struct {
	interval time.Duration
	stats    metrics.Gauge
}

func NewStat(interval time.Duration) *Stat {
	return &Stat{
		interval: interval,
		stats:    prom.NewGauge(m.DBSystemStatsGauge),
	}
}

func (s *Stat) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			goto Close
		case <-ticker.C:
			Redis.clients.Range(func(key, val interface{}) bool {
				name := key.(string)
				obj := val.(*redis.Client)
				stats := obj.PoolStats()
				addrs := strings.Join(Redis.opts[name].Addrs, ",")
				s.stats.With(namespace, name, addrs, "hits").Set(float64(stats.Hits))
				s.stats.With(namespace, name, addrs, "misses").Set(float64(stats.Misses))
				s.stats.With(namespace, name, addrs, "timeouts").Set(float64(stats.Timeouts))
				s.stats.With(namespace, name, addrs, "total_conns").Set(float64(stats.TotalConns))
				s.stats.With(namespace, name, addrs, "idle_conns").Set(float64(stats.IdleConns))
				s.stats.With(namespace, name, addrs, "stale_conns").Set(float64(stats.StaleConns))
				return true
			})
		}
	}

Close:

	logger.Info("redis: stats metrics stop")
}
