package metrics

import (
	"context"
	"errors"
	"net"
	"time"

	prom "github.com/go-kratos/kratos/contrib/metrics/prometheus/v2"
	"github.com/nextmicro/next/pkg/metrics"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
)

const component = "redis"

type MetricHook struct {
	opt *options
}

func NewMetricHook(opts ...Option) redis.Hook {
	cfg := &options{
		requests: prom.NewCounter(metrics.DBSystemMetricRequests),
		seconds:  prom.NewHistogram(metrics.DBSystemMetricMillisecond),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return &MetricHook{
		opt: cfg,
	}
}

func (m *MetricHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return hook(ctx, network, addr)
	}
}

func (m *MetricHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		var (
			code = codes.Ok
		)
		now := time.Now()
		err := hook(ctx, cmd)
		if err != nil && !errors.Is(err, redis.Nil) {
			code = codes.Error
		}

		m.opt.requests.With(component, m.opt.name, m.opt.addr, cmd.Name(), code.String()).Inc()
		m.opt.seconds.With(component, m.opt.name, m.opt.addr, cmd.Name()).Observe(float64(time.Since(now).Milliseconds()))

		return err
	}
}

func (m *MetricHook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		var (
			cmder = cmds[0]
			code  = codes.Ok
		)
		now := time.Now()
		err := hook(ctx, cmds)
		if err != nil && !errors.Is(err, redis.Nil) {
			code = codes.Error
		}

		m.opt.requests.With(component, m.opt.name, m.opt.addr, cmder.Name(), code.String()).Inc()
		m.opt.seconds.With(component, m.opt.name, m.opt.addr, cmder.Name()).Observe(float64(time.Since(now).Milliseconds()))

		return err
	}
}
