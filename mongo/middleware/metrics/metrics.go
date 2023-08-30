package metrics

import (
	"context"
	"errors"
	"time"

	prom "github.com/go-kratos/kratos/contrib/metrics/prometheus/v2"
	"github.com/nextmicro/next-component/mongo/middleware"
	"github.com/nextmicro/next/pkg/metrics"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel/codes"
)

// Client mongo metrics.
func Client(opts ...Option) middleware.Middleware {
	op := &options{
		requests: prom.NewCounter(metrics.DBSystemMetricRequests),
		seconds:  prom.NewHistogram(metrics.DBSystemMetricMillisecond),
	}
	for _, opt := range opts {
		opt(op)
	}

	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, cmdName string, req ...interface{}) (reply interface{}, err error) {
			start := time.Now()

			reply, err = handler(ctx, cmdName, req...)
			if err != nil {
				return
			}

			var code = codes.Ok
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				code = codes.Error
			}

			op.requests.With(component, op.name, op.addr, cmdName, code.String()).Inc()
			op.seconds.With(component, op.name, op.addr, cmdName).Observe(float64(time.Since(start).Milliseconds()))

			return
		}
	}
}
