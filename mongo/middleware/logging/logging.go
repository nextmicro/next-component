package logging

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/go-volo/logger"
	"github.com/nextmicro/gokit/timex"
	"github.com/nextmicro/next-component/mongo/middleware"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func Client(opts ...Option) middleware.Middleware {
	op := &options{
		SlowThreshold: 100 * time.Millisecond,
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

			duration := time.Since(start)
			fields := map[string]interface{}{
				"kind":      "db",
				"component": component,
				"method":    cmdName,
				"duration":  timex.Duration(duration),
			}
			if op.request && len(req) > 0 {
				fields["statement"] = sanitizeRequest(req...)
			}
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				fields["error"] = err
			}

			log := logger.WithContext(ctx).WithFields(fields)
			if duration > op.SlowThreshold {
				log.Info("mongodb client slow")
			}

			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				log.Error("mongodb client")
			} else {
				log.Info("mongodb client")
			}

			return
		}
	}
}

func sanitizeRequest(request ...interface{}) map[string]interface{} {
	ret := make(map[string]interface{}, len(request))
	for i, req := range request {
		switch v := req.(type) {
		case bson.D:
			ret["args_"+strconv.Itoa(i)] = v.Map()
		case bson.M:
			ret["args_"+strconv.Itoa(i)] = v
		case bson.E:
			ret["args_"+strconv.Itoa(i)] = v
		case bson.A:
			ret["args_"+strconv.Itoa(i)] = v
		default:
			ret["args_"+strconv.Itoa(i)] = v
		}
	}

	return ret
}
