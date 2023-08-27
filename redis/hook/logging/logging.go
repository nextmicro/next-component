package logging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-volo/logger"
	"github.com/nextmicro/gokit/timex"
	rediscmd "github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
)

const (
	component = "redis"
)

type Option func(o *options)

type options struct {
	Request       bool
	Response      bool
	SlowThreshold time.Duration
}

type logging struct {
	opt *options
}

func WithRequest(v bool) Option {
	return func(o *options) {
		o.Request = v
	}
}

func WithResponse(v bool) Option {
	return func(o *options) {
		o.Response = v
	}
}

func WithSlowThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.SlowThreshold = threshold
	}
}

func NewLogging(opts ...Option) redis.Hook {
	opt := &options{
		Request:       false,
		Response:      false,
		SlowThreshold: time.Millisecond * 100,
	}
	for _, o := range opts {
		o(opt)
	}

	return &logging{opt: opt}
}

func response(cmd redis.Cmder) string {
	switch cmd.(type) {
	case *redis.Cmd:
		return fmt.Sprintf("%v", cmd.(*redis.Cmd).Val())
	case *redis.StringCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StringCmd).Val())
	case *redis.StatusCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StatusCmd).Val())
	case *redis.IntCmd:
		return fmt.Sprintf("%v", cmd.(*redis.IntCmd).Val())
	case *redis.DurationCmd:
		return fmt.Sprintf("%v", cmd.(*redis.DurationCmd).Val())
	case *redis.BoolCmd:
		return fmt.Sprintf("%v", cmd.(*redis.BoolCmd).Val())
	case *redis.CommandsInfoCmd:
		return fmt.Sprintf("%v", cmd.(*redis.CommandsInfoCmd).Val())
	case *redis.StringSliceCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StringSliceCmd).Val())
	default:
		return ""
	}
}

func (l *logging) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		now := time.Now()
		conn, err := hook(ctx, network, addr)
		duration := time.Since(now)
		fields := map[string]interface{}{
			"kind":      "db",
			"component": component,
			"network":   network,
			"addr":      addr,
			"duration":  timex.Duration(duration),
		}
		if err != nil {
			fields["error"] = err
		}
		log := logger.WithContext(ctx).WithFields(fields)
		if duration > l.opt.SlowThreshold {
			log.Info("[REDIS] Dial Client Slow")
		} else if err != nil {
			log.Error("[REDIS] Dial Client")
		} else {
			log.Info("[REDIS] Dial Client")
		}
		return conn, err
	}
}

func (l *logging) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		now := time.Now()

		err := hook(ctx, cmd)
		duration := time.Since(now)
		fields := map[string]interface{}{
			"kind":      "db",
			"component": component,
			"method":    cmd.FullName(),
			"sql":       rediscmd.CmdStrins(cmd),
			"duration":  timex.Duration(duration),
		}
		if l.opt.Request {
			fields["request"] = cmd.Args()
		}
		if l.opt.Response {
			fields["response"] = response(cmd)
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			fields["error"] = err
		}

		log := logger.WithContext(ctx).WithFields(fields)
		if duration > l.opt.SlowThreshold {
			log.Info("[REDIS] Client Slow")
		}

		if err != nil && !errors.Is(err, redis.Nil) {
			log.Error("[REDIS] Client")
		} else {
			log.Info("[REDIS] Client")
		}
	}
}

func (l *logging) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) == 0 {
			return nil
		}

		now := time.Now()
		err := hook(ctx, cmds)

		cmd := cmds[0]
		cmdName, sql := rediscmd.CmdsString(cmds)

		duration := time.Since(now)
		fields := map[string]interface{}{
			"kind":      "db",
			"component": component,
			"method":    cmdName,
			"statement": sql,
			"duration":  timex.Duration(duration),
		}
		if l.opt.Request {
			fields["request"] = cmd.Args()
		}
		if l.opt.Response {
			fields["response"] = response(cmd)
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			fields["error"] = err
		}

		log := logger.WithContext(ctx).WithFields(fields)
		if duration > l.opt.SlowThreshold {
			log.Info("redis client slow")
		}

		if err != nil && !errors.Is(err, redis.Nil) {
			log.Error("redis client")
		} else {
			log.Info("redis client")
		}

		return nil
	}
}
