package mongo

import (
	"context"
	"time"

	"github.com/nextmicro/next-component/mongo/middleware"
	"github.com/nextmicro/next/runtime/loader"
)

const (
	namespace   = "mongo"
	defaultName = "default"
)

type Option interface {
	apply(*Options)
}

type OptionFunc func(*Options)

func (fn OptionFunc) apply(cfg *Options) {
	fn(cfg)
}

type Options struct {
	Address               string                  `json:"address"`                 // 数据库地址
	Username              string                  `json:"username"`                // 用户名
	Password              string                  `json:"password"`                // 密码
	Database              string                  `json:"database"`                // 数据库名称
	ReplicaSet            string                  `json:"replica_set"`             // 副本集
	DialTimeout           time.Duration           `json:"dial_timeout"`            // DialTimeout 拨超时时间
	PoolSize              uint64                  `json:"pool_size"`               // PoolSize 连接池大小(最大连接数)
	DisableMetric         bool                    `json:"disable_metric"`          // 是否禁用监控，默认开启
	DisableTrace          bool                    `json:"disable_trace"`           // 是否禁用链路追踪，默认开启
	DisableLogging        bool                    `json:"disable_logging"`         // 是否禁用，记录请求数据
	EnableLoggingRequest  bool                    `json:"enable_logging_request"`  // 是否开启记录请求参数
	EnableLoggingResponse bool                    `json:"enable_logging_response"` // 是否开启记录响应参数
	SlowThreshold         time.Duration           `json:"slow_threshold"`          // 慢日志门限值，超过该门限值的请求，将被记录到慢日志中
	Middlewares           []middleware.Middleware `json:"-"`                       // 中间件
}

type mongoConfig struct{}

// WithConfig sets the mongo config
func WithConfig(cfg Options) loader.Option {
	return func(o *loader.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, mongoConfig{}, cfg)
	}
}

// Options returns the mongo config.
func (o *Options) Options() []Option {
	opts := make([]Option, 0, 10)
	if o.Address != "" {
		opts = append(opts, WithAddress(o.Address))
	}
	if o.Password != "" {
		opts = append(opts, WithPassword(o.Password))
	}
	if o.Database != "" {
		opts = append(opts, WithDatabase(o.Database))
	}
	if o.ReplicaSet != "" {
		opts = append(opts, WithReplicaSet(o.ReplicaSet))
	}
	if o.PoolSize != 0 {
		opts = append(opts, WithPoolSize(o.PoolSize))
	}
	if o.DialTimeout != 0 {
		opts = append(opts, WithDialTimeout(o.DialTimeout))
	}
	if o.EnableLoggingRequest {
		opts = append(opts, WithEnableLoggingRequest())
	}
	if o.EnableLoggingResponse {
		opts = append(opts, WithEnableLoggingResponse())
	}
	if o.SlowThreshold != 0 {
		opts = append(opts, WithSlowThreshold(o.SlowThreshold))
	}
	if o.DisableMetric {
		opts = append(opts, WithDisableMetric())
	}
	if o.DisableTrace {
		opts = append(opts, WithDisableTrace())
	}
	if o.DisableLogging {
		opts = append(opts, WithDisableLogging())
	}
	if len(o.Middlewares) > 0 {
		opts = append(opts, Middleware(o.Middlewares...))
	}
	return opts
}

// WithAddress 设置地址
func WithAddress(address string) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Address = address
	})
}

// WithPassword 设置密码
func WithPassword(password string) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Password = password
	})
}

// WithDatabase 设置数据库名称
func WithDatabase(db string) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Database = db
	})
}

// WithReplicaSet 设置副本集
func WithReplicaSet(replicaSet string) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.ReplicaSet = replicaSet
	})
}

// WithPoolSize 设置连接池大小
func WithPoolSize(poolSize uint64) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.PoolSize = poolSize
	})
}

// WithDialTimeout 设置拨号超时时间
func WithDialTimeout(dialTimeout time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DialTimeout = dialTimeout
	})
}

// WithEnableLoggingRequest 设置开启记录请求参数
func WithEnableLoggingRequest() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.EnableLoggingRequest = true
	})
}

// WithEnableLoggingResponse 设置开启记录响应参数
func WithEnableLoggingResponse() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.EnableLoggingResponse = true
	})
}

// WithSlowThreshold 设置慢日志门限值
func WithSlowThreshold(slowThreshold time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.SlowThreshold = slowThreshold
	})
}

// WithDisableMetric 设置禁用监控
func WithDisableMetric() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableMetric = true
	})
}

// WithDisableTrace 设置禁用链路
func WithDisableTrace() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableTrace = true
	})
}

// WithDisableLogging 设置禁用日志
func WithDisableLogging() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableLogging = true
	})
}

// Middleware with mongo middleware option.
func Middleware(m ...middleware.Middleware) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Middlewares = append(cfg.Middlewares, m...)
	})
}
