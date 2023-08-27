package redis

import (
	"context"
	"time"

	"github.com/nextmicro/next/runtime/loader"
	redis "github.com/redis/go-redis/v9"
)

type Option interface {
	apply(*Options)
}

type OptionFunc func(*Options)

func (fn OptionFunc) apply(cfg *Options) {
	fn(cfg)
}

type Options struct {
	Addrs                 []string      `json:"addrs"`                   // 单个地址或者集群地址
	ClientName            string        `json:"client_name"`             // ClientName 将为每个 conn 执行 `CLIENT SETNAME ClientName` 命令
	Password              string        `json:"password"`                // Password 密码
	DB                    int           `json:"db"`                      // DB，默认为0, 一般应用不推荐使用DB分片
	PoolSize              int           `json:"pool_size"`               // PoolSize 集群内每个节点的最大连接池限制 默认每个CPU10个连接
	MaxRetries            int           `json:"max_retries"`             // MaxRetries 网络相关的错误最大重试次数 默认5次
	MinIdleConns          int           `json:"min_idle_conns"`          // MinIdleConns 最小空闲连接数 默认20个
	DialTimeout           time.Duration `json:"dial_timeout"`            // DialTimeout 拨超时时间
	ReadTimeout           time.Duration `json:"read_timeout"`            // ReadTimeout 读超时
	WriteTimeout          time.Duration `json:"write_timeout"`           // WriteTimeout 写超时
	IdleTimeout           time.Duration `json:"idle_timeout"`            // IdleTimeout 连接最大空闲时间，默认60s, 超过该时间，连接会被主动关闭
	SlowThreshold         time.Duration `json:"slow_threshold"`          // 慢日志门限值，超过该门限值的请求，将被记录到慢日志中
	DisableMetric         bool          `json:"disable_metric"`          // 禁用监控，默认开启
	DisableTrace          bool          `json:"disable_trace"`           // 禁用链路，默认开启
	DisableLogging        bool          `json:"disable_logging"`         // 禁用链路，记录请求数据
	EnableLoggingRequest  bool          `json:"enable_logging_request"`  // 是否开启记录请求参数
	EnableLoggingResponse bool          `json:"enable_logging_response"` // 是否开启记录响应参数
	Hooks                 []redis.Hook  `json:"-"`                       // redis钩子
}

const (
	namespace   = "redis"
	defaultName = "default"
)

type redisConfig struct{}

// WithConfig sets the redis config
func WithConfig(cfg Options) loader.Option {
	return func(o *loader.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, redisConfig{}, cfg)
	}
}

// Options returns the redis config.
func (o *Options) Options() []Option {
	opts := make([]Option, 0, 10)
	if o.Address != "" {
		opts = append(opts, WithAddress(o.Address))
	}
	if o.Password != "" {
		opts = append(opts, WithPassword(o.Password))
	}
	if o.DB != 0 {
		opts = append(opts, WithDB(o.DB))
	}
	if o.PoolSize != 0 {
		opts = append(opts, WithPoolSize(o.PoolSize))
	}
	if o.MaxRetries != 0 {
		opts = append(opts, WithMaxRetries(o.MaxRetries))
	}
	if o.MinIdleConns != 0 {
		opts = append(opts, WithMinIdleConns(o.MinIdleConns))
	}
	if o.DialTimeout != 0 {
		opts = append(opts, WithDialTimeout(o.DialTimeout))
	}
	if o.ReadTimeout != 0 {
		opts = append(opts, WithReadTimeout(o.ReadTimeout))
	}
	if o.WriteTimeout != 0 {
		opts = append(opts, WithWriteTimeout(o.WriteTimeout))
	}
	if o.IdleTimeout != 0 {
		opts = append(opts, WithIdleTimeout(o.IdleTimeout))
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
	if o.EnableLoggingRequest {
		opts = append(opts, WithEnableLoggingRequest())
	}
	if o.EnableLoggingResponse {
		opts = append(opts, WithEnableLoggingResponse())
	}
	if len(o.Hooks) > 0 {
		opts = append(opts, WithHook(o.Hooks...))
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

// WithDB 设置db 分片
func WithDB(db int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DB = db
	})
}

// WithPoolSize 设置连接池大小
func WithPoolSize(poolSize int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.PoolSize = poolSize
	})
}

// WithMaxRetries 设置最大重试次数
func WithMaxRetries(maxRetries int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.MaxRetries = maxRetries
	})
}

// WithMinIdleConns 设置最小空闲连接数
func WithMinIdleConns(minIdleConns int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.MinIdleConns = minIdleConns
	})
}

// WithDialTimeout 设置拨号超时时间
func WithDialTimeout(dialTimeout time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DialTimeout = dialTimeout
	})
}

// WithReadTimeout 设置读超时
func WithReadTimeout(readTimeout time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.ReadTimeout = readTimeout
	})
}

// WithWriteTimeout 设置写超时
func WithWriteTimeout(writeTimeout time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.WriteTimeout = writeTimeout
	})
}

// WithIdleTimeout 设置连接最大空闲时间
func WithIdleTimeout(idleTimeout time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.IdleTimeout = idleTimeout
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

// WithHook 设置钩子
func WithHook(hook ...redis.Hook) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Hooks = append(cfg.Hooks, hook...)
	})
}
