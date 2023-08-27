package gorm

import (
	"context"
	"time"

	"github.com/nextmicro/next/runtime/loader"
)

//{
//    "mysql": {
//        "default": {
//            "master": {
//                "database": "feed",
//                "address": "127.0.0.1:3306",
//                "username": "test",
//                "password": "test",
//                "charset": "utf8mb4",
//                "logging": true,
//                "location": "Local"
//            },
//            "slaves": [
//                {
//                    "database": "feed_1",
//                    "address": "127.0.0.1:3306",
//                    "username": "test",
//                    "password": "1223",
//                    "charset": "utf8mb4",
//                    "logging": true,
//                    "location": "Local"
//                },
//                {
//                    "database": "feed_1",
//                    "address": "127.0.0.1",
//                    "username": "test",
//                    "password": "123",
//                    "charset": "utf8mb4",
//                    "logging": true,
//                    "location": "Local"
//                }
//            ]
//        }
//    }
//}

type gormConfig struct{}

type Option interface {
	apply(*Options)
}

type OptionFunc func(*Options)

func (fn OptionFunc) apply(cfg *Options) {
	fn(cfg)
}

type Options struct {
	Master           DSN           `json:"master"`             // 主库
	Slaves           []DSN         `json:"slaves"`             // 从库
	MaxIdleConns     int           `json:"max_idle_conns"`     // 最大空闲连接数，默认10
	MaxOpenConns     int           `json:"max_open_conns"`     // 最大活动连接数，默认100
	ConnMaxLifetime  time.Duration `json:"conn_max_lifetime"`  // 连接的最大存活时间，默认300s
	SlowLogThreshold time.Duration `json:"slow_log_threshold"` // 慢日志阈值，默认500ms
	DisableMetric    bool          `json:"disable_metric"`     // 是否禁用监控，默认开启
	DisableTrace     bool          `json:"disable_trace"`      // 是否禁用链路追踪，默认开启
	DisableLogging   bool          `json:"disable_logging"`    // 是否禁用，记录请求数据
}

type DSN struct {
	Address  string `json:"address"`  // 数据库地址
	Username string `json:"username"` // 用户名
	Password string `json:"password"` // 密码
	Database string `json:"database"` // 数据库名称
	Charset  string `json:"charset"`  // 字符集
	Location string `json:"location"` // 时区
}

const (
	namespace   = "mysql"
	defaultName = "default"
)

// WithConfig sets the gorm config
func WithConfig(cfg Options) loader.Option {
	return func(o *loader.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, gormConfig{}, cfg)
	}
}

// Options returns the gorm config.
func (o *Options) Options() []Option {
	opts := make([]Option, 0, 10)
	if o.Master.Address != "" {
		opts = append(opts, WithMaster(o.Master))
	}
	if len(o.Slaves) > 0 {
		opts = append(opts, WithSlaves(o.Slaves))
	}
	if o.MaxIdleConns != 0 {
		opts = append(opts, WithMaxIdleConns(o.MaxIdleConns))
	}
	if o.MaxOpenConns != 0 {
		opts = append(opts, WithMaxOpenConns(o.MaxOpenConns))
	}
	if o.ConnMaxLifetime != 0 {
		opts = append(opts, WithConnMaxLifetime(o.ConnMaxLifetime))
	}
	if o.SlowLogThreshold != 0 {
		opts = append(opts, WithSlowLogThreshold(o.SlowLogThreshold))
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
	return opts
}

// WithMaster sets the master dsn.
func WithMaster(master DSN) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Master = master
	})
}

// WithSlaves sets the slaves dsn.
func WithSlaves(slaves []DSN) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.Slaves = slaves
	})
}

// WithMaxIdleConns sets the max idle conns for the database.
func WithMaxIdleConns(v int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.MaxIdleConns = v
	})
}

// WithMaxOpenConns sets the max open conns for the database.
func WithMaxOpenConns(v int) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.MaxOpenConns = v
	})
}

// WithConnMaxLifetime sets the max lifetime for the database.
func WithConnMaxLifetime(v time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.ConnMaxLifetime = v
	})
}

// WithSlowLogThreshold sets the slow log threshold for the database.
func WithSlowLogThreshold(v time.Duration) Option {
	return OptionFunc(func(cfg *Options) {
		cfg.SlowLogThreshold = v
	})
}

// WithDisableMetric disables the metric for the database.
func WithDisableMetric() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableMetric = true
	})
}

// WithDisableTrace disables the trace for the database.
func WithDisableTrace() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableTrace = true
	})
}

// WithDisableLogging disables the logging for the database.
func WithDisableLogging() Option {
	return OptionFunc(func(cfg *Options) {
		cfg.DisableLogging = true
	})
}
