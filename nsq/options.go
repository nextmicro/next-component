package nsq

import (
	"context"

	"github.com/nextmicro/next/runtime/loader"
)

const (
	namespace   = "nsq"
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
	Producer *Producer `json:"producer"` // 生产者配置
	Consumer *Consumer `json:"consumer"` // 消费者配置
}

type Producer struct {
	Addr string `json:"addr"` // nsqd 地址
}

type Consumer struct {
	Addr    string `json:"addr"`    // nsqlookupd 地址
	Topic   string `json:"topic"`   // 主题
	Channel string `json:"channel"` // 频道
}

type nsqConfig struct{}

// WithConfig sets the nsq config
func WithConfig(cfg Options) loader.Option {
	return func(o *loader.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, nsqConfig{}, cfg)
	}
}

// Options returns the nsq config.
func (o *Options) Options() []Option {
	return []Option{}
}
