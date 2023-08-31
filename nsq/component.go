package nsq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-volo/logger"
	"github.com/nextmicro/next/config"
	"github.com/nextmicro/next/runtime/loader"
	nsq "github.com/nsqio/go-nsq"
)

var (
	Nsq *Component
	_   loader.Loader = &Component{}
)

type Component struct {
	opts     map[string]*Options
	open     bool
	config   *nsq.Config
	options  []Option
	producer sync.Map
	consumer sync.Map
}

func New(options ...Option) *Component {
	Nsq = &Component{
		options: options,
		config:  nsq.NewConfig(),
		opts:    make(map[string]*Options),
	}
	return Nsq
}

// Producer 获取生产者
func (c *Component) Producer(name ...string) *nsq.Producer {
	group := defaultName
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}

	value, ok := c.producer.Load(group)
	if !ok {
		logger.Fatalf("nsq: producer not found, group: %s", group)
	}

	return value.(*nsq.Producer)
}

// NewConsumer 获取消费者
func (c *Component) NewConsumer(name ...string) *nsq.Consumer {
	group := defaultName
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}

	value, ok := c.producer.Load(group)
	if !ok {
		logger.Fatalf("nsq: consumer not found, group: %s", group)
	}

	return value.(*nsq.Consumer)
}

func (c *Component) Init(opts ...loader.Option) error {
	err := config.Value(namespace).Scan(&c.opts)
	if err != nil {
		return fmt.Errorf("redis: %s", err)
	}

	op := loader.Options{}
	for _, opt := range opts {
		opt(&op)
	}

	if op.Context != nil {
		cfg, ok := op.Context.Value(nsqConfig{}).(Options)
		if !ok {
			return errors.New("nsq: config not found")
		}

		for _, option := range cfg.Options() {
			for _, opt := range c.opts {
				option.apply(opt)
			}
		}
	}

	for _, option := range c.options {
		for _, opt := range c.opts {
			option.apply(opt)
		}
	}

	if len(c.opts) == 0 {
		return nil
	}

	for name, opt := range c.opts {
		if opt.Producer != nil {
			p, err := nsq.NewProducer(opt.Producer.Addr, c.config)
			if err != nil {
				return fmt.Errorf("nsq: NewProducer %w", err)
			}
			if err = p.Ping(); err != nil {
				return fmt.Errorf("nsq: Ping error %w", err)
			}
			c.producer.Store(name, p)
		}
		if opt.Consumer != nil {
			cm, err := nsq.NewConsumer(opt.Consumer.Topic, opt.Consumer.Channel, c.config)
			if err != nil {
				return fmt.Errorf("nsq: NewConsumer %w", err)
			}

			err = cm.ConnectToNSQLookupd(opt.Consumer.Addr)
			if err != nil {
				return fmt.Errorf("nsq: ConnectToNSQLookupd %w", err)
			}
			c.consumer.Store(name, cm)
		}
	}

	c.open = true
	logger.Infof("Component [%s] Init success", c.String())
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	logger.Infof("Component [%s] Start success", c.String())
	return nil
}

func (c *Component) Watch() error {
	return nil
}

func (c *Component) Stop(ctx context.Context) error {
	// stop the producers
	c.producer.Range(func(key, value interface{}) bool {
		client := value.(*nsq.Producer)
		_ = client.Stop
		return true
	})
	c.producer = sync.Map{}

	// stop the consumers
	c.consumer.Range(func(key, value interface{}) bool {
		client := value.(*nsq.Consumer)
		_ = client.Stop
		return true
	})
	c.consumer = sync.Map{}

	logger.Infof("Component [%s] stop success", c.String())
	return nil
}

func (c *Component) String() string {
	return namespace
}
