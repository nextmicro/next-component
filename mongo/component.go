package mongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nextmicro/logger"
	"github.com/nextmicro/next-component/mongo/middleware"
	"github.com/nextmicro/next-component/mongo/middleware/logging"
	"github.com/nextmicro/next-component/mongo/middleware/metrics"
	"github.com/nextmicro/next/config"
	"github.com/nextmicro/next/runtime/loader"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
)

var (
	Mongo *Component
	_     loader.Loader = &Component{}
)

type Component struct {
	status        bool
	defaultDBName string
	options       []Option
	opts          map[string]*Options
	clients       sync.Map
}

func New(options ...Option) *Component {
	Mongo = &Component{
		options: options,
		opts:    make(map[string]*Options),
	}
	return Mongo
}

func (c *Component) Init(opts ...loader.Option) error {
	err := config.Value(namespace).Scan(&Mongo.opts)
	if err != nil {
		return fmt.Errorf("redis: %s", err)
	}

	op := loader.Options{}
	for _, opt := range opts {
		opt(&op)
	}

	if op.Context != nil {
		cfg, ok := op.Context.Value(mongoConfig{}).(Options)
		if !ok {
			return errors.New("mongo: config not found")
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
		client, err := c.connect(name, opt)
		if err != nil {
			return err
		}
		Mongo.clients.Store(name, client)
	}

	c.status = true

	logger.Infof("Component [%s] Init success", c.String())
	return nil
}

func (c *Component) Instance(name ...string) *Database {
	group := defaultName
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}

	value, ok := c.clients.Load(group)
	if !ok {
		logger.Fatalf("mongo: client not found, group: %s", group)
	}

	return value.(*Client).Database(c.defaultDBName)
}

// buildDns build dns.
func (c *Component) buildDns(cfg *Options) string {
	dns := fmt.Sprintf("mongodb://%s/%s?authSource=admin", cfg.Address, cfg.Database)
	if cfg.Username != "" && cfg.Password != "" {
		dns = fmt.Sprintf("mongodb://%s:%s@%s/%s?authSource=admin", cfg.Username, cfg.Password, cfg.Address, cfg.Database)
	}
	if cfg.ReplicaSet != "" {
		dns += fmt.Sprintf("&replicaSet=%s", cfg.ReplicaSet)
	}

	return dns
}

func (c *Component) connect(name string, cfg *Options) (*Client, error) {
	if name == defaultName {
		c.defaultDBName = cfg.Database
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 300 * time.Second
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 100
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = time.Millisecond * 100
	}

	clientOpts := options.Client()
	clientOpts.MaxPoolSize = &cfg.PoolSize
	clientOpts.SocketTimeout = &cfg.DialTimeout
	if !cfg.DisableTrace {
		clientOpts.Monitor = otelmongo.NewMonitor()
	}

	clientOpts.ApplyURI(c.buildDns(cfg))

	cc, err := Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, err
	}

	ms := make([]middleware.Middleware, 0, 2)

	// metrics
	if !cfg.DisableMetric {
		ms = append(ms, metrics.Client(
			metrics.WithAddr(cfg.Address),
			metrics.WithName(cfg.Database),
			metrics.WithDisabled(cfg.DisableMetric)),
		)
	}

	// logging
	if !cfg.DisableLogging {
		ms = append(ms, logging.Client(
			logging.WithRequest(cfg.EnableLoggingRequest),
			logging.WithResponse(cfg.EnableLoggingResponse),
			logging.WithDisabled(cfg.DisableLogging),
			logging.WithSlowThreshold(cfg.SlowThreshold),
		))
	}

	ms = append(ms, cfg.Middlewares...)
	cc.middleware(ms)
	err = cc.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return nil, err
	}

	logger.Infof("%s %s connected success", namespace, name)
	return cc, nil
}

func (c *Component) Open() bool {
	return c.status
}

func (c *Component) Start(ctx context.Context) error {
	logger.Infof("Component [%s] Start success", c.String())
	return nil
}

func (c *Component) Watch() error {
	return nil
}

func (c *Component) Stop(ctx context.Context) error {
	c.clients.Range(func(key, value interface{}) bool {
		client := value.(*Client)
		_ = client.Disconnect(ctx)
		return true
	})

	c.clients = sync.Map{}

	logger.Infof("Component [%s] stop success", c.String())
	return nil
}

func (c *Component) String() string {
	return namespace
}
