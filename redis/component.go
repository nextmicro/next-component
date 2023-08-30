package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-volo/logger"
	"github.com/nextmicro/next-component/redis/hook/logging"
	"github.com/nextmicro/next-component/redis/hook/metrics"
	"github.com/nextmicro/next/config"
	"github.com/nextmicro/next/runtime/loader"
	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

var (
	Redis *component
	_     loader.Loader = &component{}
)

type component struct {
	opts    map[string]*Options
	open    bool
	options []Option
	stat    *Stat
	clients sync.Map
}

func New(options ...Option) *component {
	Redis = &component{
		options: options,
		opts:    make(map[string]*Options),
	}
	return Redis
}

// Init 初始化
func (c *component) Init(opts ...loader.Option) error {
	err := config.Value(namespace).Scan(&c.opts)
	if err != nil {
		return fmt.Errorf("redis: %s", err)
	}

	op := loader.Options{}
	for _, opt := range opts {
		opt(&op)
	}

	if op.Context != nil {
		cfg, ok := op.Context.Value(redisConfig{}).(Options)
		if !ok {
			return errors.New("redis: config not found")
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

		Redis.clients.Store(name, client)
	}

	c.stat = NewStat(time.Second * 30)

	c.open = true
	logger.Infof("Component [%s] Init success", c.String())
	return nil
}

func (c *component) Instance(name ...string) redis.UniversalClient {
	group := defaultName
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}

	value, ok := c.clients.Load(group)
	if !ok {
		logger.Fatalf("redis: client not found, group: %s", group)
	}

	return value.(redis.UniversalClient)
}

func peerInfo(addr string) (hostname string, port int) {
	if idx := strings.IndexByte(addr, ':'); idx >= 0 {
		hostname = addr[:idx]
		port, _ = strconv.Atoi(addr[idx+1:])
	}
	return hostname, port
}

func (c *component) connect(name string, cfg *Options) (redis.UniversalClient, error) {
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.MinIdleConns == 0 {
		cfg.MinIdleConns = 20
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = time.Millisecond * 100
	}
	if !cfg.DisableMetric {
		cfg.Hooks = append(cfg.Hooks, metrics.NewMetricHook(
			metrics.WithName(name),
			metrics.WithAddr(strings.Join(cfg.Addrs, ","))),
		)
	}
	if !cfg.DisableLogging {
		logOpt := make([]logging.Option, 0)
		logOpt = append(logOpt, logging.WithRequest(cfg.EnableLoggingRequest))
		logOpt = append(logOpt, logging.WithResponse(cfg.EnableLoggingResponse))
		logOpt = append(logOpt, logging.WithSlowThreshold(cfg.SlowThreshold))
		cfg.Hooks = append(cfg.Hooks, logging.NewLogging(logOpt...))
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:                 cfg.Addrs,
		ClientName:            cfg.ClientName,
		DB:                    cfg.DB,
		Username:              cfg.Username,
		Password:              cfg.Password,
		SentinelUsername:      cfg.Username,
		SentinelPassword:      cfg.Password,
		MaxRetries:            cfg.MaxRetries,
		MinRetryBackoff:       cfg.MinRetryBackoff,
		MaxRetryBackoff:       cfg.MaxRetryBackoff,
		DialTimeout:           cfg.DialTimeout,
		ReadTimeout:           cfg.ReadTimeout,
		WriteTimeout:          cfg.WriteTimeout,
		ContextTimeoutEnabled: true,
		PoolFIFO:              cfg.PoolFIFO,
		PoolSize:              cfg.PoolSize,
		PoolTimeout:           cfg.PoolTimeout,
		MinIdleConns:          cfg.MinIdleConns,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxRedirects:          cfg.MaxRetries,
		ReadOnly:              cfg.ReadOnly,
		RouteByLatency:        cfg.RouteByLatency,
		RouteRandomly:         cfg.RouteRandomly,
		MasterName:            cfg.MasterName,
	})

	for _, h := range cfg.Hooks {
		client.AddHook(h)
	}
	// Enable tracing instrumentation.
	if !cfg.DisableTrace {
		if err := redisotel.InstrumentTracing(client); err != nil {
			return nil, err
		}
	}

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	logger.Infof("%s %s connected success", namespace, name)
	return client, nil
}

func (c *component) Open() bool {
	return c.open
}

func (c *component) Start(ctx context.Context) error {
	go c.stat.Run(ctx)

	logger.Infof("Component [%s] Start success", c.String())
	return nil
}

func (c *component) Watch() error {
	return nil
}

func (c *component) Stop(ctx context.Context) error {
	c.clients.Range(func(key, value interface{}) bool {
		client := value.(*redis.Client)
		_ = client.Close()
		return true
	})

	c.clients = sync.Map{}

	logger.Infof("Component [%s] stop success", c.String())
	return nil
}

func (c *component) String() string {
	return namespace
}
