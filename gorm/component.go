package gorm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"database/sql"

	"github.com/go-volo/logger"
	"github.com/nextmicro/next-component/gorm/plugin/logging"
	"github.com/nextmicro/next-component/gorm/plugin/metrics"
	"github.com/nextmicro/next/config"
	"github.com/nextmicro/next/runtime/loader"
	"github.com/uptrace/opentelemetry-go-extra/otelgorm"
	"gorm.io/driver/mysql" //golint
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

var (
	Gorm *component
	_    loader.Loader = &component{}
)

type component struct {
	ctx      context.Context
	cancelFn func()
	init     bool
	options  []Option
	opts     map[string]*Options
	clients  sync.Map
}

// New creates mysql a new component
func New(options ...Option) loader.Loader {
	ctx, cancel := context.WithCancel(context.Background())
	Gorm = &component{
		ctx:      ctx,
		cancelFn: cancel,
		options:  options,
		opts:     make(map[string]*Options),
	}
	return Gorm
}

// Init init mysql
func (c *component) Init(opts ...loader.Option) error {
	err := config.Value(namespace).Scan(&c.opts)
	if err != nil {
		return fmt.Errorf("gorm: %s", err)
	}

	op := loader.Options{}
	for _, opt := range opts {
		opt(&op)
	}

	if op.Context != nil {
		cfg, ok := op.Context.Value(gormConfig{}).(Options)
		if !ok {
			return errors.New("gorm: config not found")
		}

		for _, option := range cfg.Options() {
			for _, opt := range c.opts {
				option.apply(opt)
			}
		}
	}

	// use options
	for _, option := range c.options {
		for _, opt := range c.opts {
			option.apply(opt)
		}
	}

	if len(c.opts) == 0 {
		return nil
	}

	for name, opt := range c.opts {
		db, err := c.connect(name, opt)
		if err != nil {
			return err
		}

		c.clients.Store(name, db)
	}

	c.init = true
	logger.Infof("Component [%s] Init success", c.String())

	return nil
}

func (c *component) Instance(name ...string) *gorm.DB {
	group := defaultName
	if len(name) > 0 && name[0] != "" {
		group = name[0]
	}

	value, ok := c.clients.Load(group)
	if !ok {
		panic("not found instance" + group)
	}

	return value.(*gorm.DB)
}

func (c *component) connect(name string, cfg *Options) (*gorm.DB, error) {
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 16
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 256
	}
	if cfg.ConnMaxLifetime == 0 {
		cfg.ConnMaxLifetime = 300 * time.Second
	}
	if cfg.SlowLogThreshold == 0 {
		cfg.SlowLogThreshold = 500 * time.Millisecond
	}

	dsn := c.buildDns(cfg.Master)
	logOpts := make([]logging.Option, 0)
	if cfg.DisableLogging {
		logOpts = append(logOpts, logging.WithLevel(glogger.Silent))
	}
	if cfg.SlowLogThreshold != 0 {
		logOpts = append(logOpts, logging.WithSlowThreshold(cfg.SlowLogThreshold))
	}
	client, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:      logging.NewLogging(logOpts...),
		QueryFields: true,
	})
	if err != nil {
		return nil, err
	}

	// tracing
	if !cfg.DisableTrace {
		err = client.Use(otelgorm.NewPlugin(otelgorm.WithAttributes()))
		if err != nil {
			return nil, err
		}
	}

	// metrics
	if !cfg.DisableMetric {
		err = client.Use(metrics.New(c.ctx,
			metrics.WithName(cfg.Master.Database),
			metrics.WithAddr(cfg.Master.Address),
		))
		if err != nil {
			return nil, err
		}
	}

	// slaves databases
	if slaves := c.buildSlaves(cfg.Slaves); slaves != nil {
		err = client.Use(dbresolver.Register(dbresolver.Config{
			Replicas: slaves,
			Policy:   dbresolver.RandomPolicy{}, // 随机选择
		}))
		if err != nil {
			return nil, err
		}
	}

	var DB *sql.DB
	DB, err = client.DB()
	if err != nil {
		return nil, err
	}

	DB.SetMaxIdleConns(cfg.MaxIdleConns)
	DB.SetMaxOpenConns(cfg.MaxOpenConns)
	if cfg.ConnMaxLifetime != 0 {
		DB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	if err = DB.Ping(); err != nil {
		return nil, err
	}

	logger.Infof("%s %s connected success", namespace, name)
	return client, nil
}

func (c *component) buildSlaves(dns []DSN) []gorm.Dialector {
	ret := make([]gorm.Dialector, 0, len(dns))
	for _, item := range dns {
		dsn := c.buildDns(item)
		ret = append(ret, mysql.Open(dsn))
	}
	return ret
}

func (c *component) buildDns(dns DSN) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=True&loc=%s",
		dns.Username,
		dns.Password,
		dns.Address,
		dns.Database,
		dns.Charset,
		dns.Location,
	)
}

func (c *component) Start(ctx context.Context) error {
	if !c.init {
		return nil
	}

	logger.Infof("Component [%s] Start success", c.String())
	return nil
}

func (c *component) Watch() error {
	return nil
}

func (c *component) Stop(ctx context.Context) error {
	if !c.init {
		return nil
	}
	if c.cancelFn != nil {
		c.cancelFn()
	}

	c.clients.Range(func(key, value interface{}) bool {

		db := value.(*gorm.DB)
		s, err := db.DB()
		if err != nil {
			return true
		}

		err = s.Close()
		if err != nil {
			return true
		}

		return true
	})

	c.clients = sync.Map{}

	logger.Infof("Component [%s] stop success", c.String())
	return nil
}

func (c *component) String() string {
	return "gorm"
}
