package metrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	prom "github.com/go-kratos/kratos/contrib/metrics/prometheus/v2"
	"github.com/go-volo/logger"
	"github.com/nextmicro/next/pkg/metrics"
	"go.opentelemetry.io/otel/codes"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

type metricPlugin struct {
	ops *options
	ctx context.Context
}

// New gorm metrics.
func New(ctx context.Context, opts ...Option) gorm.Plugin {
	op := &options{
		interval: time.Second * 30,
		stats:    prom.NewGauge(metrics.DBSystemStatsGauge),
		requests: prom.NewCounter(metrics.DBSystemMetricRequests),
		seconds:  prom.NewHistogram(metrics.DBSystemMetricMillisecond),
	}
	for _, opt := range opts {
		opt(op)
	}

	p := &metricPlugin{
		ctx: ctx,
		ops: op,
	}

	return p
}

func (p *metricPlugin) Name() string {
	return "metrics"
}

type gormHookFunc func(tx *gorm.DB)

type gormRegister interface {
	Register(name string, fn func(*gorm.DB)) error
}

func (p *metricPlugin) Initialize(db *gorm.DB) (err error) {
	cb := db.Callback()

	hooks := []struct {
		callback gormRegister
		hook     gormHookFunc
		name     string
	}{
		{cb.Create().Before("gorm:create"), p.before("gorm.Create"), "before:create"},
		{cb.Create().After("gorm:create"), p.after(), "after:create"},

		{cb.Query().Before("gorm:query"), p.before("gorm.Query"), "before:select"},
		{cb.Query().After("gorm:query"), p.after(), "after:select"},

		{cb.Delete().Before("gorm:delete"), p.before("gorm.Delete"), "before:delete"},
		{cb.Delete().After("gorm:delete"), p.after(), "after:delete"},

		{cb.Update().Before("gorm:update"), p.before("gorm.Update"), "before:update"},
		{cb.Update().After("gorm:update"), p.after(), "after:update"},

		{cb.Row().Before("gorm:row"), p.before("gorm.Row"), "before:row"},
		{cb.Row().After("gorm:row"), p.after(), "after:row"},

		{cb.Raw().Before("gorm:raw"), p.before("gorm.Raw"), "before:raw"},
		{cb.Raw().After("gorm:raw"), p.after(), "after:raw"},
	}

	var firstErr error

	for _, h := range hooks {
		if err := h.callback.Register("metric:"+h.name, h.hook); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("callback register %s failed: %w", h.name, err)
		}
	}

	// metrics stats
	go p.run(p.ctx, db)

	return firstErr
}

func (p *metricPlugin) before(spanName string) gormHookFunc {
	return func(tx *gorm.DB) {
		tx.Statement.Context = context.WithValue(tx.Statement.Context, startTime{}, time.Now())
	}
}

func (p *metricPlugin) after() gormHookFunc {
	return func(tx *gorm.DB) {
		var (
			ok    bool
			start time.Time
		)
		start, ok = tx.Statement.Context.Value(startTime{}).(time.Time)
		if !ok {
			return
		}

		var (
			err  = tx.Error
			code = codes.Ok
		)

		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			code = codes.Error
		}

		vars := tx.Statement.Vars
		// Replace query variables with '?' to mask them
		vars = make([]interface{}, len(tx.Statement.Vars))

		for i := 0; i < len(vars); i++ {
			vars[i] = "?"
		}

		query := tx.Dialector.Explain(tx.Statement.SQL.String(), vars...)
		query = p.formatQuery(query)
		cmd := tx.Statement.Table + ":" + query
		p.ops.requests.With(component, p.ops.name, p.ops.addr, cmd, code.String()).Inc()
		p.ops.seconds.With(component, p.ops.name, p.ops.addr, cmd).Observe(float64(time.Since(start).Milliseconds()))
	}
}

func (p *metricPlugin) formatQuery(query string) string {
	if p.ops.queryFormatter != nil {
		return p.ops.queryFormatter(query)
	}
	return query
}

func (p *metricPlugin) run(ctx context.Context, gdb *gorm.DB) {
	ticker := time.NewTicker(p.ops.interval)
	defer ticker.Stop()

	var (
		dbResolver *dbresolver.DBResolver
		sqlDB, err = gdb.DB()
	)
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			goto Close
		case <-ticker.C:
			p.Set(sqlDB.Stats())
			if dbResolver != nil {
				connPoolMap := map[gorm.ConnPool]bool{}
				dbResolver.Call(func(connPool gorm.ConnPool) (err error) {
					if _, ok := connPoolMap[connPool]; !ok {
						if statser, ok := connPool.(interface{ Stats() sql.DBStats }); ok {
							stats := statser.Stats()
							p.Set(stats)
						}

						connPoolMap[connPool] = true
					}
					return
				})
			}
		}
	}
Close:

	logger.Info("gorm: stats metrics stop")
}

func (p *metricPlugin) Set(stat sql.DBStats) {
	p.ops.stats.With(component, p.ops.name, p.ops.addr, MaxOpenConnections).Set(float64(stat.MaxOpenConnections))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, OpenConnections).Set(float64(stat.OpenConnections))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, InUse).Set(float64(stat.InUse))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, Idle).Set(float64(stat.Idle))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, MaxIdleClosed).Set(float64(stat.MaxIdleClosed))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, MaxIdleTimeClosed).Set(float64(stat.MaxIdleTimeClosed))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, MaxLifetimeClosed).Set(float64(stat.MaxLifetimeClosed))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, WaitCount).Set(float64(stat.WaitCount))
	p.ops.stats.With(component, p.ops.name, p.ops.addr, WaitDuration).Set(float64(stat.WaitDuration.Milliseconds()))
}
