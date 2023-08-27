package metrics

import (
	"time"

	"github.com/go-kratos/kratos/v2/metrics"
)

const (
	component = "mysql"

	MaxOpenConnections = "max_open_connections"
	OpenConnections    = "open_connections"
	InUse              = "in_use"
	Idle               = "idle"
	WaitCount          = "wait_count"
	WaitDuration       = "wait_duration_ms"
	MaxIdleClosed      = "max_idle_closed"
	MaxIdleTimeClosed  = "max_idle_time_closed"
	MaxLifetimeClosed  = "max_lifetime_closed"
)

type startTime struct{}

// Option is metrics option.
type Option func(options *options)

type options struct {
	disabled       bool
	name           string
	addr           string
	interval       time.Duration
	queryFormatter func(query string) string
	requests       metrics.Counter
	seconds        metrics.Observer
	stats          metrics.Gauge
}

// WithDisabled set disabled metrics.
func WithDisabled(disabled bool) Option {
	return func(o *options) {
		o.disabled = disabled
	}
}

// WithName with name label.
func WithName(name string) Option {
	return func(o *options) {
		o.name = name
	}
}

// WithAddr with addr label.
func WithAddr(address string) Option {
	return func(o *options) {
		o.addr = address
	}
}

// WithInterval with interval label.
func WithInterval(interval time.Duration) Option {
	return func(o *options) {
		o.interval = interval
	}
}

// WithQueryFormatter with queryFormatter label.
func WithQueryFormatter(queryFormatter func(query string) string) Option {
	return func(o *options) {
		o.queryFormatter = queryFormatter
	}
}

// WithRequests with requests counter.
func WithRequests(c metrics.Counter) Option {
	return func(o *options) {
		o.requests = c
	}
}

// WithSeconds with seconds histogram.
func WithSeconds(c metrics.Observer) Option {
	return func(o *options) {
		o.seconds = c
	}
}

// WithStats with stats gauge.
func WithStats(c metrics.Gauge) Option {
	return func(o *options) {
		o.stats = c
	}
}
