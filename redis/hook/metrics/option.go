package metrics

import (
	"github.com/go-kratos/kratos/v2/metrics"
)

// Option is metrics option.
type Option func(options *options)

type options struct {
	// disabled metrics.
	disabled bool
	// redis name.
	name string
	// redis address.
	addr string
	// counter: db_client_requests_total{kind,addr,method, status}
	requests metrics.Counter
	// histogram: db_client_requests_duration_ms_bucket{kind,addr,method}
	seconds metrics.Observer
}

// WithDisabled set disabled metrics.
func WithDisabled(disabled bool) Option {
	return func(o *options) {
		o.disabled = disabled
	}
}

// WithAddr with addr label.
func WithAddr(addr string) Option {
	return func(o *options) {
		o.addr = addr
	}
}

// WithName with name label.
func WithName(name string) Option {
	return func(o *options) {
		o.name = name
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
