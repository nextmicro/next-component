package metrics

import (
	"github.com/go-kratos/kratos/v2/metrics"
)

const component = "mongodb"

// Option is metrics option.
type Option func(options *options)

type options struct {
	disabled bool
	name     string
	addr     string
	requests metrics.Counter
	seconds  metrics.Observer
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
