package logging

import (
	"time"
)

const component = "mongo"

// Option is  option.
type Option func(options *options)

type options struct {
	disabled      bool
	request       bool
	response      bool
	SlowThreshold time.Duration
}

func WithDisabled(v bool) Option {
	return func(o *options) {
		o.disabled = v
	}
}

func WithRequest(v bool) Option {
	return func(o *options) {
		o.request = v
	}
}

func WithResponse(v bool) Option {
	return func(o *options) {
		o.response = v
	}
}

func WithSlowThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.SlowThreshold = threshold
	}
}
