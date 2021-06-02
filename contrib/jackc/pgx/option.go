package pgxtrace

import (
	"math"
)

type config struct {
	serviceName   string
	analyticsRate float64
	meta          map[string]string
}

func resolveOptions(cfg *config, opts ...Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}

type Option func(*config)

// WithServiceName sets the given service name when registering a driver,
// or opening a database connection.
func WithServiceName(name string) Option {
	return func(cfg *config) {
		cfg.serviceName = name
	}
}

// WithAnalytics enables Trace Analytics for all started spans.
func WithAnalytics(on bool) Option {
	return func(cfg *config) {
		if on {
			cfg.analyticsRate = 1.0
		} else {
			cfg.analyticsRate = math.NaN()
		}
	}
}

// WithAnalyticsRate sets the sampling rate for Trace Analytics events
// correlated to started spans.
func WithAnalyticsRate(rate float64) Option {
	return func(cfg *config) {
		if rate >= 0.0 && rate <= 1.0 {
			cfg.analyticsRate = rate
		} else {
			cfg.analyticsRate = math.NaN()
		}
	}
}

// WithSpanTags sets arbitrary key-value pairs to be included as tags on every span.
func WithSpanTags(tags map[string]string) Option {
	return func(cfg *config) {
		cfg.meta = tags
	}
}
