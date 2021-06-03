package tracing

import (
	"math"
)

type Config struct {
	ServiceName   string
	AnalyticsRate float64
	Meta          map[string]string
}

func ResolveOptions(cfg *Config, opts ...Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}

type Option func(*Config)

// WithServiceName sets the given service name when registering a driver,
// or opening a database connection.
func WithServiceName(name string) Option {
	return func(cfg *Config) {
		cfg.ServiceName = name
	}
}

// WithAnalytics enables Trace Analytics for all started spans.
func WithAnalytics(on bool) Option {
	return func(cfg *Config) {
		if on {
			cfg.AnalyticsRate = 1.0
		} else {
			cfg.AnalyticsRate = math.NaN()
		}
	}
}

// WithAnalyticsRate sets the sampling rate for Trace Analytics events
// correlated to started spans.
func WithAnalyticsRate(rate float64) Option {
	return func(cfg *Config) {
		if rate >= 0.0 && rate <= 1.0 {
			cfg.AnalyticsRate = rate
		} else {
			cfg.AnalyticsRate = math.NaN()
		}
	}
}

// WithSpanTags sets arbitrary key-value pairs to be included as tags on every span.
func WithSpanTags(tags map[string]string) Option {
	return func(cfg *Config) {
		cfg.Meta = tags
	}
}
