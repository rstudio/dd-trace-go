package pgxtrace

import (
	"math"

	"gopkg.in/DataDog/dd-trace-go.v1/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/globalconfig"
)

// Option is the function type used to populate internal tracing
// configuration.
type Option func(cfg *config)

type config struct {
	serviceName   string
	analyticsRate float64
	opName        string
	spanTags      map[string]interface{}
}

func defaults(cfg *config) {
	if internal.BoolEnv("DD_TRACE_SQL_ANALYTICS_ENABLED", false) {
		cfg.analyticsRate = 1.0
	} else {
		cfg.analyticsRate = globalconfig.AnalyticsRate()
	}
	cfg.opName = "pgx.query"
	cfg.serviceName = globalconfig.ServiceName()
}

// WithOpName sets the operation name use for starting spans. The
// default value is "pgx.query".
func WithOpName(opName string) Option {
	return func(cfg *config) {
		cfg.opName = opName
	}
}

// WithServiceName sets the service name used in the start span
// options. The default value is set from globalconfig.ServiceName.
func WithServiceName(serviceName string) Option {
	return func(cfg *config) {
		cfg.serviceName = serviceName
	}
}

// withSpanTag adds arbitrary tags to ever span.
func WithSpanTags(spanTags map[string]interface{}) Option {
	return func(cfg *config) {
		cfg.spanTags = spanTags
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
