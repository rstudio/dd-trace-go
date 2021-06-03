package tracing

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	cfg := &Config{}
	assert.Zero(t, cfg.ServiceName)
	assert.Zero(t, cfg.AnalyticsRate)
	assert.Zero(t, cfg.Meta)

	opts := []Option{
		WithServiceName("testing-time"),
		WithAnalytics(false),
		WithSpanTags(map[string]string{
			"altered": "beast",
			"alter":   "me",
		}),
	}

	ResolveOptions(cfg, opts...)

	assert.Equal(t, "testing-time", cfg.ServiceName)
	assert.True(t, math.IsNaN(cfg.AnalyticsRate))
	assert.Contains(t, cfg.Meta, "altered")
	assert.Contains(t, cfg.Meta, "alter")
}
