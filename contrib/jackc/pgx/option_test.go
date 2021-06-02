package pgxtrace

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	cfg := &config{}
	assert.Zero(t, cfg.serviceName)
	assert.Zero(t, cfg.analyticsRate)
	assert.Zero(t, cfg.meta)

	opts := []Option{
		WithServiceName("testing-time"),
		WithAnalytics(false),
		WithSpanTags(map[string]string{
			"altered": "beast",
			"alter":   "me",
		}),
	}

	resolveOptions(cfg, opts...)

	assert.Equal(t, "testing-time", cfg.serviceName)
	assert.True(t, math.IsNaN(cfg.analyticsRate))
	assert.Contains(t, cfg.meta, "altered")
	assert.Contains(t, cfg.meta, "alter")
}
