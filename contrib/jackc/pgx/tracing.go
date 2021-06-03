package pgxtrace

import (
	"context"
	"math"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type contextKey int

const spanTagsKey contextKey = 0 // map[string]string

// ContextWithSpanTags creates a new context containing the given set of tags. They will be added
// to any query created with the returned context.
func ContextWithSpanTags(ctx context.Context, tags map[string]string) context.Context {
	return context.WithValue(ctx, spanTagsKey, tags)
}

func traceQuery(cfg *config, ctx context.Context, qtype queryType, query string, startTime, finishTime time.Time, err error) {
	opts := []ddtrace.StartSpanOption{
		tracer.ServiceName(cfg.serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.StartTime(startTime),
	}

	if !math.IsNaN(cfg.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, cfg.analyticsRate))
	}

	span, _ := tracer.StartSpanFromContext(ctx, opName, opts...)
	resource := string(qtype)
	if query != "" {
		resource = query
	}
	span.SetTag("sql.query_type", string(qtype))
	span.SetTag(ext.ResourceName, resource)
	for k, v := range cfg.meta {
		span.SetTag(k, v)
	}

	if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
		for k, v := range meta {
			span.SetTag(k, v)
		}
	}

	finishOpts := []tracer.FinishOption{
		tracer.WithError(err),
	}
	if !finishTime.IsZero() {
		finishOpts = append(finishOpts, tracer.FinishTime(finishTime))
	}

	span.Finish(finishOpts...)
}
