package tracing

import (
	"context"
	"math"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type contextKey int

const (
	spanTagsKey contextKey = 0 // map[string]string
	opName                 = "pgx.query"
)

// ContextWithSpanTags creates a new context containing the given set of tags. They will be added
// to any query created with the returned context.
func ContextWithSpanTags(ctx context.Context, tags map[string]string) context.Context {
	return context.WithValue(ctx, spanTagsKey, tags)
}

type TraceQueryParams struct {
	ServiceName   string
	AnalyticsRate float64
	Meta          map[string]string
	QueryType     QueryType
	Query         string
	StartTime     time.Time
	FinishTime    time.Time
	Err           error
}

func TraceQuery(ctx context.Context, params TraceQueryParams) {
	opts := []ddtrace.StartSpanOption{
		tracer.ServiceName(params.ServiceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.StartTime(params.StartTime),
	}

	if !math.IsNaN(params.AnalyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, params.AnalyticsRate))
	}

	span, _ := tracer.StartSpanFromContext(ctx, opName, opts...)
	resource := string(params.QueryType)
	if params.Query != "" {
		resource = params.Query
	}
	span.SetTag("sql.query_type", string(params.QueryType))
	span.SetTag(ext.ResourceName, resource)
	for k, v := range params.Meta {
		span.SetTag(k, v)
	}

	if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
		for k, v := range meta {
			span.SetTag(k, v)
		}
	}

	finishOpts := []tracer.FinishOption{
		tracer.WithError(params.Err),
	}
	if !params.FinishTime.IsZero() {
		finishOpts = append(finishOpts, tracer.FinishTime(params.FinishTime))
	}

	span.Finish(finishOpts...)
}
