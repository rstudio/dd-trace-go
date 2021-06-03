package pgxtrace

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgx/v4"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var (
	knownPgxMsg = map[string]struct{}{
		"BatchResult.Close": {},
		"BatchResult.Exec":  {},
		"BatchResult.Query": {},
		"CopyFrom":          {},
		"Exec":              {},
		"Query":             {},
	}
)

// LoggerWithTracing allows any pgx.Logger to be wrapped for
// producing tracing spans. The log level controls provided by pgx
// will affect whether or not tracing spans are created.
func LoggerWithTracing(logger pgx.Logger, opts ...Option) pgx.Logger {
	cfg := &config{}
	defaults(cfg)

	for _, opt := range opts {
		opt(cfg)
	}

	return &wrappedLogger{logger: logger, cfg: cfg}
}

type spanTag struct {
	Key   string
	Value interface{}
}

type wrappedLogger struct {
	logger pgx.Logger
	cfg    *config
}

func (wl *wrappedLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	wl.trace(ctx, msg, data)
	wl.logger.Log(ctx, level, msg, data)
}

func (wl *wrappedLogger) trace(ctx context.Context, msg string, data map[string]interface{}) {
	endTime := time.Now()

	if _, ok := knownPgxMsg[msg]; !ok {
		return
	}

	span, _ := tracer.StartSpanFromContext(
		ctx,
		wl.cfg.opName,
		wl.buildStartSpanOptions(endTime, data)...,
	)

	for _, st := range wl.buildSpanTags(ctx, msg, data) {
		span.SetTag(st.Key, st.Value)
	}

	span.Finish(wl.buildFinishOptions(endTime, data)...)
}

func (wl *wrappedLogger) buildStartSpanOptions(endTime time.Time, data map[string]interface{}) []ddtrace.StartSpanOption {
	startSpanOpts := []ddtrace.StartSpanOption{
		tracer.ServiceName(wl.cfg.serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
	}

	if t, ok := data["startTime"]; ok {
		if tTime, ok := t.(time.Time); ok {
			startSpanOpts = append(startSpanOpts, tracer.StartTime(tTime))
		}
	} else if d, ok := data["time"]; ok {
		if dDur, ok := d.(time.Duration); ok {
			startSpanOpts = append(startSpanOpts, tracer.StartTime(endTime.Add(-dDur)))
		}
	}

	if !math.IsNaN(wl.cfg.analyticsRate) {
		startSpanOpts = append(startSpanOpts, tracer.Tag(ext.EventSampleRate, wl.cfg.analyticsRate))
	}

	return startSpanOpts
}

func (wl *wrappedLogger) buildFinishOptions(endTime time.Time, data map[string]interface{}) []ddtrace.FinishOption {
	finishOpts := []ddtrace.FinishOption{
		tracer.FinishTime(endTime),
	}

	if e, ok := data["err"]; ok {
		if eErr, ok := e.(error); ok {
			finishOpts = append(finishOpts, tracer.WithError(eErr))
		}
	}

	return finishOpts
}

func (wl *wrappedLogger) buildSpanTags(ctx context.Context, msg string, data map[string]interface{}) []spanTag {
	spanTags := []spanTag{
		{Key: "pgx.query_type", Value: string(msg)},
	}

	resourceSpanTag := spanTag{Key: ext.ResourceName, Value: string(msg)}

	if sql, ok := data["sql"]; ok {
		resourceSpanTag.Value = sql
	}

	spanTags = append(spanTags, resourceSpanTag)

	for dataKey, spanKey := range map[string]string{
		"commandTag":  "pgx.command_tag",
		"rowCount":    "pgx.row_count",
		"tableName":   "pgx.table_name",
		"columnNames": "pgx.column_names",
	} {
		if v, ok := data[dataKey]; ok {
			spanTags = append(spanTags, spanTag{Key: spanKey, Value: v})
		}
	}

	for k, v := range wl.cfg.spanTags {
		spanTags = append(spanTags, spanTag{Key: k, Value: v})
	}

	if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
		for k, v := range meta {
			spanTags = append(spanTags, spanTag{Key: k, Value: v})
		}
	}

	return spanTags
}
