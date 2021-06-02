package pgxtrace

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type queryType string

const (
	queryTypeQuery      queryType = "Query"
	queryTypePing                 = "Ping"
	queryTypePrepare              = "Prepare"
	queryTypeExec                 = "Exec"
	queryTypeBegin                = "Begin"
	queryTypeClose                = "Close"
	queryTypeCopyFrom             = "CopyFrom"
	queryTypeDeallocate           = "Deallocate"
	queryTypeSendBatch            = "SendBatch"

	opName = "pgx.query"
)

func Connect(ctx context.Context, connString string) (*Conn, error) {
	pgxConnn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &Conn{Conn: pgxConnn}, nil
}

func ConnectConfig(ctx context.Context, connConfig *pgx.ConnConfig) (*Conn, error) {
	pgxConnn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	return &Conn{Conn: pgxConnn}, nil
}

type Conn struct {
	*pgx.Conn

	cfg *traceConfig
}

func (conn *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return conn.BeginTx(ctx, pgx.TxOptions{})
}

func (conn *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	_, err := conn.Exec(ctx, txOptionsBeginSQL(txOptions))
	if err != nil {
		return nil, err
	}

	return newTx(conn, conn.cfg), nil
}

func (conn *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	commandTag, err := conn.Conn.Exec(ctx, sql, arguments...)

	var qtype queryType = queryTypeExec
	if strings.HasPrefix(sql, "begin") {
		qtype = queryTypeBegin
	}
	tryTrace(conn.cfg, ctx, qtype, sql, start, err)

	return commandTag, err
}

type traceConfig struct {
	serviceName   string
	analyticsRate float64
	meta          map[string]string
}

func tryTrace(cfg *traceConfig, ctx context.Context, qtype queryType, query string, startTime time.Time, err error) {
	opts := []ddtrace.StartSpanOption{
		// TODO: service name from config/options
		// tracer.ServiceName(cfg.serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.StartTime(startTime),
	}

	// TODO: analytics rate from config/options
	// if !math.IsNaN(cfg.analyticsRate) {
	// opts = append(opts, tracer.Tag(ext.EventSampleRate, cfg.analyticsRate))
	// }

	span, _ := tracer.StartSpanFromContext(ctx, opName, opts...)
	resource := string(qtype)
	if query != "" {
		resource = query
	}
	span.SetTag("sql.query_type", string(qtype))
	span.SetTag(ext.ResourceName, resource)
	// TODO: meta tags from config/options
	// for k, v := range cfg.meta {
	// span.SetTag(k, v)
	// }
	// TODO: meta tags from context map
	// if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
	// for k, v := range meta {
	// span.SetTag(k, v)
	// }
	// }
	span.Finish(tracer.WithError(err))
}
