package pgxtrace

import (
	"context"
	"errors"
	"fmt"
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
	queryTypeCommit               = "Commit"
	queryTypeCopyFrom             = "CopyFrom"
	queryTypeDeallocate           = "Deallocate"
	queryTypeSendBatch            = "SendBatch"

	opName = "pgx.query"
)

func Connect(ctx context.Context, connString string, opts ...Option) (*Conn, error) {
	pgxConnn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	cfg := &config{}
	resolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConnn, cfg: cfg}, nil
}

func ConnectConfig(ctx context.Context, connConfig *pgx.ConnConfig, opts ...Option) (*Conn, error) {
	pgxConnn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	cfg := &config{}
	resolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConnn, cfg: cfg}, nil
}

type Conn struct {
	*pgx.Conn

	cfg *config
}

func (conn *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return conn.BeginTx(ctx, pgx.TxOptions{})
}

func (conn *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	_, err := conn.Exec(ctx, txOptionsBeginSQL(txOptions))
	if err != nil {
		return nil, err
	}

	return &Tx{conn: conn}, nil
}

func (conn *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return conn.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (conn *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) (err error) {
	var tx pgx.Tx
	tx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if !(rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed)) {
			err = rollbackErr
		}
	}()

	fErr := f(tx)
	if fErr != nil {
		_ = tx.Rollback(ctx)
		return fErr
	}

	return tx.Commit(ctx)
}

func (conn *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	commandTag, err := conn.Conn.Exec(ctx, sql, arguments...)

	var qtype queryType = queryTypeExec
	if strings.HasPrefix(sql, "begin") {
		qtype = queryTypeBegin
	} else if sql == "commit" {
		qtype = queryTypeCommit
	}
	tryTrace(conn.cfg, ctx, qtype, sql, start, err)

	return commandTag, err
}

func (conn *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	start := time.Now()
	n, err := conn.Conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
	tryTrace(conn.cfg, ctx, queryTypeCopyFrom, fmt.Sprintf("COPY %s FROM stdin", tableName.Sanitize()), start, err)
	return n, err
}

func tryTrace(cfg *config, ctx context.Context, qtype queryType, query string, startTime time.Time, err error) {
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
