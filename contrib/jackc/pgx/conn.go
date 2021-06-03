package pgxtrace

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

func Connect(ctx context.Context, connString string, opts ...tracing.Option) (*Conn, error) {
	pgxConn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	cfg := &tracing.Config{}
	tracing.ResolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConn, cfg: cfg}, nil
}

func ConnectConfig(ctx context.Context, connConfig *pgx.ConnConfig, opts ...tracing.Option) (*Conn, error) {
	pgxConn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	cfg := &tracing.Config{}
	tracing.ResolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConn, cfg: cfg}, nil
}

type Conn struct {
	*pgx.Conn

	cfg *tracing.Config
}

func (conn *Conn) die(err error) {
	if conn.IsClosed() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn.PgConn().Close(ctx)
}

func (conn *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return conn.BeginTx(ctx, pgx.TxOptions{})
}

func (conn *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	_, err := conn.Exec(ctx, txOptionsBeginSQL(txOptions))
	if err != nil {
		return nil, err
	}

	return &Tx{conn: conn, cfg: conn.cfg}, nil
}

func (conn *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return conn.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (conn *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) (err error) {
	start := time.Now()

	return conn.Conn.BeginTxFunc(ctx, txOptions, func(tx pgx.Tx) error {
		tracing.TraceQuery(ctx, tracing.TraceQueryParams{
			ServiceName:   conn.cfg.ServiceName,
			AnalyticsRate: conn.cfg.AnalyticsRate,
			Meta:          conn.cfg.Meta,
			QueryType:     tracing.QueryTypeBegin,
			StartTime:     start,
		})

		err := f(&Tx{Tx: tx, conn: conn, cfg: conn.cfg})

		var qtype tracing.QueryType = tracing.QueryTypeCommit
		if err != nil {
			qtype = tracing.QueryTypeRollback
		}

		defer tracing.TraceQuery(ctx, tracing.TraceQueryParams{
			ServiceName:   conn.cfg.ServiceName,
			AnalyticsRate: conn.cfg.AnalyticsRate,
			Meta:          conn.cfg.Meta,
			QueryType:     qtype,
			StartTime:     time.Now(),
			Err:           err,
		})

		return err
	})
}

func (conn *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	start := time.Now()
	n, err := conn.Conn.CopyFrom(ctx, tableName, columnNames, rowSrc)

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   conn.cfg.ServiceName,
		AnalyticsRate: conn.cfg.AnalyticsRate,
		Meta:          conn.cfg.Meta,
		QueryType:     tracing.QueryTypeCopyFrom,
		Query:         fmt.Sprintf("COPY %s FROM stdin", tableName.Sanitize()),
		StartTime:     start,
		Err:           err,
	})

	return n, err
}

func (conn *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	commandTag, err := conn.Conn.Exec(ctx, sql, arguments...)

	var qtype tracing.QueryType = tracing.QueryTypeExec
	if strings.HasPrefix(sql, "begin") {
		qtype = tracing.QueryTypeBegin
	} else if sql == "commit" {
		qtype = tracing.QueryTypeCommit
	} else if sql == ";" {
		qtype = tracing.QueryTypePing
	}

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   conn.cfg.ServiceName,
		AnalyticsRate: conn.cfg.AnalyticsRate,
		Meta:          conn.cfg.Meta,
		QueryType:     qtype,
		Query:         sql,
		StartTime:     start,
		Err:           err,
	})

	return commandTag, err
}

func (conn *Conn) Ping(ctx context.Context) error {
	_, err := conn.Exec(ctx, ";")
	return err
}

func (conn *Conn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	start := time.Now()
	rows, err := conn.Conn.Query(ctx, sql, args...)

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   conn.cfg.ServiceName,
		AnalyticsRate: conn.cfg.AnalyticsRate,
		Meta:          conn.cfg.Meta,
		QueryType:     tracing.QueryTypeQuery,
		Query:         sql,
		StartTime:     start,
		Err:           err,
	})

	return rows, err
}

func (conn *Conn) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	start := time.Now()
	ct, err := conn.Conn.QueryFunc(ctx, sql, args, scans, f)

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   conn.cfg.ServiceName,
		AnalyticsRate: conn.cfg.AnalyticsRate,
		Meta:          conn.cfg.Meta,
		QueryType:     tracing.QueryTypeQuery,
		Query:         sql,
		StartTime:     start,
		Err:           err,
	})

	return ct, err
}

func (conn *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	start := time.Now()
	row := conn.Conn.QueryRow(ctx, sql, args...)
	finish := time.Now()

	return &Row{
		start:  start,
		finish: finish,
		row:    row,
		ctx:    ctx,
		sql:    sql,
		cfg:    conn.cfg,
	}
}

func (conn *Conn) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	start := time.Now()
	br := conn.Conn.SendBatch(ctx, b)
	finish := time.Now()

	return &BatchResults{
		start:        start,
		finish:       finish,
		ctx:          ctx,
		cfg:          conn.cfg,
		batchLen:     b.Len(),
		batchResults: br,
	}
}
