package pgxpooltrace

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

type Conn struct {
	*pgxpool.Conn

	cfg *tracing.Config
}

func (conn *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return conn.BeginTx(ctx, pgx.TxOptions{})
}

func (conn *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	start := time.Now()
	tx, err := conn.Conn.Conn().BeginTx(ctx, txOptions)

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   conn.cfg.ServiceName,
		AnalyticsRate: conn.cfg.AnalyticsRate,
		Meta:          conn.cfg.Meta,
		QueryType:     tracing.QueryTypeBegin,
		StartTime:     start,
		Err:           err,
	})

	return &Tx{Tx: tx, conn: conn, cfg: conn.cfg}, err
}

func (conn *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return conn.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (conn *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	start := time.Now()

	return conn.Conn.Conn().BeginTxFunc(ctx, txOptions, func(tx pgx.Tx) error {
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

func (conn *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	commandTag, err := conn.Conn.Conn().Exec(ctx, sql, arguments...)

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
		StartTime:     start,
		Err:           err,
	})

	return commandTag, err
}
