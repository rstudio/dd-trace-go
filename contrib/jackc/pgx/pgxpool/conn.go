package pgxpooltrace

import (
	"context"
	"time"

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

	return tx, err
}

func (conn *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return conn.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (conn *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	return conn.Conn.Conn().BeginTxFunc(ctx, txOptions, func(tx pgx.Tx) error {
		return f(&Tx{Tx: tx, conn: conn, cfg: conn.cfg})
	})
}
