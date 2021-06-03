package pgxpooltrace

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

type Pool struct {
	*pgxpool.Pool

	cfg *tracing.Config
}

func Connect(ctx context.Context, connString string, opts ...tracing.Option) (*Pool, error) {
	pgxPool, err := pgxpool.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	cfg := &tracing.Config{}
	tracing.ResolveOptions(cfg, opts...)

	return &Pool{Pool: pgxPool, cfg: cfg}, nil
}

func ConnectConfig(ctx context.Context, config *pgxpool.Config, opts ...tracing.Option) (*Pool, error) {
	pgxPool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	cfg := &tracing.Config{}
	tracing.ResolveOptions(cfg, opts...)

	return &Pool{Pool: pgxPool, cfg: cfg}, nil
}

func (pool *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	return pool.BeginTx(ctx, pgx.TxOptions{})
}

func (pool *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, txOptions)
	if err != nil {
		conn.Release()
		return nil, err
	}

	return &Tx{Tx: tx, conn: conn, cfg: pool.cfg}, err
}

func (pool *Pool) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return pool.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (pool *Pool) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}

	defer conn.Release()

	return conn.BeginTxFunc(ctx, txOptions, f)
}

func (pool *Pool) Acquire(ctx context.Context) (*Conn, error) {
	pgxPoolConn, err := pool.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return &Conn{Conn: pgxPoolConn, cfg: pool.cfg}, nil
}
