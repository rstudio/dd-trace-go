package pgxpooltrace

import (
	"context"

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
