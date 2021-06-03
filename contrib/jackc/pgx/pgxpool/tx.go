package pgxpooltrace

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

type Tx struct {
	pgx.Tx

	conn *Conn
	cfg  *tracing.Config
}

func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return tx.conn.Exec(ctx, sql, arguments...)
}
