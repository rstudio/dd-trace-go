package pgxpooltrace

import (
	"context"
	"strings"
	"time"

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
	start := time.Now()
	commandTag, err := tx.Tx.Exec(ctx, sql, arguments...)

	var qtype tracing.QueryType = tracing.QueryTypeExec
	if strings.HasPrefix(sql, "begin") {
		qtype = tracing.QueryTypeBegin
	} else if sql == "commit" {
		qtype = tracing.QueryTypeCommit
	} else if sql == ";" {
		qtype = tracing.QueryTypePing
	}

	tracing.TraceQuery(ctx, tracing.TraceQueryParams{
		ServiceName:   tx.cfg.ServiceName,
		AnalyticsRate: tx.cfg.AnalyticsRate,
		Meta:          tx.cfg.Meta,
		QueryType:     qtype,
		Query:         sql,
		StartTime:     start,
		Err:           err,
	})

	return commandTag, err
}
