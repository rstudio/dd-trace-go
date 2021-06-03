package pgxtrace

import (
	"context"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

// Row is a complete implementation of pgx.Row
type Row struct {
	start  time.Time
	finish time.Time
	row    pgx.Row
	ctx    context.Context
	sql    string
	cfg    *tracing.Config
}

func (row *Row) Scan(dest ...interface{}) error {
	err := row.row.Scan(dest...)

	tracing.TraceQuery(row.ctx, tracing.TraceQueryParams{
		ServiceName:   row.cfg.ServiceName,
		AnalyticsRate: row.cfg.AnalyticsRate,
		Meta:          row.cfg.Meta,
		QueryType:     tracing.QueryTypeQuery,
		Query:         row.sql,
		StartTime:     row.start,
		FinishTime:    row.finish,
		Err:           err,
	})

	return err
}

func (row *Row) asRows() pgx.Rows {
	return (pgx.Rows)(row.row.(pgx.Rows))
}

func (row *Row) Close() {
	row.asRows().Close()
}

func (row *Row) CommandTag() pgconn.CommandTag {
	return row.asRows().CommandTag()
}

func (row *Row) Err() error {
	return row.asRows().Err()
}

func (row *Row) FieldDescriptions() []pgproto3.FieldDescription {
	return row.asRows().FieldDescriptions()
}

func (row *Row) Next() bool {
	return row.asRows().Next()
}

func (row *Row) RawValues() [][]byte {
	return row.asRows().RawValues()
}

func (row *Row) Values() ([]interface{}, error) {
	return row.asRows().Values()
}
