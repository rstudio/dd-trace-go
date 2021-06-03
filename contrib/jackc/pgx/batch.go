package pgxtrace

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

type closedBatchResults struct {
	pgx.BatchResults
	err error
}

type BatchResults struct {
	start        time.Time
	finish       time.Time
	ctx          context.Context
	cfg          *tracing.Config
	batchLen     int
	batchResults pgx.BatchResults
}

func (br *BatchResults) Exec() (pgconn.CommandTag, error) {
	return br.batchResults.Exec()
}

func (br *BatchResults) Query() (pgx.Rows, error) {
	return br.batchResults.Query()
}

func (br *BatchResults) QueryRow() pgx.Row {
	return br.batchResults.QueryRow()
}

func (br *BatchResults) Close() error {
	err := br.batchResults.Close()

	tracing.TraceQuery(br.ctx, tracing.TraceQueryParams{
		ServiceName:   br.cfg.ServiceName,
		AnalyticsRate: br.cfg.AnalyticsRate,
		Meta:          br.cfg.Meta,
		QueryType:     tracing.QueryTypeSendBatch,
		Query:         fmt.Sprintf("send batch (len = %v)", br.batchLen),
		StartTime:     br.start,
		FinishTime:    br.finish,
		Err:           err,
	})

	return err
}
