package pgxtrace

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type closedBatchResults struct {
	pgx.BatchResults
	err error
}

type BatchResults struct {
	start        time.Time
	finish       time.Time
	ctx          context.Context
	cfg          *config
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
	traceQuery(br.cfg, br.ctx, queryTypeSendBatch, fmt.Sprintf("(batch len=%v)", br.batchLen), br.start, br.finish, err)
	return err
}
