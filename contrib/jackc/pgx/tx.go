package pgxtrace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// Tx is a (mostly?) complete implementation of the pgx.Tx interface
//
// TODO: remove this if/when *pgxtrace.Conn can be set on pgx.Tx
type Tx struct {
	pgx.Tx

	conn         *Conn
	err          error
	cfg          *tracing.Config
	savepointNum int64
	closed       bool
}

func (tx *Tx) Begin(ctx context.Context) (pgx.Tx, error) {
	if tx.closed {
		return nil, pgx.ErrTxClosed
	}

	tx.savepointNum++
	_, err := tx.conn.Exec(ctx, "savepoint sp_"+strconv.FormatInt(tx.savepointNum, 10))
	if err != nil {
		return nil, err
	}

	return &savepoint{tx: tx, savepointNum: tx.savepointNum}, nil
}

func (tx *Tx) BeginFunc(ctx context.Context, f func(pgx.Tx) error) (err error) {
	if tx.closed {
		return pgx.ErrTxClosed
	}

	var sp pgx.Tx
	sp, err = tx.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := sp.Rollback(ctx)
		if !(rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed)) {
			err = rollbackErr
		}
	}()

	fErr := f(sp)
	if fErr != nil {
		_ = sp.Rollback(ctx)
		return fErr
	}

	return sp.Commit(ctx)
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.closed {
		return pgx.ErrTxClosed
	}

	commandTag, err := tx.conn.Exec(ctx, "commit")
	tx.closed = true
	if err != nil {
		if tx.conn.PgConn().TxStatus() != 'I' {
			_ = tx.conn.Close(ctx)
		}
		return err
	}
	if string(commandTag) == "ROLLBACK" {
		return pgx.ErrTxCommitRollback
	}

	return nil
}

func (tx *Tx) Rollback(ctx context.Context) error {
	if tx.closed {
		return pgx.ErrTxClosed
	}

	_, err := tx.conn.Exec(ctx, "rollback")
	tx.closed = true
	if err != nil {
		tx.conn.die(fmt.Errorf("rollback failed: %w", err))
		return err
	}

	return nil
}

func (tx *Tx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if tx.closed {
		return 0, pgx.ErrTxClosed
	}

	return tx.conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (tx *Tx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if tx.closed {
		return &closedBatchResults{err: pgx.ErrTxClosed}
	}

	return tx.conn.SendBatch(ctx, b)
}

func (tx *Tx) LargeObjects() pgx.LargeObjects {
	// TODO: implement tx.LargeObjects if/when the tx struct member
	// is accessible
	log.Warn("pgxtrace.Tx.LargeObjects cannot be traced. The returned LargeObjects struct is not usable.")
	return pgx.LargeObjects{}
}

func (tx *Tx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	if tx.closed {
		return nil, pgx.ErrTxClosed
	}

	return tx.conn.Prepare(ctx, name, sql)
}

func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return tx.conn.Exec(ctx, sql, arguments...)
}

func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	start := time.Now()

	if tx.closed {
		err := pgx.ErrTxClosed

		tracing.TraceQuery(ctx, tracing.TraceQueryParams{
			ServiceName:   tx.cfg.ServiceName,
			AnalyticsRate: tx.cfg.AnalyticsRate,
			Meta:          tx.cfg.Meta,
			QueryType:     tracing.QueryTypeQuery,
			Query:         sql,
			StartTime:     start,
			Err:           err,
		})

		return &closedErrRows{err: err}, err
	}

	return tx.conn.Query(ctx, sql, args...)
}

func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	rows, _ := tx.Query(ctx, sql, args...)
	return (pgx.Row)(rows.(pgx.Row))
}

func (tx *Tx) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	if tx.closed {
		return nil, pgx.ErrTxClosed
	}

	return tx.conn.QueryFunc(ctx, sql, args, scans, f)
}

func (tx *Tx) Conn() *pgx.Conn {
	return tx.conn.Conn
}

// txOptionsBeginSQL is a copy of the private
// pgx.TtxOptions.beginSQL func
func txOptionsBeginSQL(txOptions pgx.TxOptions) string {
	buf := &bytes.Buffer{}
	buf.WriteString("begin")
	if txOptions.IsoLevel != "" {
		fmt.Fprintf(buf, " isolation level %s", txOptions.IsoLevel)
	}
	if txOptions.AccessMode != "" {
		fmt.Fprintf(buf, " %s", txOptions.AccessMode)
	}
	if txOptions.DeferrableMode != "" {
		fmt.Fprintf(buf, " %s", txOptions.DeferrableMode)
	}

	return buf.String()
}
