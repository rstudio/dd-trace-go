package pgxtrace

import (
	"context"
	"strconv"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// savepoint is mostly a copy of the private dbSavepoint struct in pgx
type savepoint struct {
	tx           *Tx
	savepointNum int64
	closed       bool
}

func (sp *savepoint) Begin(ctx context.Context) (pgx.Tx, error) {
	if sp.closed {
		return nil, pgx.ErrTxClosed
	}

	return sp.tx.Begin(ctx)
}

func (sp *savepoint) BeginFunc(ctx context.Context, f func(pgx.Tx) error) (err error) {
	if sp.closed {
		return pgx.ErrTxClosed
	}

	return sp.tx.BeginFunc(ctx, f)
}

func (sp *savepoint) Commit(ctx context.Context) error {
	if sp.closed {
		return pgx.ErrTxClosed
	}

	_, err := sp.Exec(ctx, "release savepoint sp_"+strconv.FormatInt(sp.savepointNum, 10))
	sp.closed = true
	return err
}

func (sp *savepoint) Rollback(ctx context.Context) error {
	if sp.closed {
		return pgx.ErrTxClosed
	}

	_, err := sp.Exec(ctx, "rollback to savepoint sp_"+strconv.FormatInt(sp.savepointNum, 10))
	sp.closed = true
	return err
}

func (sp *savepoint) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	if sp.closed {
		return nil, pgx.ErrTxClosed
	}

	return sp.tx.Exec(ctx, sql, arguments...)
}

func (sp *savepoint) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	if sp.closed {
		return nil, pgx.ErrTxClosed
	}

	return sp.tx.Prepare(ctx, name, sql)
}

func (sp *savepoint) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	if sp.closed {
		err := pgx.ErrTxClosed
		return &closedErrRows{err: err}, err
	}

	return sp.tx.Query(ctx, sql, args...)
}

func (sp *savepoint) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	rows, _ := sp.Query(ctx, sql, args...)
	return (pgx.Row)(rows.(pgx.Row))
}

func (sp *savepoint) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	if sp.closed {
		return nil, pgx.ErrTxClosed
	}

	return sp.tx.QueryFunc(ctx, sql, args, scans, f)
}

func (sp *savepoint) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if sp.closed {
		return 0, pgx.ErrTxClosed
	}

	return sp.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (sp *savepoint) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if sp.closed {
		return &closedBatchResults{err: pgx.ErrTxClosed}
	}

	return sp.tx.SendBatch(ctx, b)
}

func (sp *savepoint) LargeObjects() pgx.LargeObjects {
	log.Warn("pgxtrace.Tx.(savepoint).LargeObjects cannot be traced. The returned LargeObjects struct is not usable.")
	return pgx.LargeObjects{}
}

func (sp *savepoint) Conn() *pgx.Conn {
	return sp.tx.Conn()
}
