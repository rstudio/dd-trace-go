package pgxtrace

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// Tx is a complete implementation of the pgx.Tx interface
//
// TODO: remove this if/when *pgxtrace.Conn can be set on pgx.Tx
type Tx struct {
	conn   *Conn
	closed bool
}

func (tx *Tx) Begin(ctx context.Context) (pgx.Tx, error) {
	// TODO: implement tx.Begin
	return tx, nil
}

func (tx *Tx) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	// TODO: implement tx.BeginFunc
	return nil
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
	// TODO: implement tx.Rollback
	return nil
}

func (tx *Tx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if tx.closed {
		return 0, pgx.ErrTxClosed
	}

	return tx.conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (tx *Tx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	// TODO: implement tx.SendBatch
	return nil
}

func (tx *Tx) LargeObjects() pgx.LargeObjects {
	// TODO: implement tx.LargeObjects if/when the tx struct member
	// is accessible
	log.Println("WARNING: pgxtrace.Tx.LargeObjects cannot be traced; the returned LargeObjects struct will not work")
	return pgx.LargeObjects{}
}

func (tx *Tx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	// TODO: implement tx.Prepare
	return nil, nil
}

func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return tx.conn.Exec(ctx, sql, arguments...)
}

func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	// TODO: implement tx.Query
	return nil, nil
}

func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	// TODO: implement tx.QueryRow
	return nil
}

func (tx *Tx) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	// TODO: implement tx.QueryFunc
	return nil, nil
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
