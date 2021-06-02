package pgxtrace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type queryType string

const (
	queryTypeQuery      queryType = "Query"
	queryTypeBegin                = "Begin"
	queryTypeClose                = "Close"
	queryTypeCommit               = "Commit"
	queryTypeCopyFrom             = "CopyFrom"
	queryTypeDeallocate           = "Deallocate"
	queryTypeExec                 = "Exec"
	queryTypePing                 = "Ping"
	queryTypePrepare              = "Prepare"
	queryTypeSendBatch            = "SendBatch"

	opName = "pgx.query"
)

func Connect(ctx context.Context, connString string, opts ...Option) (*Conn, error) {
	pgxConnn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	cfg := &config{}
	resolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConnn, cfg: cfg}, nil
}

func ConnectConfig(ctx context.Context, connConfig *pgx.ConnConfig, opts ...Option) (*Conn, error) {
	pgxConnn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	cfg := &config{}
	resolveOptions(cfg, opts...)

	return &Conn{Conn: pgxConnn, cfg: cfg}, nil
}

type Conn struct {
	*pgx.Conn

	cfg *config
}

// TODO: all methods on *pgx.Conn, where 'x' means it has been
// implemented and '-' means no override is needed:
// - [x] Begin
// - [x] BeginTx
// - [x] BeginFunc
// - [x] BeginTxFunc
// - [-] Close
// - [-] Config
// - [-] ConnInfo
// - [x] CopyFrom
// - [-] Deallocate
// - [x] Exec
// - [-] IsClosed
// - [-] PgConn
// - [x] Ping
// - [-] Prepare
// - [x] Query
// - [x] QueryFunc
// - [ ] QueryRow
// - [ ] SendBatch
// - [-] StatementCache
// - [-] WaitForNotification

func (conn *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
	return conn.BeginTx(ctx, pgx.TxOptions{})
}

func (conn *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	_, err := conn.Exec(ctx, txOptionsBeginSQL(txOptions))
	if err != nil {
		return nil, err
	}

	return &Tx{conn: conn}, nil
}

func (conn *Conn) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return conn.BeginTxFunc(ctx, pgx.TxOptions{}, f)
}

func (conn *Conn) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) (err error) {
	var tx pgx.Tx
	tx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if !(rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed)) {
			err = rollbackErr
		}
	}()

	fErr := f(tx)
	if fErr != nil {
		_ = tx.Rollback(ctx)
		return fErr
	}

	return tx.Commit(ctx)
}

func (conn *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	start := time.Now()
	n, err := conn.Conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
	traceQuery(conn.cfg, ctx, queryTypeCopyFrom, fmt.Sprintf("COPY %s FROM stdin", tableName.Sanitize()), start, err)
	return n, err
}

func (conn *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	commandTag, err := conn.Conn.Exec(ctx, sql, arguments...)

	var qtype queryType = queryTypeExec
	if strings.HasPrefix(sql, "begin") {
		qtype = queryTypeBegin
	} else if sql == "commit" {
		qtype = queryTypeCommit
	} else if sql == ";" {
		qtype = queryTypePing
	}
	traceQuery(conn.cfg, ctx, qtype, sql, start, err)

	return commandTag, err
}

func (conn *Conn) Ping(ctx context.Context) error {
	_, err := conn.Exec(ctx, ";")
	return err
}

func (conn *Conn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	start := time.Now()
	rows, err := conn.Conn.Query(ctx, sql, args...)
	traceQuery(conn.cfg, ctx, queryTypeQuery, sql, start, err)
	return rows, err
}

// QueryFunc is a full copy of pgx.Conn.QueryFunc given that it is
// calling Query and struct embedding doesn't work like that.
// TODO: remove this if/when possible
func (conn *Conn) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, err
		}

		err = f(rows)
		if err != nil {
			return nil, err
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rows.CommandTag(), nil
}
