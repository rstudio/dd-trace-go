package pgxtrace

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
)

var (
	testDB = func() string {
		if v, ok := os.LookupEnv("PGX_TEST_DATABASE"); ok {
			return v
		}

		return "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
	}()
)

func TestConnect(t *testing.T) {
	conn, err := Connect(context.Background(), testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
}

func TestConnectConfig(t *testing.T) {
	connConfig, err := pgx.ParseConfig(testDB)
	assert.Nil(t, err)
	assert.NotNil(t, connConfig)

	conn, err := ConnectConfig(context.Background(), connConfig)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
}

func TestConnBegin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	tx, err := conn.Begin(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, tx)

	commandTag, err := tx.Exec(ctx, "SELECT NOW(), $1::text AS ok", "ok")
	assert.Nil(t, err)
	assert.NotNil(t, commandTag)

	assert.Len(t, tr.FinishedSpans(), 2)

	span0 := tr.FinishedSpans()[0]
	assert.Equal(t, "pgx.query", span0.OperationName())
	assert.Equal(t, span0.Tags()["sql.query_type"], queryTypeBegin)

	span1 := tr.FinishedSpans()[1]
	assert.Equal(t, "pgx.query", span1.OperationName())
	assert.Equal(t, span1.Tags()["sql.query_type"], queryTypeExec)
}

func TestConnBeginFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	err = conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT NOW()"); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, "SELECT $1::text AS ok", "ok"); err != nil {
			return err
		}

		return nil
	})
	assert.Nil(t, err)

	assert.Len(t, tr.FinishedSpans(), 4)

	expectedQueryTypes := []string{
		string(queryTypeBegin),
		string(queryTypeExec),
		string(queryTypeExec),
		string(queryTypeCommit),
	}
	for i, span := range tr.FinishedSpans() {
		assert.Equal(t, expectedQueryTypes[i], span.Tags()["sql.query_type"])
	}
}

func TestConnExec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	commandTag, err := conn.Exec(ctx, "SELECT NOW(), $1::text AS ok", "ok")
	assert.Nil(t, err)
	assert.NotNil(t, commandTag)

	assert.Len(t, tr.FinishedSpans(), 1)
	span0 := tr.FinishedSpans()[0]
	assert.Equal(t, "pgx.query", span0.OperationName())
	assert.Equal(t, span0.Tags()["sql.query_type"], queryTypeExec)
}

func TestConnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	assert.False(t, conn.IsClosed())
	err = conn.Close(ctx)
	assert.Nil(t, err)

	assert.True(t, conn.IsClosed())
}

func TestConnCopyFrom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS altered_futures")
	assert.Nil(t, err)

	_, err = conn.Exec(ctx, "CREATE TABLE altered_futures (location text, heinous bool)")
	assert.Nil(t, err)

	sliceRows := [][]interface{}{
		[]interface{}{"by a tree", true},
		[]interface{}{"inside me", false},
	}

	n, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{"altered_futures"},
		[]string{"location", "heinous"},
		pgx.CopyFromSlice(len(sliceRows), func(i int) ([]interface{}, error) {
			return sliceRows[i], nil
		}),
	)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), n)

	assert.Len(t, tr.FinishedSpans(), 3)
	span2 := tr.FinishedSpans()[2]
	assert.Equal(t, "pgx.query", span2.OperationName())
	assert.Equal(t, string(queryTypeCopyFrom), span2.Tags()["sql.query_type"])
}

func TestConnQueryRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	conn, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, conn)

	now := time.Time{}
	err = conn.QueryRow(ctx, "SELECT NOW() AS now").Scan(&now)
	assert.Nil(t, err)

	assert.Len(t, tr.FinishedSpans(), 1)
	span0 := tr.FinishedSpans()[0]
	assert.Equal(t, "pgx.query", span0.OperationName())
	assert.Equal(t, string(queryTypeQuery), span0.Tags()["sql.query_type"])
}
