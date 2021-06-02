package pgxtrace

import (
	"context"
	"os"
	"testing"

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
