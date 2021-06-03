package pgxpooltrace

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
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
	pool, err := Connect(context.Background(), testDB)
	assert.Nil(t, err)
	assert.NotNil(t, pool)
}

func TestConnectConfig(t *testing.T) {
	config, err := pgxpool.ParseConfig(testDB)
	assert.Nil(t, err)
	assert.NotNil(t, config)

	pool, err := ConnectConfig(context.Background(), config)
	assert.Nil(t, err)
	assert.NotNil(t, pool)
}

func TestPoolBegin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	pool, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, pool)

	tx, err := pool.Begin(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, tx)

	commandTag, err := tx.Exec(ctx, "SELECT NOW(), $1::text AS ok", "ok")
	assert.Nil(t, err)
	assert.NotNil(t, commandTag)

	assert.Len(t, tr.FinishedSpans(), 2)

	span0 := tr.FinishedSpans()[0]
	assert.Equal(t, "pgx.query", span0.OperationName())
	assert.Equal(t, string(tracing.QueryTypeBegin), span0.Tags()["sql.query_type"])

	span1 := tr.FinishedSpans()[1]
	assert.Equal(t, "pgx.query", span1.OperationName())
	assert.Equal(t, string(tracing.QueryTypeExec), span1.Tags()["sql.query_type"])
}

func TestPoolBeginFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	pool, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, pool)

	err = pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT NOW()"); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, "SELECT $1::text AS ok", "ok"); err != nil {
			return err
		}

		return nil
	})
	assert.Nil(t, err)

	expectedQueryTypes := []string{
		string(tracing.QueryTypeBegin),
		string(tracing.QueryTypeExec),
		string(tracing.QueryTypeExec),
		string(tracing.QueryTypeCommit),
	}

	assert.Len(t, tr.FinishedSpans(), len(expectedQueryTypes))

	for i, span := range tr.FinishedSpans() {
		assert.Equal(t, expectedQueryTypes[i], span.Tags()["sql.query_type"])
	}
}

func TestPoolExec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := mocktracer.Start()
	defer tr.Stop()

	pool, err := Connect(ctx, testDB)
	assert.Nil(t, err)
	assert.NotNil(t, pool)

	commandTag, err := pool.Exec(ctx, "SELECT NOW(), $1::text AS ok", "ok")
	assert.Nil(t, err)
	assert.NotNil(t, commandTag)

	assert.Len(t, tr.FinishedSpans(), 1)
	span0 := tr.FinishedSpans()[0]
	assert.Equal(t, "pgx.query", span0.OperationName())
	assert.Equal(t, tracing.QueryTypeExec, span0.Tags()["sql.query_type"])
}

/***
 * TODO
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
	assert.Equal(t, string(tracing.QueryTypeCopyFrom), span2.Tags()["sql.query_type"])
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
	assert.Equal(t, string(tracing.QueryTypeQuery), span0.Tags()["sql.query_type"])
}
*/
