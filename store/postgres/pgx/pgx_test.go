package pgx_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	store "github.com/x4b1/messenger/store/postgres/pgx"
	"github.com/x4b1/messenger/store/postgres/test"
)

var (
	dbURL    string
	connPool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	conn, close, err := test.Setup(ctx)
	if err != nil {
		log.Fatal(err)
	}

	dbURL = conn.Config().ConnString()

	if connPool, err = pgxpool.New(ctx, dbURL); err != nil {
		fmt.Println(err.Error())
	}

	code := m.Run()

	close()

	os.Exit(code)
}

type testInstance struct {
	pgx.Tx
}

func (ti testInstance) Ping(ctx context.Context) error {
	return ti.Tx.Conn().Ping(ctx)
}

func NewTestStore(t *testing.T) (*store.Store, pgx.Tx) {
	t.Helper()

	tx, err := connPool.Begin(context.TODO())
	require.NoError(t, err)

	s, err := store.WithInstance(context.Background(), testInstance{tx})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := tx.Rollback(context.TODO()); err != nil {
			t.Error(err)
		}
	})

	return s, tx
}
