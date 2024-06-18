package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/internal/testhelpers"
	"github.com/x4b1/messenger/store/postgres"
	store "github.com/x4b1/messenger/store/postgres/pgx"
)

const tableName = "pgx_messages"

func TestStore_Open(t *testing.T) {
	pgConn, err := testhelpers.CreatePostgresContainer(context.TODO())
	require.NoError(t, err)

	t.Run("fails connecting", func(t *testing.T) {
		s, err := store.Open(context.TODO(), "!!!!1234", postgres.WithTableName(tableName))
		require.Error(t, err)
		require.Nil(t, s)
	})

	t.Run("success", func(t *testing.T) {
		s, err := store.Open(context.TODO(), pgConn.ConnectionString, postgres.WithTableName(tableName))
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}

func TestStore_WithInstance(t *testing.T) {
	pgConn, err := testhelpers.CreatePostgresContainer(context.TODO())
	require.NoError(t, err)

	connPool, err := pgxpool.New(context.TODO(), pgConn.ConnectionString)
	require.NoError(t, err)

	s, err := store.WithInstance(context.TODO(), connPool, postgres.WithTableName(tableName))
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestStore_Store(t *testing.T) {
	ctx := context.TODO()

	pgConn, err := testhelpers.CreatePostgresContainer(ctx)
	require.NoError(t, err)

	connPool, err := pgxpool.New(ctx, pgConn.ConnectionString)
	require.NoError(t, err)

	s, err := store.WithInstance(ctx, connPool, postgres.WithTableName(tableName))
	require.NoError(t, err)

	tx, err := connPool.Begin(ctx)
	require.NoError(t, err)

	msg, err := messenger.NewMessage([]byte("message"))
	require.NoError(t, err)

	require.NoError(t, s.Store(context.TODO(), tx, msg))

	require.NoError(t, tx.Commit(ctx))

	msgs, err := s.Messages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	require.Equal(t, msg.ID(), msgs[0].ID())
}
