package stdsql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/internal/testhelpers"
	"github.com/x4b1/messenger/store/postgres"
	store "github.com/x4b1/messenger/store/postgres/stdsql"
)

const tableName = "stdsql_messages"

func TestStore_Open(t *testing.T) {
	pgConn, err := testhelpers.CreatePostgresContainer(context.TODO())
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		s, err := store.Open(context.TODO(), pgConn.ConnectionString, postgres.WithTableName(tableName))
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}

func TestStore_WithInstance(t *testing.T) {
	pgConn, err := testhelpers.CreatePostgresContainer(context.TODO())
	require.NoError(t, err)

	db, err := sql.Open("pgx", pgConn.ConnectionString)
	require.NoError(t, err)

	s, err := store.WithInstance(context.TODO(), db, postgres.WithTableName(tableName))
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestStore_Store(t *testing.T) {
	ctx := context.TODO()

	pgConn, err := testhelpers.CreatePostgresContainer(ctx)
	require.NoError(t, err)

	db, err := sql.Open("pgx", pgConn.ConnectionString)
	require.NoError(t, err)

	s, err := store.WithInstance(ctx, db, postgres.WithTableName(tableName))
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	msg, err := messenger.NewMessage([]byte("message"))
	require.NoError(t, err)

	require.NoError(t, s.Store(context.TODO(), tx, msg))

	require.NoError(t, tx.Commit())

	msgs, err := s.Messages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	require.Equal(t, msg.ID(), msgs[0].ID())
}
