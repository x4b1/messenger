package pgx_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/store/postgres"
	store "github.com/x4b1/messenger/store/postgres/pgx"
)

func TestOpen(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	_, err := store.Open(context.Background(), dbURL)
	require.NoError(err)
}

func TestCustomTable(t *testing.T) {
	t.Parallel()

	table := "my-messages"

	_, err := store.WithInstance(context.Background(), connPool, postgres.WithTableName(table))
	require.NoError(t, err)
	row := connPool.QueryRow(
		context.Background(),
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		"public",
		table,
	)

	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, count, 1)
}

func TestCustomSchemaNotExistsReturnsError(t *testing.T) {
	t.Parallel()

	_, err := store.WithInstance(context.Background(), connPool, postgres.WithSchema("custom"))

	require.Error(t, err)
}

func TestInitializeTwiceNotReturnError(t *testing.T) {
	require := require.New(t)

	_, err := store.WithInstance(context.Background(), connPool)
	require.NoError(err)

	_, err = store.WithInstance(context.Background(), connPool)
	require.NoError(err)
}

func TestStorePublishMessages(t *testing.T) {
	t.Parallel()
	var (
		totalMsgs = 15
		batch     = 10
	)

	t.Run("with transaction", func(t *testing.T) {
		t.Parallel()

		pg, db := NewTestStore(t)

		ctx := context.Background()
		require := require.New(t)

		tx, err := db.Begin(ctx)
		require.NoError(err)

		publishMsgs := make([]messenger.Message, totalMsgs)
		for i := 0; i < totalMsgs; i++ {
			var err error
			msg, err := messenger.NewMessage([]byte(fmt.Sprintf("%d", i+1)))
			require.NoError(err)
			msg.SetMetadata("some", fmt.Sprintf("meta-%d", i+1))
			msg.SetMetadata("test", "with transaction")
			publishMsgs[i] = msg
		}

		require.NoError(pg.Store(ctx, tx, publishMsgs...))
		require.NoError(tx.Commit(ctx))

		msgs, err := pg.Messages(ctx, batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.GetMetadata(), msgs[i].GetMetadata())
			require.Equal(msg.GetPayload(), msgs[i].GetPayload())
		}

		require.NoError(pg.Published(ctx, msgs...))

		msgs, err = pg.Messages(ctx, batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.GetMetadata(), msgs[i].GetMetadata())
		}
	})

	t.Run("without transaction", func(t *testing.T) {
		t.Parallel()

		pg, _ := NewTestStore(t)

		require := require.New(t)

		publishMsgs := make([]messenger.Message, totalMsgs)
		for i := 0; i < totalMsgs; i++ {
			var err error
			msg, err := messenger.NewMessage([]byte(fmt.Sprintf("%d", i+1)))
			require.NoError(err)
			msg.SetMetadata("some", fmt.Sprintf("meta-%d", i+1))
			msg.SetMetadata("test", "without transaction")
			publishMsgs[i] = msg
		}

		require.NoError(pg.Store(context.Background(), nil, publishMsgs...))

		msgs, err := pg.Messages(context.Background(), batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.GetMetadata(), msgs[i].GetMetadata())
			require.Equal(msg.GetMetadata(), msgs[i].GetMetadata())
		}

		require.NoError(pg.Published(context.Background(), msgs...))

		msgs, err = pg.Messages(context.Background(), batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.GetMetadata(), msgs[i].GetMetadata())
			require.Equal(msg.GetPayload(), msgs[i].GetPayload())
		}
	})
}

func TestDeletePublishedByExpiration(t *testing.T) {
	t.Parallel()
	batch := 10

	t.Run("expired and not published not deletes", func(t *testing.T) {
		t.Parallel()
		pg, db := NewTestStore(t)

		eventID := uuid.Must(uuid.NewRandom())

		_, err := db.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %q (id, metadata, payload, created_at) VALUES ($1, $2, $3, $4)`, postgres.DefaultMessagesTable),
			eventID,
			"{}",
			"test",
			time.Now().AddDate(0, 0, -2),
		)
		require.NoError(t, err)

		d, err := time.ParseDuration("24h")
		require.NoError(t, err)
		require.NoError(t, pg.DeletePublishedByExpiration(context.Background(), d))

		msgs, err := pg.Messages(context.Background(), batch)
		require.NoError(t, err)

		require.Len(t, msgs, 1)
		require.Equal(t, eventID.String(), msgs[0].ID())
	})
	t.Run("not expired and published not deletes", func(t *testing.T) {
		t.Parallel()
		pg, db := NewTestStore(t)

		eventID := uuid.Must(uuid.NewRandom())

		_, err := db.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %q (id, metadata, payload, created_at) VALUES ($1, $2, $3, $4)`, postgres.DefaultMessagesTable),
			eventID,
			"{}",
			"test",
			time.Now().AddDate(0, 0, -1),
		)
		require.NoError(t, err)

		d, err := time.ParseDuration("48h")
		require.NoError(t, err)
		require.NoError(t, pg.DeletePublishedByExpiration(context.Background(), d))

		msgs, err := pg.Messages(context.Background(), batch)
		require.NoError(t, err)

		require.Len(t, msgs, 1)
		require.Equal(t, eventID.String(), msgs[0].ID())
	})

	t.Run("expired and published deletes", func(t *testing.T) {
		t.Parallel()
		pg, db := NewTestStore(t)

		eventID := uuid.Must(uuid.NewRandom())

		_, err := db.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %q (id, metadata, payload, published, created_at) VALUES ($1, $2, $3, true, $4)`, postgres.DefaultMessagesTable),
			eventID,
			"{}",
			"test",
			time.Now().AddDate(0, 0, -2),
		)
		require.NoError(t, err)

		d, err := time.ParseDuration("24h")
		require.NoError(t, err)
		require.NoError(t, pg.DeletePublishedByExpiration(context.Background(), d))

		msgs, err := pg.Messages(context.Background(), batch)
		require.NoError(t, err)

		require.Empty(t, msgs)
	})
}
