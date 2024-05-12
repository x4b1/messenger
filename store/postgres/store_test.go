package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/inspect"
	"github.com/x4b1/messenger/store"
	"github.com/x4b1/messenger/store/postgres"
	pgxstore "github.com/x4b1/messenger/store/postgres/pgx"
)

func TestOpen(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	_, err := pgxstore.Open(context.Background(), dbURL)
	require.NoError(err)
}

func TestCustomTable(t *testing.T) {
	t.Parallel()

	table := "my-messages"

	_, err := pgxstore.WithInstance(context.Background(), connPool, postgres.WithTableName(table))
	require.NoError(t, err)
	row := connPool.QueryRow(
		context.Background(),
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		"public",
		table,
	)

	var count int
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count)
}

func TestCustomSchemaNotExistsReturnsError(t *testing.T) {
	t.Parallel()

	_, err := pgxstore.WithInstance(context.Background(), connPool, postgres.WithSchema("custom"))

	require.Error(t, err)
}

func TestInitializeTwiceNotReturnError(t *testing.T) {
	require := require.New(t)

	_, err := pgxstore.WithInstance(context.Background(), connPool)
	require.NoError(err)

	_, err = pgxstore.WithInstance(context.Background(), connPool)
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
			msg, err := messenger.NewMessage([]byte(strconv.Itoa(i + 1)))
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
			require.Equal(msg.Metadata(), msgs[i].Metadata())
			require.Equal(msg.Payload(), msgs[i].Payload())
			require.NoError(pg.Published(ctx, msg))
		}

		msgs, err = pg.Messages(ctx, batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata())
		}
	})

	t.Run("without transaction", func(t *testing.T) {
		t.Parallel()

		pg, _ := NewTestStore(t)

		require := require.New(t)

		publishMsgs := make([]messenger.Message, totalMsgs)
		for i := 0; i < totalMsgs; i++ {
			var err error
			msg, err := messenger.NewMessage([]byte(strconv.Itoa(i + 1)))
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
			require.Equal(msg.Metadata(), msgs[i].Metadata())
			require.Equal(msg.Metadata(), msgs[i].Metadata())
			require.NoError(pg.Published(context.Background(), msg))
		}

		msgs, err = pg.Messages(context.Background(), batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata())
			require.Equal(msg.Payload(), msgs[i].Payload())
		}
	})
	t.Run("with transformation", func(t *testing.T) {
		t.Parallel()

		ctxKey := "some-data"
		ctx := context.WithValue(context.TODO(), ctxKey, "data")

		pg, _ := NewTestStore(t, postgres.WithTransformer(store.TransformerFunc(func(ctx context.Context, msg messenger.Message) error {
			msg.Metadata().Set(ctxKey, ctx.Value(ctxKey).(string))
			return nil
		})))

		require := require.New(t)
		msg, err := messenger.NewMessage([]byte("message"))
		require.NoError(err)
		msg.SetMetadata("some", "meta")

		require.NoError(pg.Store(ctx, nil, msg))

		msgs, err := pg.Messages(context.Background(), 1)
		require.NoError(err)

		for _, got := range msgs {
			require.Equal(msg.Metadata(), got.Metadata())
			require.Equal(msg.Payload(), got.Payload())
		}
	})
	t.Run("transformation fails", func(t *testing.T) {
		t.Parallel()
		someErr := errors.New("some err")
		pg, _ := NewTestStore(t, postgres.WithTransformer(store.TransformerFunc(func(context.Context, messenger.Message) error {
			return someErr
		})))

		require := require.New(t)
		msg, err := messenger.NewMessage([]byte("message"))
		require.NoError(err)
		msg.SetMetadata("some", "meta")

		require.ErrorIs(pg.Store(context.TODO(), nil, msg), someErr)
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

		_, err := db.Exec(context.Background(),
			fmt.Sprintf(`
				INSERT INTO
					%q
					(id, metadata, payload, published, created_at)
				VALUES
					($1, $2, $3, true, $4)`,
				postgres.DefaultMessagesTable),
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

func TestFind(t *testing.T) {
	t.Parallel()

	pg, _ := NewTestStore(t)

	require := require.New(t)

	publishMsgs := make([]messenger.Message, 15)
	for i := 0; i < 15; i++ {
		var err error
		msg, err := messenger.NewMessage([]byte(strconv.Itoa(i + 1)))
		require.NoError(err)
		msg.SetMetadata("some", fmt.Sprintf("meta-%d", i+1))
		publishMsgs[i] = msg
	}

	require.NoError(pg.Store(context.Background(), nil, publishMsgs...))

	result, err := pg.Find(context.Background(), &inspect.Query{
		Pagination: inspect.Pagination{
			Page:  1,
			Limit: 10,
		},
	})
	require.NoError(err)

	require.Equal(15, result.Total)
	require.Len(result.Msgs, 10)

	expected := publishMsgs[5:15]
	slices.Reverse(expected)
	for i := range expected {
		require.Equal(expected[i].ID(), result.Msgs[i].ID())
		require.Equal(expected[i].Metadata(), result.Msgs[i].Metadata())
		require.Equal(expected[i].Payload(), result.Msgs[i].Payload())
		require.Equal(expected[i].Published(), result.Msgs[i].Published())
	}
}
