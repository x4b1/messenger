package stdsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger"
	store "github.com/x4b1/messenger/store/stdsql"
)

const (
	user     = "test"
	password = "test"
)

var (
	db    *sql.DB
	dbURL string
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not dbect to docker: %s", err)
	}

	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "13",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%s", user),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		log.Fatalf("Could not start postgres: %s", err)
	}

	postgresContainer.Expire(60)

	dbURL = fmt.Sprintf("postgres://%s:%s@localhost:%s/postgres", user, password, postgresContainer.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept dbections yet
	if err := pool.Retry(func() error {
		db, err = sql.Open("pgx", dbURL)
		if err != nil {
			return err
		}

		return db.PingContext(context.Background())
	}); err != nil {
		log.Fatalf("Could not dbect to docker: %s", err)
	}

	code := m.Run()

	if err := pool.Purge(postgresContainer); err != nil {
		log.Fatalf("Could not purge postgres: %s", err)
	}

	os.Exit(code)
}

func TestOpen(t *testing.T) {
	require := require.New(t)

	_, err := store.Open(context.Background(), dbURL)
	require.NoError(err)
}

func TestCustomTable(t *testing.T) {
	require := require.New(t)

	table := "my-messages"

	_, err := store.WithInstance(context.Background(), db, store.WithTableName(table))
	require.NoError(err)
	row := db.QueryRowContext(
		context.Background(),
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		"public",
		table,
	)

	var count int
	require.NoError(row.Scan(&count))
	require.Equal(count, 1)
}

func TestCustomSchemaNotExistsReturnsError(t *testing.T) {
	require := require.New(t)

	_, err := store.WithInstance(context.Background(), db, store.WithSchema("custom"))

	require.Error(err)
}

func TestInitializeTwiceNotReturnError(t *testing.T) {
	require := require.New(t)

	_, err := store.WithInstance(context.Background(), db)
	require.NoError(err)

	_, err = store.WithInstance(context.Background(), db)
	require.NoError(err)
}

func TestStorePublishMessages(t *testing.T) {
	var (
		totalMsgs = 15
		batch     = 10
	)

	pg, err := store.WithInstance(context.Background(), db)
	require.NoError(t, err)

	t.Run("with transaction", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)

		t.Cleanup(func() {
			db.ExecContext(context.Background(), fmt.Sprintf("TRUNCATE %s", store.MessagesTable))
		})

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(err)

		publishMsgs := make([]messenger.Message, totalMsgs)
		for i := 0; i < totalMsgs; i++ {
			var err error
			msg, err := messenger.NewMessage([]byte(fmt.Sprintf("%d", i+1)))
			require.NoError(err)
			msg.AddMetadata("some", fmt.Sprintf("meta-%d", i+1))
			msg.AddMetadata("test", "with transaction")
			publishMsgs[i] = msg
		}

		require.NoError(pg.Store(ctx, tx, publishMsgs...))
		require.NoError(tx.Commit())

		msgs, err := pg.Messages(ctx, batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}

		require.NoError(pg.Published(ctx, msgs...))

		msgs, err = pg.Messages(ctx, batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
		}
	})

	t.Run("without transaction", func(t *testing.T) {
		require := require.New(t)

		t.Cleanup(func() {
			db.ExecContext(context.Background(), fmt.Sprintf("TRUNCATE %s", store.MessagesTable))
		})

		publishMsgs := make([]messenger.Message, totalMsgs)
		for i := 0; i < totalMsgs; i++ {
			var err error
			msg, err := messenger.NewMessage([]byte(fmt.Sprintf("%d", i+1)))
			require.NoError(err)
			msg.AddMetadata("some", fmt.Sprintf("meta-%d", i+1))
			msg.AddMetadata("test", "without transaction")
			publishMsgs[i] = msg
		}

		require.NoError(pg.Store(context.Background(), nil, publishMsgs...))

		msgs, err := pg.Messages(context.Background(), batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}

		require.NoError(pg.Published(context.Background(), msgs...))

		msgs, err = pg.Messages(context.Background(), batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}
	})
}
