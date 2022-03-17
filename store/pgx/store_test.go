//nolint: paralleltest // need connection pool in order to run tests in parallel
// TODO: run tests in parallel
package pgx_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/xabi93/messenger"
	store "github.com/xabi93/messenger/store/pgx"
)

const (
	user     = "test"
	password = "test"

	publisherName = "my-publisher"
)

var conn *pgx.Conn

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "10",
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

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		conn, err = pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s:%s@localhost:%s/postgres", user, password, postgresContainer.GetPort("5432/tcp")))
		if err != nil {
			return err
		}

		return conn.Ping(context.Background())
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	if err := pool.Purge(postgresContainer); err != nil {
		log.Fatalf("Could not purge postgres: %s", err)
	}

	os.Exit(code)
}

func TestOpen(t *testing.T) {
	require := require.New(t)

	_, err := store.Open(context.Background(), conn.Config().ConnString(), store.Config{})
	require.NoError(err)
}

func TestCustomTable(t *testing.T) {
	require := require.New(t)

	msgtable := "my-messages"
	statusTable := "my-status"

	_, err := store.WithInstance(context.Background(), conn, store.Config{
		MessagesTable:      msgtable,
		PublishStatusTable: statusTable,
	})
	require.NoError(err)
	row := conn.QueryRow(
		context.Background(),
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name IN ($2, $3) LIMIT 1`,
		"public",
		msgtable,
		statusTable,
	)

	var count int
	require.NoError(row.Scan(&count))
	require.Equal(count, 2)
}

func TestCustomSchemaNotExistsReturnsError(t *testing.T) {
	require := require.New(t)

	_, err := store.WithInstance(context.Background(), conn, store.Config{
		Schema: "custom",
	})

	require.Error(err)
}

func TestInitializeTwiceNotReturnError(t *testing.T) {
	require := require.New(t)

	_, err := store.WithInstance(context.Background(), conn, store.Config{})
	require.NoError(err)

	_, err = store.WithInstance(context.Background(), conn, store.Config{})
	require.NoError(err)
}

func TestStorePublishMessages(t *testing.T) {
	totalMsgs := 15
	batch := 10

	pg, err := store.WithInstance(context.Background(), conn, store.Config{})
	require.NoError(t, err)

	t.Run("with transaction", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)

		t.Cleanup(func() {
			conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s", store.MessagesTable))
		})

		tx, err := conn.Begin(ctx)
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
		require.NoError(tx.Commit(ctx))

		msgs, err := pg.Messages(ctx, publisherName, batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}

		require.NoError(pg.SaveLastPublished(ctx, publisherName, msgs[len(msgs)-1]))

		msgs, err = pg.Messages(ctx, publisherName, batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
		}
	})

	t.Run("without transaction", func(t *testing.T) {
		require := require.New(t)

		t.Cleanup(func() {
			conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s", store.MessagesTable))
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

		msgs, err := pg.Messages(context.Background(), publisherName, batch)
		require.NoError(err)

		require.Len(msgs, batch)
		for i, msg := range publishMsgs[:batch] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}

		require.NoError(pg.SaveLastPublished(context.Background(), publisherName, msgs[len(msgs)-1]))

		msgs, err = pg.Messages(context.Background(), publisherName, batch)
		require.NoError(err)

		require.Len(msgs, totalMsgs-batch)
		for i, msg := range publishMsgs[batch:] {
			require.Equal(msg.Metadata(), msgs[i].Metadata)
			require.Equal(msg.Payload(), msgs[i].Payload)
		}
	})
}
