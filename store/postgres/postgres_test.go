package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-txdb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	store "github.com/x4b1/messenger/store/postgres/stdsql"
)

var (
	db    *sql.DB
	dbURL string
)

func TestMain(m *testing.M) {
	close, err := setupPostgresDB()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := store.Open(context.Background(), dbURL); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	close()

	os.Exit(code)
}

func setupPostgresDB() (func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("Could not connect to docker: %w", err)
	}

	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%s", "test"),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", "test"),
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

	dbURL = fmt.Sprintf("postgres://%s:%s@localhost:%s/postgres", "test", "test", postgresContainer.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept dbections yet
	if err := pool.Retry(func() error {
		db, err = sql.Open("pgx", dbURL)
		if err != nil {
			return err
		}

		return db.PingContext(context.Background())
	}); err != nil {
		pool.Purge(postgresContainer)
		return nil, err
	}

	txdb.Register("txdb", "pgx", dbURL)

	return func() {
		pool.Purge(postgresContainer)
	}, nil
}

func NewTestStore(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()

	txDB, err := sql.Open("txdb", fmt.Sprintf("connection_%d", time.Now().UnixNano()))
	require.NoError(t, err)

	s, err := store.WithInstance(context.Background(), txDB)
	require.NoError(t, err)

	return s, txDB
}
