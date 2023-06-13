package test

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

const (
	PostgresImage    = "postgres"
	PostgresVersion  = "15"
	PostgresUser     = "test"
	PostgresPassword = PostgresUser
)

func Setup(ctx context.Context) (*pgx.Conn, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to docker: %w", err)
	}

	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: PostgresImage,
		Tag:        PostgresVersion,
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%s", PostgresUser),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", PostgresPassword),
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

	if err = postgresContainer.Expire(60); err != nil {
		return nil, nil, err
	}

	dbURL := fmt.Sprintf("postgres://%s:%s@localhost:%s/postgres", PostgresUser, PostgresPassword, postgresContainer.GetPort("5432/tcp"))

	var conn *pgx.Conn
	// exponential backoff-retry, because the application in the container might not be ready to accept dbections yet
	if err = pool.Retry(func() error {
		if conn, err = pgx.Connect(ctx, dbURL); err != nil {
			return err
		}

		return conn.Ping(ctx)
	}); err != nil {
		_ = pool.Purge(postgresContainer)
		return nil, nil, err
	}

	return conn, func() {
		_ = pool.Purge(postgresContainer)
	}, nil
}
