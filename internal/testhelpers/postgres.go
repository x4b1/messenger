package testhelpers

import (
	"context"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresStartupWaitTime     = 5 * time.Second
	postgresReadyLogOccurrences = 2
)

//nolint:gochecknoglobals // need it for singleton
var (
	postgresOnce sync.Once
	pgContainer  PostgresContainer
)

// PostgresContainer contains a docker instance of postgres and the url where is exposed.
type PostgresContainer struct {
	*postgres.PostgresContainer
	ConnectionString string
}

// CreatePostgresContainer starts a postgres container and returns its instance.
func CreatePostgresContainer(ctx context.Context) (*PostgresContainer, error) {
	var err error
	postgresOnce.Do(func() {
		pgContainer.PostgresContainer, err = postgres.Run(ctx,
			"postgres:16.4-alpine",
			postgres.WithDatabase("test-db"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(postgresReadyLogOccurrences).
					WithStartupTimeout(postgresStartupWaitTime)),
		)
		if err != nil {
			return
		}
		pgContainer.ConnectionString, err = pgContainer.PostgresContainer.ConnectionString(ctx, "sslmode=disable")
	})
	if err != nil {
		return nil, err
	}

	return &pgContainer, nil
}
