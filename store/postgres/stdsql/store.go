package stdsql

import (
	"context"
	"database/sql"
	"fmt"

	// initialize pgx stdlib driver.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/store/postgres"
)

// Ensure implements messenger.Store interface.
var _ messenger.Store = (*Store)(nil)

// Open returns a pgx source connected to database connection string with config.
func Open(ctx context.Context, connStr string, opts ...postgres.Option) (*Store, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres connect parsing conf: %w", err)
	}

	return WithInstance(ctx, db, opts...)
}

// WithInstance returns Store source initialised with the given connection instance and config.
func WithInstance(ctx context.Context, db *sql.DB, opts ...postgres.Option) (*Store, error) {
	s, err := postgres.New(ctx, &conn{db, executor{db}}, opts...)

	return &Store{s}, err
}

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store struct {
	*postgres.Storer
}

// Store saves message in postgres database with the given transaction.
func (s *Store) Store(ctx context.Context, tx *sql.Tx, msgs ...messenger.Message) error {
	var exec postgres.Executor
	if tx != nil {
		exec = &executor{tx}
	}

	return s.Storer.Store(ctx, exec, msgs...)
}
