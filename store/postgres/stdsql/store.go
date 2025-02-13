// Package stdsql expose the methods to connect to postgres with std driver.
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
var _ messenger.Store = (*Store[any])(nil)

// Open returns a pgx source connected to database connection string with config.
func Open[T any](ctx context.Context, connStr string, opts ...postgres.Option) (*Store[T], error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres connect parsing conf: %w", err)
	}

	return WithInstance[T](ctx, db, opts...)
}

// WithInstance returns Store source initialised with the given connection instance and config.
func WithInstance[T any](ctx context.Context, db *sql.DB, opts ...postgres.Option) (*Store[T], error) {
	s, err := postgres.New[T](ctx, &conn{db, executor{db}}, opts...)

	return &Store[T]{s}, err
}

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store[T any] struct {
	*postgres.Storer[T]
}

// Store saves message in postgres database with the given transaction.
func (s *Store[T]) Store(ctx context.Context, tx *sql.Tx, msgs ...T) error {
	var exec postgres.Executor
	if tx != nil {
		exec = &executor{tx}
	}

	return s.Storer.Store(ctx, exec, msgs...)
}
