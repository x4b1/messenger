package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/store/postgres"
)

// Ensure implements messenger.Store interface.
var _ messenger.Store = (*Store)(nil)

// Open returns a pgx source connected to database connection string with config.
func Open(ctx context.Context, connStr string, opts ...postgres.Option) (*Store, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}

	return WithConn(ctx, conn, opts...)
}

// WithConn returns Store source initialised with the given connection instance and config.
func WithConn(ctx context.Context, conn *pgx.Conn, opts ...postgres.Option) (*Store, error) {
	s, err := postgres.New(ctx, newWrapper(conn), opts...)

	return &Store{s}, err
}

// WithPool returns Store source initialised with the given connection pool instance and config.
func WithPool(ctx context.Context, pool *pgxpool.Pool, opts ...postgres.Option) (*Store, error) {
	s, err := postgres.New(ctx, newWrapper(pool), opts...)

	return &Store{s}, err
}

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store struct {
	*postgres.Storer
}

// Store saves message in postgres database with the given transaction.
func (s *Store) Store(ctx context.Context, tx pgx.Tx, msgs ...messenger.Message) error {
	var exec postgres.Executor
	if tx != nil {
		exec = &execWrapper{tx}
	}

	return s.Storer.Store(ctx, exec, msgs...)
}
