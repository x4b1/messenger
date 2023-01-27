package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/internal/postgres"
	"github.com/x4b1/messenger/publish"
)

// WithTableName setups table name.
func WithTableName(t string) postgres.Option {
	return postgres.WithTableName(t)
}

// WithSchema setups schema name.
func WithSchema(s string) postgres.Option {
	return postgres.WithSchema(s)
}

type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type instance interface {
	executor
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var _ postgres.Instance = (*wrapper)(nil)

func newWrapper(i instance) *wrapper {
	return &wrapper{i}
}

type wrapper struct {
	instance
}

func (w *wrapper) Ping(ctx context.Context) error {
	return w.instance.Ping(ctx)
}

func (w *wrapper) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	return w.instance.Query(ctx, sql, args...)
}

func (w *wrapper) QueryRow(ctx context.Context, sql string, args ...any) postgres.Row {
	return w.instance.QueryRow(ctx, sql, args...)
}

func (w *wrapper) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := w.instance.Exec(ctx, sql, args...)

	return err
}

type execWrapper struct {
	executor
}

func (ew *execWrapper) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := ew.executor.Exec(ctx, sql, args...)

	return err
}

// Open returns a pgx source connected to database connection string with config.
func Open(ctx context.Context, connStr string, opts ...postgres.Option) (*Store, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}

	return WithConn(ctx, conn, opts...)
}

// WithInstance returns Store source initialised with the given connection instance and config.
func WithConn(ctx context.Context, conn *pgx.Conn, opts ...postgres.Option) (*Store, error) {
	s, err := postgres.New(ctx, newWrapper(conn), opts...)

	return &Store{s}, err
}

func WithPool(ctx context.Context, pool *pgxpool.Pool, opts ...postgres.Option) (*Store, error) {
	s, err := postgres.New(ctx, newWrapper(pool), opts...)

	return &Store{s}, err
}

var _ publish.Source = (*Store)(nil)

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store struct {
	*postgres.Storer
}

func (s *Store) Store(ctx context.Context, tx pgx.Tx, msgs ...messenger.Message) error {
	var exec postgres.Executor
	if tx != nil {
		exec = &execWrapper{tx}
	}

	return s.Storer.Store(ctx, exec, msgs...)
}
