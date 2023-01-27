package stdsql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"

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

type db interface {
	PingContext(context.Context) error
	QueryContext(ctx context.Context, sql string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, sql string, args ...any) *sql.Row
	exec
}

type exec interface {
	ExecContext(ctx context.Context, sql string, args ...any) (sql.Result, error)
}

type conn struct {
	db db
	executor
}

func (c *conn) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *conn) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	r, err := c.db.QueryContext(ctx, sql, args...)
	return &rows{r}, err
}

func (c *conn) QueryRow(ctx context.Context, sql string, args ...any) postgres.Row {
	return c.db.QueryRowContext(ctx, sql, args...)
}

type executor struct {
	exec
}

func (c *executor) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := c.exec.ExecContext(ctx, sql, args...)

	return err
}

type rows struct {
	*sql.Rows
}

func (r *rows) Close() {
	_ = r.Rows.Close()
}

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

var _ publish.Source = (*Store)(nil)

type Store struct {
	*postgres.Storer
}

func (s *Store) Store(ctx context.Context, tx *sql.Tx, msgs ...messenger.Message) error {
	var exec postgres.Executor
	if tx != nil {
		exec = &executor{tx}
	}

	return s.Storer.Store(ctx, exec, msgs...)
}
