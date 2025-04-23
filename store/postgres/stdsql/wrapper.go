package stdsql

import (
	"context"
	"database/sql"

	"github.com/x4b1/messenger/store/postgres"
)

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
	//nolint:rowserrcheck // just propagating rows.
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
	_, err := c.ExecContext(ctx, sql, args...)

	return err
}

type rows struct {
	*sql.Rows
}

func (r *rows) Close() {
	_ = r.Rows.Close()
}
