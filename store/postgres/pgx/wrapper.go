package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/x4b1/messenger/store/postgres"
)

var _ postgres.Instance = (*wrapper)(nil)

type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Instance is an abstraction of pgx API, to allow to use pgx.Conn or pgxpool.Pool.
type Instance interface {
	executor
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func newWrapper(i Instance) *wrapper {
	return &wrapper{i}
}

type wrapper struct {
	instance Instance
}

func (w *wrapper) Ping(ctx context.Context) error {
	return w.instance.Ping(ctx)
}

func (w *wrapper) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	//nolint:sqlclosecheck,rowserrcheck // just propagating
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
