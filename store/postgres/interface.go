package postgres

import (
	"context"
)

// Instance defines the postgres interface to be used by the store.
type Instance interface {
	Executor
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

type Rows interface {
	Row
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while reading.
	Err() error

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available. It automatically closes rows
	// when all rows are read.
	Next() bool
}

type Row interface {
	Scan(dest ...any) error
}

type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) error
}
