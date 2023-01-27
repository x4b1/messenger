package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Instance defines the postgres interface to be used by the store.
type Instance interface {
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Executor
}

type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while reading.
	Err() error

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available. It automatically closes rows
	// when all rows are read.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely. It is an error to
	// call Scan without first calling Next() and checking that it returned true.
	Scan(dest ...any) error
}

type Row pgx.Row

type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) error
}
