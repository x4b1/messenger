// Package log provides provides logging functionality.
package log

import (
	"context"
	"log/slog"
)

// NewDefault returns an instance of error handler using default log/slog.
func NewDefault() *Default {
	return &Default{slog.Default()}
}

// Default uses std implementation of slog.
type Default struct {
	l *slog.Logger
}

// Error prints the given error using slog error method.
func (d *Default) Error(ctx context.Context, err error) {
	d.l.ErrorContext(ctx, err.Error())
}
