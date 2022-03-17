package publish

import (
	"context"
	"time"
)

// Reporter has the functions to report the status of the publishing process.
type Reporter interface {
	Report(ctx context.Context, r *Report)
}

// Report defines the fields needed to register publish process
type Report struct {
	StartTime time.Time
	EndTime   time.Time
	Error     error
	Total     int
	Success   int
}

// NoopReporter is a reporter that does nothing.
type NoopReporter struct{}

// Report implements the Reporter interface.
func (*NoopReporter) Report(ctx context.Context, r *Report) {}
