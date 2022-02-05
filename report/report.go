package report

import (
	"time"
)

type stats struct {
	StartTime time.Time `json:"start"`
	EndTime   time.Time `json:"end"`
	Err       error     `json:"error"`
	Total     int       `json:"total"`
}

type Reporter struct {
	stats stats
}

// Init initialises the report stats.
func (r *Reporter) Init() {
	r.stats = stats{
		StartTime: time.Now(),
	}
}

// Error registers the error that occurred during the publishing process.
func (r *Reporter) Error(err error) {
	r.stats.Err = err
}

// TotalMessages registers the total number of messages to be published.
func (r *Reporter) TotalMessages(total int) {
	r.stats.Total = total
}

// Finish marks as ended a single run of the worker, and register the result.
func (r *Reporter) Finish() {
	r.stats.EndTime = time.Now()
}
