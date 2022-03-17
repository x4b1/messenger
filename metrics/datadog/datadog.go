package datadog

import (
	"context"

	"github.com/DataDog/datadog-go/v5/statsd"

	"github.com/xabi93/messenger/publish"
)

var _ publish.Reporter = (*Reporter)(nil)

func NewDatadog(cli statsd.ClientInterface) *Reporter {
	return &Reporter{cli}
}

// Metrics is a datadog wrapper for metrics registerer
type Reporter struct {
	cli statsd.ClientInterface
}

func (m *Reporter) Report(_ context.Context, report *publish.Report) {
	m.cli.Gauge("publish.messages.total", float64(report.Total), nil, 1)
	m.cli.Gauge("publish.messages.success", float64(report.Success), nil, 1)
	if report.Error != nil {
		m.cli.Gauge("publish.error", 1, nil, 1)
	}
}
