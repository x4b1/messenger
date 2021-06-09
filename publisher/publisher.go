package publisher

import (
	"context"
	"time"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publisher/report"
)

//go:generate go run github.com/matryer/moq -stub -out publisher_mock_test.go . Source Publisher Reporter

// Source is the interface that wraps the message retrieval and update methods.
type Source interface {
	// List unpublished messages with a batch size
	Messages(ctx context.Context, batch int64) ([]*messenger.Message, error)
	// Mark as published the given messages
	Published(ctx context.Context, msg ...*messenger.Message) error
}

// Source is the interface that wraps the basic message publishing.
type Publisher interface {
	// Send to the message queue the message
	Publish(ctx context.Context, msg *messenger.Message) error
}

// Reporter is the interface that wraps worker report methods.
type Reporter interface {
	// outputs an error happened during the publishing message process
	Error(err error)
	// initializes the reporting
	Init(totalMsgs int)
	// reports failed publishing message
	Failed(msg messenger.Message, err error)
	// reports success publishing message
	Success(msg messenger.Message)
	// marks as ended a single run of the worker
	End()
}

// Option defines the optional parameters for the worker.
type Option func(*Worker)

// WithReport sets a custom Reporter for worker.
func WithReport(r Reporter) Option {
	return func(m *Worker) {
		m.reporter = r
	}
}

// NewWorker returns an initialized Worker given the parameters.
// By default the worker will be initialized with NoopReporter that will not do any output.
func NewWorker(src Source, pub Publisher, batchSize int64, ticker *time.Ticker, opts ...Option) Worker {
	m := Worker{
		batchSize: batchSize,
		ticker:    ticker,
		source:    src,
		publisher: pub,
		reporter:  report.Noop{},
	}
	for _, opt := range opts {
		opt(&m)
	}

	return m
}

// Worker wraps the logic of publishing an amount of (batchSize) messages periodically (ticker) from a source,
// to a queue (publisher).
type Worker struct {
	batchSize int64
	ticker    *time.Ticker

	source    Source
	publisher Publisher
	reporter  Reporter
}

// publish runs once the process of publishing messages and reports the result of published messages.
func (w Worker) publish(ctx context.Context) {
	msgs, err := w.source.Messages(ctx, w.batchSize)
	if err != nil {
		w.reporter.Error(err)

		return
	}
	w.reporter.Init(len(msgs))

	published := make([]*messenger.Message, 0, len(msgs))
	for _, msg := range msgs {
		if err := w.publisher.Publish(ctx, msg); err != nil {
			w.reporter.Failed(*msg, err)

			continue
		}
		published = append(published, msg)
		w.reporter.Success(*msg)
	}

	if len(published) != 0 {
		if err := w.source.Published(ctx, published...); err != nil {
			w.reporter.Error(err)

			return
		}
	}

	w.reporter.End()
}

// Start starts the worker, it will be stopped once the received context is done.
func (w Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.ticker.Stop()

			return
		case <-w.ticker.C:
			w.publish(ctx)
		}
	}
}
