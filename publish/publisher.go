package publish

import (
	"context"
	"errors"
	"time"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/report"
)

//go:generate moq -stub -out publish_mock_test.go . Source Queue Reporter

// Source is the interface that wraps the message retrieval and update methods.
type Source interface {
	// List unpublished messages with a batch size
	Messages(ctx context.Context, batch int64) ([]messenger.Message, error)
	// Mark as published the given messages, if one of the messages fails will not update any of the messages
	Published(ctx context.Context, msg ...messenger.Message) error
}

// Queue is the interface that wraps the basic message publishing.
type Queue interface {
	// Sends the message to queue
	Publish(ctx context.Context, msg messenger.Message) error
}

// Reporter has the functions to report the status of the publishing process.
type Reporter interface {
	// outputs an error happened during the publishing message process
	Error(err error)
	// initialises the reporting
	Init()
	// Stores total messages to be published
	TotalMessages(total int)
	// marks as ended a single run of the worker
	Finish()
}

// Option defines the optional parameters for publisher.
type Option func(*Publisher)

// WithReport sets a custom Reporter for publisher.
func WithReport(r Reporter) Option {
	return func(p *Publisher) {
		p.reporter = r
	}
}

// NewPublisher returns a `Publisher` instance
// By default the worker will be initialised with NoopReporter that will not do any output.
func NewPublisher(src Source, queue Queue, batchSize int64, opts ...Option) *Publisher {
	p := Publisher{
		batchSize: batchSize,
		source:    src,
		queue:     queue,
		reporter:  report.Noop{},
	}
	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

// Publisher is responsible for publishing messages from a source to a queue.
type Publisher struct {
	batchSize int64
	source    Source
	queue     Queue
	reporter  Reporter
}

// Publish runs once publishing process.
func (p *Publisher) Publish(ctx context.Context) error {
	p.reporter.Init()
	defer p.reporter.Finish()

	msgs, err := p.source.Messages(ctx, p.batchSize)
	if err != nil {
		return err
	}

	errs := NewPublishErrors()
	for _, msg := range msgs {
		if err := p.queue.Publish(ctx, msg); err != nil {
			errs.Add(msg, err)
			continue
		}
		if err := p.source.Published(ctx, msg); err != nil {
			errs.Add(msg, err)
			continue
		}
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}

// Start starts the publishing process. In case there is an error, it will be reported to the reporter,
// without stopping the process.
func (p *Publisher) Start(ctx context.Context, t *time.Ticker) error {
	for {
		select {
		case <-ctx.Done():
			t.Stop()

			return nil
		case <-t.C:
			err := p.Publish(ctx)
			if err != nil {
				p.reporter.Error(err)
				if !errors.As(err, &PublishErrors{}) {
					return err
				}
			}
		}
	}
}
