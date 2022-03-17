package publish

import (
	"context"
	"time"

	"github.com/xabi93/messenger/store"
)

//go:generate moq -stub -out publish_mock_test.go . Source Queue Reporter

// Source is the interface that wraps the message retrieval and update methods.
type Source interface {
	// List unpublished messages with a batch size.
	Messages(ctx context.Context, publisherName string, batch int) ([]*store.Message, error)
	// Stores the status of the last published message.
	SaveLastPublished(ctx context.Context, publisherName string, msg *store.Message) error
}

// Queue is the interface that wraps the basic message publishing.
type Queue interface {
	// Sends the message to queue
	Publish(ctx context.Context, msg *store.Message) error
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
func NewPublisher(name string, src Source, queue Queue, batchSize int, opts ...Option) *Publisher {
	p := Publisher{
		name:      name,
		batchSize: batchSize,
		source:    src,
		queue:     queue,
		reporter:  &NoopReporter{},
	}
	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

// Publisher is responsible for publishing messages from a source to a queue.
type Publisher struct {
	name      string
	batchSize int
	source    Source
	queue     Queue
	reporter  Reporter
}

func (p *Publisher) publish(ctx context.Context) (int, int, error) {
	msgs, err := p.source.Messages(ctx, p.name, p.batchSize)
	if err != nil {
		return 0, 0, WrapFatalError(err)
	}

	var (
		lastPublished *store.Message
		totalSuccess  int
	)
	for _, msg := range msgs {
		err = p.queue.Publish(ctx, msg)
		if err != nil {
			break
		}
		totalSuccess++
		lastPublished = msg
	}

	if lastPublished != nil {
		if err := p.source.SaveLastPublished(ctx, p.name, lastPublished); err != nil {
			return len(msgs), 0, WrapFatalError(err)
		}
	}

	return len(msgs), totalSuccess, err
}

// Publish runs once publishing process.
func (p *Publisher) Publish(ctx context.Context) error {
	report := Report{
		StartTime: time.Now(),
	}

	defer func() {
		report.EndTime = time.Now()
		p.reporter.Report(ctx, &report)
	}()

	report.Total, report.Success, report.Error = p.publish(ctx)

	return report.Error
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
			if err := p.Publish(ctx); err != nil {
				if IsFatalError(err) {
					return err
				}
			}
		}
	}
}
