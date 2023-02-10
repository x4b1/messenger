package messenger

import (
	"context"
	"errors"
	"time"

	"github.com/x4b1/messenger/log"
	"github.com/x4b1/messenger/store"
	"golang.org/x/sync/errgroup"
)

const (
	defaultBatchSize = 100
)

//go:generate moq -stub -pkg messenger_test -out mock_test.go . Store Queue ErrorLogger

// Source is the interface that wraps the message retrieval and update methods.
type Store interface {
	// List unpublished messages with a batch size
	Messages(ctx context.Context, batch int) ([]*store.Message, error)
	// Mark as published the given messages, if one of the messages fails will not update any of the messages
	Published(ctx context.Context, msg ...*store.Message) error
	// Deletes messages marked as published and older than expiration period from datastore.
	DeletePublishedByExpiration(ctx context.Context, exp time.Duration) error
}

// Queue is the interface that wraps the basic message publishing.
type Queue interface {
	// Sends the message to queue
	Publish(ctx context.Context, msg *store.Message) error
}

// ErrorLogger is the interface that wraps the basic message publishing.
type ErrorLogger interface {
	Error(err error)
}

type fatalError struct {
	err error
}

func (e *fatalError) Error() string {
	return e.err.Error()
}

func (e *fatalError) Unwrap() error {
	return e.err
}

// Option defines the optional parameters for worker.
type Option func(*Worker)

func WithPublishBatchSize(bs int) Option {
	return func(w *Worker) {
		w.batchSize = bs
	}
}

func WithPublishPeriod(p time.Duration) Option {
	return func(w *Worker) {
		w.publishPeriod = p
	}
}

func WithErrorLogger(l ErrorLogger) Option {
	return func(w *Worker) {
		w.logger = l
	}
}

func WithCleanUp(period time.Duration, expiration time.Duration) Option {
	return func(w *Worker) {
		w.cleanPeriod = period
		w.expiration = expiration
	}
}

// NewWorker returns a `Worker` instance with defaults.
//   - Publish batch size: 100
//   - Publish period: 1s
//   - Golang standard error logger.
func NewWorker(store Store, queue Queue, opts ...Option) *Worker {
	p := Worker{
		publishPeriod: time.Second,
		batchSize:     defaultBatchSize,

		logger: log.NewDefault(),
		queue:  queue,
		store:  store,
	}
	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

// Worker is responsible of publishing messages from datastore to queue,
// and cleaning already published messages.
type Worker struct {
	// publish params
	publishPeriod time.Duration
	batchSize     int

	// clean params
	cleanPeriod time.Duration
	expiration  time.Duration

	logger ErrorLogger
	store  Store
	queue  Queue
}

// Publish runs once publishing process.
func (w *Worker) Publish(ctx context.Context) error {
	msgs, err := w.store.Messages(ctx, w.batchSize)
	if err != nil {
		return &fatalError{err}
	}

	errs := []error{}
	for _, msg := range msgs {
		if err := w.queue.Publish(ctx, msg); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := w.store.Published(ctx, msg); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return errors.Join(errs...)
}

// StartPublisher runs the publishing process.
// In case there is a publish error, it will call to error handler without stopping the process.
// If a fatal error happens, ex, cant connect to datastore it will stop the process.
func (w *Worker) StartPublisher(ctx context.Context) error {
	t := time.NewTicker(w.publishPeriod)
	for {
		select {
		case <-ctx.Done():
			t.Stop()

			return nil
		case <-t.C:
			err := w.Publish(ctx)
			if err != nil {
				var fatalErr *fatalError
				if errors.As(err, &fatalErr) {
					return err
				}
				w.logger.Error(err)
			}
		}
	}
}

// Clean runs once the message cleaning process given a message expiration time.
func (w *Worker) Clean(ctx context.Context) error {
	return w.store.DeletePublishedByExpiration(ctx, w.expiration)
}

// StartClean runs the cleaning process preriodically defined by the clean period.
// If there is an error it stops and returns the error.
func (w *Worker) StartClean(ctx context.Context) error {
	t := time.NewTicker(w.cleanPeriod)
	for {
		select {
		case <-ctx.Done():
			t.Stop()

			return nil
		case <-t.C:
			if err := w.Clean(ctx); err != nil {
				return err
			}
		}
	}
}

// Start runs publish and clean processes.
func (w *Worker) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return w.StartPublisher(ctx)
	})

	if w.cleanPeriod > 0 {
		g.Go(func() error {
			return w.StartClean(ctx)
		})
	}

	return g.Wait()
}
