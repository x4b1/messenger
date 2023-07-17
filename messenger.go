package messenger

import (
	"context"
	"errors"
	"time"

	"github.com/x4b1/messenger/log"
)

const (
	defaultBatchSize = 100
)

//go:generate moq -stub -pkg messenger_test -out mock_test.go . Store Publisher ErrorLogger

// Source is the interface that wraps the message retrieval and update methods.
type Store interface {
	// List unpublished messages with a batch size
	Messages(ctx context.Context, batch int) ([]Message, error)
	// Mark as published the given messages, if one of the messages fails will not update any of the messages
	Published(ctx context.Context, msg ...Message) error
	// Deletes messages marked as published and older than expiration period from datastore.
	DeletePublishedByExpiration(ctx context.Context, exp time.Duration) error
}

// Publisher is the interface that wraps the basic message publishing.
type Publisher interface {
	// Sends the message to broker.
	Publish(ctx context.Context, msg Message) error
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

// Option defines the optional parameters for messenger.
type Option func(*Messenger)

func WithPublishBatchSize(bs int) Option {
	return func(w *Messenger) {
		w.batchSize = bs
	}
}

func WithInterval(p time.Duration) Option {
	return func(w *Messenger) {
		w.interval = p
	}
}

func WithErrorLogger(l ErrorLogger) Option {
	return func(w *Messenger) {
		w.logger = l
	}
}

func WithCleanUp(expiration time.Duration) Option {
	return func(w *Messenger) {
		w.expiration = expiration
	}
}

// NewMessenger returns a `Messenger` instance with defaults.
//   - Publish batch size: 100
//   - Publish period: 1s
//   - Golang standard error logger.
func NewMessenger(store Store, publisher Publisher, opts ...Option) *Messenger {
	p := Messenger{
		interval:  time.Second,
		batchSize: defaultBatchSize,

		logger:    log.NewDefault(),
		publisher: publisher,
		store:     store,
	}
	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

// Messenger is responsible of publishing messages from datastore to publisher,
// and cleaning already published messages.
type Messenger struct {
	interval time.Duration

	// publish params
	batchSize int

	// clean params
	expiration time.Duration

	logger    ErrorLogger
	store     Store
	publisher Publisher
}

// Publish runs once publishing process.
func (w *Messenger) Publish(ctx context.Context) error {
	msgs, err := w.store.Messages(ctx, w.batchSize)
	if err != nil {
		return &fatalError{err}
	}

	errs := []error{}
	for _, msg := range msgs {
		if err := w.publisher.Publish(ctx, msg); err != nil {
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

// Clean runs once the message cleaning process given a message expiration time.
func (w *Messenger) Clean(ctx context.Context) error {
	return w.store.DeletePublishedByExpiration(ctx, w.expiration)
}

// Start runs the process of publishing/cleaning messages every period.
// In case there is a publish error, it will call to error handler without stopping the process.
// If a fatal error happens, ex, cant connect to datastore it will stop the process.
func (w *Messenger) Start(ctx context.Context) error {
	t := time.NewTicker(w.interval)
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
			if w.expiration > 0 {
				if err := w.Clean(ctx); err != nil {
					return err
				}
			}
		}
	}
}
