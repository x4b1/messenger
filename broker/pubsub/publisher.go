// Package pubsub Google pubsub broker implementation
package pubsub

import (
	"context"
	"maps"

	"cloud.google.com/go/pubsub/v2"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

var _ broker.Broker = &Publisher{}

// Option is a function to set options to Publisher.
type Option func(*Publisher)

// WithMetaOrderingKey setups the metadata key to get the ordering key.
func WithMetaOrderingKey(key string) Option {
	return func(p *Publisher) {
		p.publisher.EnableMessageOrdering = true
		p.metaOrdKey = key
	}
}

// WithDefaultOrderingKey setups the default ordering key.
func WithDefaultOrderingKey(key string) Option {
	return func(p *Publisher) {
		p.publisher.EnableMessageOrdering = true
		p.defaultOrdKey = key
	}
}

// WithMessageIDKey modify default message id key.
func WithMessageIDKey(key string) Option {
	return func(p *Publisher) {
		p.msgIDKey = key
	}
}

// Open returns a new Publisher instance.
func Open(pubsubClient *pubsub.Client, topicID string, opts ...Option) *Publisher {
	return New(pubsubClient.Publisher(topicID), opts...)
}

// New returns a new Publisher instance.
func New(publisher *pubsub.Publisher, opts ...Option) *Publisher {
	p := Publisher{
		publisher: publisher,
		msgIDKey:  broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

// Publisher handles the pubsub topic messages.
type Publisher struct {
	// pubsub topic instance where are going to publish messages
	publisher *pubsub.Publisher
	// meta property of the message to use as ordering key
	metaOrdKey string
	// default ordering key in case not provided in message metadata
	defaultOrdKey string
	// metadata key where will be send the message id.
	msgIDKey string
}

// Publish publishes the given message to the pubsub topic.
func (p Publisher) Publish(ctx context.Context, msg messenger.Message) error {
	md := make(map[string]string)
	maps.Copy(md, msg.Metadata())

	md[p.msgIDKey] = msg.ID()

	_, err := p.publisher.Publish(ctx, &pubsub.Message{
		Attributes:  md,
		Data:        msg.Payload(),
		OrderingKey: p.orderingKey(msg),
	}).Get(ctx)

	return err
}

// orderingKey tries to get the ordering key from message metadata
// in case the message does not have the key it defaults to Publisher setup.
func (p Publisher) orderingKey(msg messenger.Message) string {
	key, ok := msg.Metadata()[p.metaOrdKey]
	if ok {
		return key
	}

	return p.defaultOrdKey
}
