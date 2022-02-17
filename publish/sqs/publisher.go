package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/xabi93/messenger/publish"
	"github.com/xabi93/messenger/store"
)

// Option is a function to set options to Publisher.
type Option func(*Publisher)

// WithMetaOrderingKey setups the metadata key to get the ordering key.
func WithMetaOrderingKey(key string) Option {
	return func(p *Publisher) {
		p.metaOrdKey = key
	}
}

// WithDefaultOrderingKey setups the default ordering key.
func WithDefaultOrderingKey(key string) Option {
	return func(p *Publisher) {
		p.defaultOrdKey = key
	}
}

// New returns a new Publisher instance.
func New(svc *sqs.SQS, queue string, opts ...Option) *Publisher {
	p := Publisher{svc: svc}

	for _, opt := range opts {
		opt(&p)
	}

	return &p
}

var _ publish.Queue = &Publisher{}

// Publisher handles the pubsub topic messages.
type Publisher struct {
	// sqs service instance where are going to publish messages
	svc *sqs.SQS
	// queue url where are going to publish messages
	queue string
	// meta property of the message to use as ordering key
	metaOrdKey string
	// default ordering key in case not provided in message metadata
	defaultOrdKey string
}

// Publish publishes the given message to the pubsub topic.
func (p Publisher) Publish(ctx context.Context, msg store.Message) error {
	att := make(map[string]*sqs.MessageAttributeValue, len(msg.Metadata))
	for k, v := range msg.Metadata {
		att[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	_, err := p.svc.SendMessage(&sqs.SendMessageInput{
		MessageDeduplicationId: aws.String(msg.ID),
		MessageAttributes:      att,
		MessageBody:            aws.String(string(msg.Payload)),
		QueueUrl:               aws.String(p.queue),
		MessageGroupId:         p.orderingKey(msg),
	})

	return err
}

// orderingKey tries to get the ordering key from message metadata
// in case the message does not have the key it defaults to Publisher setup.
func (p Publisher) orderingKey(msg store.Message) *string {
	key, ok := msg.Metadata[p.metaOrdKey]
	if ok {
		return aws.String(key)
	}

	if p.defaultOrdKey != "" {
		return aws.String(p.defaultOrdKey)
	}

	return nil
}
