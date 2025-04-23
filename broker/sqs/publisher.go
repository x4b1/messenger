package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

var _ broker.Broker = &Publisher{}

var awsStringDataType = aws.String("String") //nolint: gochecknoglobals // aws constant

// PublisherOption is a function to set options to Publisher.
type PublisherOption func(*Publisher)

// PublisherWithMetaOrderingKey setups the metadata key to get the ordering key.
func PublisherWithMetaOrderingKey(key string) PublisherOption {
	return func(p *Publisher) {
		p.metaOrdKey = key
	}
}

// PublisherWithDefaultOrderingKey setups the default ordering key.
func PublisherWithDefaultOrderingKey(key string) PublisherOption {
	return func(p *Publisher) {
		p.defaultOrdKey = key
	}
}

// PublisherWithFifoQueue setups the flag to use fifo queue.
func PublisherWithFifoQueue(fifo bool) PublisherOption {
	return func(p *Publisher) {
		p.fifo = fifo
	}
}

// PublisherWithMessageIDKey modify default message id key.
func PublisherWithMessageIDKey(key string) PublisherOption {
	return func(p *Publisher) {
		p.msgIDKey = key
	}
}

// NewPublisherFromDefault returns a new Publisher instance.
func NewPublisherFromDefault(
	ctx context.Context,
	queue string,
	opts ...PublisherOption,
) (*Publisher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading aws config from default: %w", err)
	}

	return NewPublisher(ctx, sqs.NewFromConfig(cfg), queue, opts...)
}

// NewPublisher returns a new Publisher instance.
func NewPublisher(
	ctx context.Context,
	svc Client,
	queue string,
	opts ...PublisherOption,
) (*Publisher, error) {
	q, err := svc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queue)})
	if err != nil {
		return nil, fmt.Errorf("getting queue url: %w", err)
	}

	p := Publisher{
		svc:      svc,
		queue:    aws.ToString(q.QueueUrl),
		msgIDKey: broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt(&p)
	}

	return &p, nil
}

// Publisher handles the pubsub topic messages.
type Publisher struct {
	// sqs service instance where are going to publish messages
	svc Client
	// queue url where are going to publish messages
	queue string
	// meta property of the message to use as ordering key
	metaOrdKey string
	// default ordering key in case not provided in message metadata
	defaultOrdKey string
	// flag to use fifo queue
	fifo bool
	// metadata key where will be send the message id.
	msgIDKey string
}

// Publish publishes the given message to the pubsub topic.
func (p Publisher) Publish(ctx context.Context, msg messenger.Message) error {
	md := msg.Metadata()
	att := make(map[string]types.MessageAttributeValue)

	for k, v := range md {
		att[k] = types.MessageAttributeValue{
			DataType:    awsStringDataType,
			StringValue: aws.String(v),
		}
	}

	att[p.msgIDKey] = types.MessageAttributeValue{
		DataType:    awsStringDataType,
		StringValue: aws.String(msg.ID()),
	}

	_, err := p.svc.SendMessage(
		ctx,
		&sqs.SendMessageInput{
			MessageDeduplicationId: p.messageDeduplication(msg),
			MessageAttributes:      att,
			MessageBody:            aws.String(string(msg.Payload())),
			QueueUrl:               aws.String(p.queue),
			MessageGroupId:         p.orderingKey(msg),
		})
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}

	return nil
}

// messageDeduplication checks if the publisher is setup as fifo and returns the message deduplication id.
func (p Publisher) messageDeduplication(msg messenger.Message) *string {
	if !p.fifo {
		return nil
	}

	return aws.String(msg.ID())
}

// orderingKey tries to get the ordering key from message metadata
// in case the message does not have the key it defaults to Publisher setup.
func (p Publisher) orderingKey(msg messenger.Message) *string {
	if !p.fifo {
		return nil
	}

	key, ok := msg.Metadata()[p.metaOrdKey]
	if ok {
		return aws.String(key)
	}

	if p.defaultOrdKey != "" {
		return aws.String(p.defaultOrdKey)
	}

	return nil
}
