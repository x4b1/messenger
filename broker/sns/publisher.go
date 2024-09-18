package sns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

var _ broker.Broker = &Publisher{}

var awsStringDataType = aws.String("String") //nolint: gochecknoglobals // aws constant

//go:generate moq -pkg sns_test -stub -out publisher_mock_test.go . Client

// Client defines the AWS SNS methods used by the Publisher. This is used for testing purposes.
type Client interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

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

// WithMessageIDKey modify default message id key.
func WithMessageIDKey(key string) Option {
	return func(p *Publisher) {
		p.msgIDKey = key
	}
}

// WithFifoQueue setups the flag to use fifo queue.
func WithFifoQueue(fifo bool) Option {
	return func(p *Publisher) {
		p.fifo = fifo
	}
}

// Open returns a new Publisher instance.
func Open(awsOpts sns.Options, topicARN string, opts ...Option) (*Publisher, error) {
	return New(sns.New(awsOpts), topicARN, opts...)
}

// New returns a new Publisher instance.
func New(cli Client, topicARN string, opts ...Option) (*Publisher, error) {
	p := Publisher{
		cli:      cli,
		topicARN: topicARN,
		msgIDKey: broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt(&p)
	}

	return &p, nil
}

// Publisher handles the pubsub topic messages.
type Publisher struct {
	// sns service instance where are going to publish messages
	cli Client
	// queue url where are going to publish messages
	topicARN string
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

	_, err := p.cli.Publish(
		ctx,
		&sns.PublishInput{
			MessageDeduplicationId: p.messageDeduplication(msg),
			MessageAttributes:      att,
			Message:                aws.String(string(msg.Payload())),
			TopicArn:               aws.String(p.topicARN),
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
