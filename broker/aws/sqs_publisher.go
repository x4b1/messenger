package aws

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

var _ broker.Broker = &SQSPublisher{}

// SQSPublisherOption defines an interface for applying configuration options to SQSPublisher instances.
type SQSPublisherOption interface {
	applySQSPublisher(*SQSPublisher)
}

// OpenSQSPublisher creates a new SQSPublisher using the default AWS configuration.
func OpenSQSPublisher(
	ctx context.Context,
	queueARN string,
	opts ...SQSPublisherOption,
) (*SQSPublisher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return NewSQSPublisher(sqs.NewFromConfig(cfg), queueARN, opts...), nil
}

// NewSQSPublisher creates a new SQSPublisher with the given SQS client and queue ARN.
func NewSQSPublisher(svc SQSClient, queueARN string, opts ...SQSPublisherOption) *SQSPublisher {
	p := SQSPublisher{
		svc:      svc,
		queue:    queueARN,
		msgIDKey: broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt.applySQSPublisher(&p)
	}

	return &p
}

// SQSPublisher is an implementation of broker.Broker that publishes messages to AWS SQS queues.
// It supports both standard and FIFO queues, allowing for message ordering and deduplication.
// The publisher can be customized via SQSPublisherOption to set ordering keys, deduplication, and other behaviors.
// Messages are published with metadata as SQS message attributes, including a message ID for tracking.
type SQSPublisher struct {
	// sqs service instance where are going to publish messages
	svc SQSClient
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
func (p SQSPublisher) Publish(ctx context.Context, msg messenger.Message) error {
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
func (p SQSPublisher) messageDeduplication(msg messenger.Message) *string {
	if !p.fifo {
		return nil
	}

	return aws.String(msg.ID())
}

// orderingKey tries to get the ordering key from message metadata
// in case the message does not have the key it defaults to Publisher setup.
func (p SQSPublisher) orderingKey(msg messenger.Message) *string {
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
