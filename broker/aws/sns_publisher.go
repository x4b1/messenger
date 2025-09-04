package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

var _ broker.Broker = &SNSPublisher{}

// SNSPublisherOption is a function to set options to SNSPublisher.
type SNSPublisherOption interface {
	applySNSPublisher(*SNSPublisher)
}

// OpenSNSPublisher creates a new SNSPublisher using the default AWS configuration.
func OpenSNSPublisher(
	ctx context.Context,
	topicARN string,
	opts ...SNSPublisherOption,
) (*SNSPublisher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return NewSNSPublisher(sns.NewFromConfig(cfg), topicARN, opts...), nil
}

// NewSNSPublisher creates a new SNSPublisher with the given SNS client and topic ARN.
func NewSNSPublisher(cli SNSClient, topicARN string, opts ...SNSPublisherOption) *SNSPublisher {
	p := SNSPublisher{
		cli:      cli,
		topicARN: topicARN,
		msgIDKey: broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt.applySNSPublisher(&p)
	}

	return &p
}

// SNSPublisher is an implementation of broker.Broker that publishes messages to AWS SNS topics.
// It supports both standard and FIFO topics, allowing for message ordering and deduplication.
// The publisher can be customized via SNSPublisherOption to set ordering keys, deduplication, and other behaviors.
// Messages are published with metadata as SNS message attributes, including a message ID for tracking.
type SNSPublisher struct {
	// sns service instance where are going to publish messages
	cli SNSClient
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

// Publish sends the provided message to the configured AWS SNS topic.
// It attaches message metadata as SNS attributes and includes a message ID for tracking.
// For FIFO topics, it sets ordering and deduplication keys as required.
func (p SNSPublisher) Publish(ctx context.Context, msg messenger.Message) error {
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
func (p SNSPublisher) messageDeduplication(msg messenger.Message) *string {
	if !p.fifo {
		return nil
	}

	return aws.String(msg.ID())
}

// orderingKey tries to get the ordering key from message metadata
// in case the message does not have the key it defaults to Publisher setup.
func (p SNSPublisher) orderingKey(msg messenger.Message) *string {
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
