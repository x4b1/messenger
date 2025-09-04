package aws

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
	"github.com/x4b1/messenger/log"
	"golang.org/x/sync/errgroup"
)

const (
	defaultMaxWaitSeconds  = 20
	defaultReceiveMessages = 1
)

// SQSSubscriberOption defines an interface for applying configuration options to SQSSubscriber instances.
type SQSSubscriberOption interface {
	applySQSSubscriber(*SQSSubscriber)
}

// OpenSQSSubscriber initializes a new SQSSubscriber using the default AWS configuration and provided options.
// It loads the AWS config from the environment and returns a ready-to-use subscriber.
func OpenSQSSubscriber(ctx context.Context, opts ...SQSSubscriberOption) (*SQSSubscriber, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading aws config from default: %w", err)
	}

	return NewSQSSubscriber(sqs.NewFromConfig(cfg), opts...), nil
}

// NewSQSSubscriber creates a new SQSSubscriber with the given AWS config, SQS client, and options.
// This function allows for custom configuration and dependency injection.
func NewSQSSubscriber(cli SQSClient, opts ...SQSSubscriberOption) *SQSSubscriber {
	s := SQSSubscriber{
		cli:        cli,
		subs:       make([]messenger.Subscription, 0),
		errHandler: log.NewDefault(),
		group:      new(errgroup.Group),

		maxWaitSeconds: defaultMaxWaitSeconds,
		maxMessages:    defaultReceiveMessages,
		msgIDKey:       broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt.applySQSSubscriber(&s)
	}

	return &s
}

// SQSSubscriber manages subscriptions and message processing for AWS SQS queues.
// It handles receiving, processing, and deleting messages from SQS queues.
type SQSSubscriber struct {
	cli SQSClient

	errHandler messenger.ErrorHandler
	group      *errgroup.Group
	subs       []messenger.Subscription

	maxWaitSeconds int
	maxMessages    int
	msgIDKey       string
}

// Register adds one or more subscriptions to the SQSSubscriber.
// Subscriptions define the queues and handlers to listen to.
func (s *SQSSubscriber) Register(subs ...messenger.Subscription) {
	s.subs = append(s.subs, subs...)
}

// subscribe registers one subscription.
func (s *SQSSubscriber) subscribe(ctx context.Context, sub messenger.Subscription) error {
	arnSplit := strings.Split(sub.Name(), ":")

	queueURL, err := s.cli.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(arnSplit[len(arnSplit)-1]),
	})
	if err != nil {
		return fmt.Errorf("getting queue url for %s: %w", sub.Name(), err)
	}

	s.group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				msgs, err := s.cli.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:              queueURL.QueueUrl,
					MaxNumberOfMessages:   int32(s.maxMessages),
					WaitTimeSeconds:       int32(s.maxWaitSeconds),
					MessageAttributeNames: []string{"All"},
				})
				if err != nil {
					return fmt.Errorf("%s: %w", sub.Name(), err)
				}

				for _, msg := range msgs.Messages {
					err := s.processMessage(ctx, sub, msg)
					if err != nil {
						s.errHandler.Error(ctx, err)
						continue
					}
					if _, err := s.cli.DeleteMessage(ctx, &sqs.DeleteMessageInput{
						ReceiptHandle: msg.ReceiptHandle,
						QueueUrl:      queueURL.QueueUrl,
					}); err != nil {
						s.errHandler.Error(ctx, err)
						continue
					}
				}
			}
		}
	})
	return nil
}

func (s *SQSSubscriber) processMessage(
	ctx context.Context,
	sub messenger.Subscription,
	msg types.Message,
) error {
	parsed := messenger.GenericMessage{
		MsgPayload: []byte(aws.ToString(msg.Body)),
	}

	parsed.MsgMetadata = make(map[string]string, len(msg.MessageAttributes))
	for k, v := range msg.MessageAttributes {
		if k == s.msgIDKey {
			parsed.MsgID = aws.ToString(v.StringValue)
			continue
		}
		parsed.MsgMetadata[k] = aws.ToString(v.StringValue)
	}
	if parsed.MsgID == "" {
		parsed.MsgID = aws.ToString(msg.MessageId)
	}

	return sub.Handle(ctx, &parsed)
}

// Listen starts the message polling and processing loop for all registered subscriptions.
// It blocks until all subscription goroutines have finished or an error occurs.
func (s *SQSSubscriber) Listen(ctx context.Context) error {
	for _, sub := range s.subs {
		if err := s.subscribe(ctx, sub); err != nil {
			return err
		}
	}

	return s.group.Wait()
}
