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
	"github.com/x4b1/messenger/log"
	"golang.org/x/sync/errgroup"
)

const (
	defaultMaxWaitSeconds  = 20
	defaultReceiveMessages = 1
)

// SubscriberOption is a function to set options to Subscriber.
type SubscriberOption func(*Subscriber)

// SubscriberWithMaxWaitSeconds replaces default max time wait seconds.
func SubscriberWithMaxWaitSeconds(waitSec int) SubscriberOption {
	return func(s *Subscriber) {
		s.maxWaitSeconds = waitSec
	}
}

// SubscriberWithMaxMessages replaces default number of messages to receive.
func SubscriberWithMaxMessages(msgs int) SubscriberOption {
	return func(s *Subscriber) {
		s.maxMessages = msgs
	}
}

// SubscriberWithMessageIDKey replaces default metadata key for id.
func SubscriberWithMessageIDKey(key string) SubscriberOption {
	return func(s *Subscriber) {
		s.msgIDKey = key
	}
}

// NewSubscriberFromDefault returns a new Publisher instance.
func NewSubscriberFromDefault(ctx context.Context, queue string, opts ...SubscriberOption) (*Subscriber, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading aws config from default: %w", err)
	}

	return NewSubscriber(sqs.NewFromConfig(cfg), opts...), nil
}

// NewSubscriber returns a new Publisher instance.
func NewSubscriber(cli Client, opts ...SubscriberOption) *Subscriber {
	s := Subscriber{
		cli:        cli,
		subs:       make([]messenger.Subscription, 0),
		errHandler: log.NewDefault(),
		group:      new(errgroup.Group),

		maxWaitSeconds: defaultMaxWaitSeconds,
		maxMessages:    defaultReceiveMessages,
		msgIDKey:       broker.MessageIDKey,
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s
}

// Subscriber registers subscriptions to AWS SQS.
type Subscriber struct {
	cli        Client
	errHandler messenger.ErrorHandler
	group      *errgroup.Group
	subs       []messenger.Subscription

	maxWaitSeconds int
	maxMessages    int
	msgIDKey       string
}

// Register adds subscriptions to subscriber.
func (s *Subscriber) Register(subs ...messenger.Subscription) {
	s.subs = append(s.subs, subs...)
}

// subscribe registers one subscription.
func (s *Subscriber) subscribe(ctx context.Context, sub messenger.Subscription) error {
	queueURL, err := s.cli.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(sub.Name()),
	})
	if err != nil {
		return fmt.Errorf("%s: %w", sub.Name(), err)
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
					return fmt.Errorf("%s: %w", aws.ToString(queueURL.QueueUrl), err)
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

func (s *Subscriber) processMessage(ctx context.Context, sub messenger.Subscription, msg types.Message) error {
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

// Listen starts listening for events.
func (s *Subscriber) Listen(ctx context.Context) error {
	for _, sub := range s.subs {
		if err := s.subscribe(ctx, sub); err != nil {
			return err
		}
	}

	return s.group.Wait()
}
