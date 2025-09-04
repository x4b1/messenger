package aws_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
	awsx "github.com/x4b1/messenger/broker/aws"
)

var errUnexpected = errors.New("error")

var (
	awsMessageID = "104ed4b7-f36e-4b71-af4d-72fa0857ef33"
	customMsgID  = "a3fe4a83-ebb0-425f-96b6-edf322fc4dba"
	message      = &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				MessageId: aws.String(awsMessageID),
				Body:      aws.String("hello world"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"AN":                {StringValue: aws.String("ATTRIBUTE")},
					broker.MessageIDKey: {StringValue: aws.String(customMsgID)},
				},
			},
		},
	}
)

func TestSQSSubscriber_Subscribe(t *testing.T) {
	queueURLOut := &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://awsx.eu-west-1.amazonaws.com/12345/test"),
	}
	t.Run("fails getting queue url", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", nil)

		s := awsx.NewSQSSubscriber(&SQSClientMock{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return nil, errUnexpected
			},
		})
		s.Register(testSub)

		require.ErrorIs(t, s.Listen(context.Background()), errUnexpected)
	})

	t.Run("fails receiving messages", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", nil)

		s := awsx.NewSQSSubscriber(&SQSClientMock{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return queueURLOut, nil
			},
			ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				return nil, errUnexpected
			},
		})
		s.Register(testSub)

		require.ErrorIs(t, s.Listen(context.Background()), errUnexpected)
	})

	t.Run("no messages does nothing", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", nil)

		ctx, cancel := context.WithCancel(context.Background())

		receiveTimeCalls := 0

		s := awsx.NewSQSSubscriber(&SQSClientMock{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return queueURLOut, nil
			},
			ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				receiveTimeCalls++
				if receiveTimeCalls >= 2 {
					cancel()
				}
				return &sqs.ReceiveMessageOutput{}, nil
			},
		})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("handling error fails", func(t *testing.T) {
		testSub := messenger.NewSubscription(
			"test",
			func(context.Context, messenger.Message) error { return errUnexpected },
		)

		ctx, cancel := context.WithCancel(context.Background())

		s := awsx.NewSQSSubscriber(
			&SQSClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					cancel()

					return message, nil
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("deleting message fails", func(t *testing.T) {
		testSub := messenger.NewSubscription(
			"test",
			func(context.Context, messenger.Message) error { return nil },
		)

		ctx, cancel := context.WithCancel(context.Background())

		s := awsx.NewSQSSubscriber(
			&SQSClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					cancel()

					return message, nil
				},
				DeleteMessageFunc: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
					return nil, errUnexpected
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("Success", func(t *testing.T) {
		var (
			hID       string
			hMsg      []byte
			hMetadata map[string]string
		)

		testSub := messenger.NewSubscription(
			"test",
			func(_ context.Context, msg messenger.Message) error {
				hID = msg.ID()
				hMsg = msg.Payload()
				hMetadata = msg.Metadata()

				return nil
			},
		)

		ctx, cancel := context.WithCancel(context.Background())

		s := awsx.NewSQSSubscriber(
			&SQSClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					cancel()

					return message, nil
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))

		require.Equal(t, customMsgID, hID)
		require.Equal(t, aws.ToString(message.Messages[0].Body), string(hMsg))
		require.Equal(t, map[string]string{"AN": "ATTRIBUTE"}, hMetadata)
	})
	t.Run("Success without id gets from aws", func(t *testing.T) {
		var hID string

		testSub := messenger.NewSubscription(
			"test",
			func(_ context.Context, msg messenger.Message) error {
				hID = msg.ID()

				return nil
			},
		)

		ctx, cancel := context.WithCancel(context.Background())

		s := awsx.NewSQSSubscriber(
			&SQSClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					cancel()

					return &sqs.ReceiveMessageOutput{
						Messages: []types.Message{
							{
								MessageId: aws.String(awsMessageID),
								Body:      aws.String("hello world"),
								MessageAttributes: map[string]types.MessageAttributeValue{
									"AN": {StringValue: aws.String("ATTRIBUTE")},
								},
							},
						},
					}, nil
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))

		require.Equal(t, awsMessageID, hID)
	})

	t.Run("Success with custom id key", func(t *testing.T) {
		var hID string

		testSub := messenger.NewSubscription(
			"test",
			func(_ context.Context, msg messenger.Message) error {
				hID = msg.ID()

				return nil
			},
		)

		ctx, cancel := context.WithCancel(context.Background())

		s := awsx.NewSQSSubscriber(
			&SQSClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					cancel()

					return &sqs.ReceiveMessageOutput{
						Messages: []types.Message{
							{
								MessageId: aws.String(awsMessageID),
								Body:      aws.String("hello world"),
								MessageAttributes: map[string]types.MessageAttributeValue{
									"AN":         {StringValue: aws.String("ATTRIBUTE")},
									"custom_key": {StringValue: aws.String("custom_id")},
								},
							},
						},
					}, nil
				},
			}, awsx.WithMessageIDKey("custom_key"))
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))

		require.Equal(t, "custom_id", hID)
	})
}
