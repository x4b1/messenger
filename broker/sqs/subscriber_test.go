package sqs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker/sqs"
)

var errUnexpected = errors.New("error")

var message = &awssqs.ReceiveMessageOutput{
	Messages: []types.Message{
		{
			Body: aws.String("hello world"),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"AN": {StringValue: aws.String("ATTRIBUTE")},
			},
		},
	},
}

func TestSubscriber(t *testing.T) {
	queueURLOut := &awssqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://sqs.eu-west-1.amazonaws.com/12345/test"),
	}
	t.Run("fails getting queue url", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", nil)

		s := sqs.NewSubscriber(&ClientMock{
			GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
				return nil, errUnexpected
			},
		})
		s.Register(testSub)

		require.ErrorIs(t, s.Listen(context.Background()), errUnexpected)
	})

	t.Run("fails receiving messages", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", nil)

		s := sqs.NewSubscriber(&ClientMock{
			GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
				return queueURLOut, nil
			},
			ReceiveMessageFunc: func(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
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

		s := sqs.NewSubscriber(&ClientMock{
			GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
				return queueURLOut, nil
			},
			ReceiveMessageFunc: func(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
				receiveTimeCalls++
				if receiveTimeCalls >= 2 {
					cancel()
				}
				return &awssqs.ReceiveMessageOutput{}, nil
			},
		})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("handling error fails", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", func(ctx context.Context, msg []byte, metadata map[string]string) error { return errUnexpected })

		ctx, cancel := context.WithCancel(context.Background())

		s := sqs.NewSubscriber(
			&ClientMock{
				GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
					cancel()

					return &awssqs.ReceiveMessageOutput{
						Messages: []types.Message{
							{
								Body:       aws.String("hello world"),
								Attributes: map[string]string{"AN": "ATTRIBUTE"},
							},
						},
					}, nil
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("deleting message fails", func(t *testing.T) {
		testSub := messenger.NewSubscription("test", func(ctx context.Context, msg []byte, metadata map[string]string) error { return nil })

		ctx, cancel := context.WithCancel(context.Background())

		s := sqs.NewSubscriber(
			&ClientMock{
				GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
					cancel()

					return message, nil
				},
				DeleteMessageFunc: func(contextMoqParam context.Context, deleteMessageInput *awssqs.DeleteMessageInput, fns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
					return nil, errUnexpected
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))
	})

	t.Run("Success", func(t *testing.T) {
		var (
			hMsg      []byte
			hMetadata map[string]string
		)

		testSub := messenger.NewSubscription("test", func(ctx context.Context, msg []byte, metadata map[string]string) error {
			hMsg = msg
			hMetadata = metadata

			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())

		s := sqs.NewSubscriber(
			&ClientMock{
				GetQueueUrlFunc: func(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
					return queueURLOut, nil
				},
				ReceiveMessageFunc: func(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
					cancel()

					return message, nil
				},
			})
		s.Register(testSub)

		require.NoError(t, s.Listen(ctx))

		require.Equal(t, aws.ToString(message.Messages[0].Body), string(hMsg))
		require.Equal(t, map[string]string{"AN": "ATTRIBUTE"}, hMetadata)
	})
}
