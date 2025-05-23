package sqs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
	publisher "github.com/x4b1/messenger/broker/sqs"
)

const (
	queue    = "test-queue"
	queueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

	defaultOrdKey = "default-ordering"

	metaKey       = "meta-key"
	orderingValue = "value-1"
)

var errAws = errors.New("aws error")

var msg = &messenger.GenericMessage{
	MsgID: uuid.NewString(),
	MsgMetadata: map[string]string{
		"aggregate_id": "29a7556a-ae85-4c1d-8f04-d57ed3122586",
		metaKey:        orderingValue,
	},
	MsgPayload: []byte("some message"),
}

func TestFailsGettingQueueURL(t *testing.T) {
	sqsMock := ClientMock{
		GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return nil, errAws
		},
	}

	_, err := publisher.NewPublisher(context.Background(), &sqsMock, queue)
	require.ErrorIs(t, err, errAws)
}

func TestPublish(t *testing.T) {
	t.Parallel()
	t.Run("fails", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqsMock := ClientMock{
			GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
				return &sqs.GetQueueUrlOutput{
					QueueUrl: aws.String(queueURL),
				}, nil
			},

			SendMessageFunc: func(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
				return nil, errAws
			},
		}

		pub, err := publisher.NewPublisher(ctx, &sqsMock, queue)
		require.NoError(t, err)

		require.ErrorIs(t, pub.Publish(ctx, msg), errAws)
	})

	for _, tc := range []struct {
		name          string
		expectedInput *sqs.SendMessageInput
		opts          []publisher.PublisherOption
	}{
		{
			name: "no ordering key",
			expectedInput: &sqs.SendMessageInput{
				MessageDeduplicationId: nil,
				MessageBody:            aws.String(string(msg.Payload())),
				MessageGroupId:         nil,
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.MsgMetadata["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				QueueUrl: aws.String(queueURL),
			},
		},
		{
			name: "default ordering key",
			opts: []publisher.PublisherOption{
				publisher.PublisherWithFifoQueue(true),
				publisher.PublisherWithDefaultOrderingKey(defaultOrdKey),
			},
			expectedInput: &sqs.SendMessageInput{
				MessageDeduplicationId: aws.String(msg.ID()),
				MessageBody:            aws.String(string(msg.Payload())),
				MessageGroupId:         aws.String(defaultOrdKey),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.Metadata()["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				QueueUrl: aws.String(queueURL),
			},
		},
		{
			name: "metadata ordering key",
			opts: []publisher.PublisherOption{
				publisher.PublisherWithFifoQueue(true),
				publisher.PublisherWithDefaultOrderingKey(defaultOrdKey),
				publisher.PublisherWithMetaOrderingKey(metaKey),
			},
			expectedInput: &sqs.SendMessageInput{
				MessageDeduplicationId: aws.String(msg.ID()),
				MessageBody:            aws.String(string(msg.Payload())),
				MessageGroupId:         aws.String(orderingValue),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.MsgMetadata["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				QueueUrl: aws.String(queueURL),
			},
		},
		{
			name: "custom message id key",
			opts: []publisher.PublisherOption{
				publisher.PublisherWithMessageIDKey("custom_key"),
			},
			expectedInput: &sqs.SendMessageInput{
				MessageBody: aws.String(string(msg.Payload())),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id": {DataType: aws.String("String"), StringValue: aws.String(msg.MsgMetadata["aggregate_id"])},
					metaKey:        {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					"custom_key":   {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				QueueUrl: aws.String(queueURL),
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := require.New(t)
			ctx := context.Background()

			sqsMock := ClientMock{
				GetQueueUrlFunc: func(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
					return &sqs.GetQueueUrlOutput{
						QueueUrl: aws.String(queueURL),
					}, nil
				},
			}

			pub, err := publisher.NewPublisher(ctx, &sqsMock, queue, tc.opts...)
			r.NoError(err)

			r.NoError(pub.Publish(ctx, msg))

			r.Len(sqsMock.SendMessageCalls(), 1)
			r.Equal(tc.expectedInput, sqsMock.SendMessageCalls()[0].SendMessageInput)
		})
	}
}
