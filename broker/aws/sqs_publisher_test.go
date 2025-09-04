package aws_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger/broker"
	publisher "github.com/x4b1/messenger/broker/aws"
)

const (
	queueARN = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
)

func TestPublish(t *testing.T) {
	t.Parallel()
	t.Run("fails", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqsMock := SQSClientMock{
			SendMessageFunc: func(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
				return nil, errAws
			},
		}

		pub := publisher.NewSQSPublisher(&sqsMock, queueARN)

		require.ErrorIs(t, pub.Publish(ctx, msg), errAws)
	})

	for _, tc := range []struct {
		name          string
		expectedInput *sqs.SendMessageInput
		opts          []publisher.SQSPublisherOption
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
				QueueUrl: aws.String(queueARN),
			},
		},
		{
			name: "default ordering key",
			opts: []publisher.SQSPublisherOption{
				publisher.WithFifoQueue(true),
				publisher.WithDefaultOrderingKey(defaultOrdKey),
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
				QueueUrl: aws.String(queueARN),
			},
		},
		{
			name: "metadata ordering key",
			opts: []publisher.SQSPublisherOption{
				publisher.WithFifoQueue(true),
				publisher.WithDefaultOrderingKey(defaultOrdKey),
				publisher.WithMetaOrderingKey(metaKey),
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
				QueueUrl: aws.String(queueARN),
			},
		},
		{
			name: "custom message id key",
			opts: []publisher.SQSPublisherOption{
				publisher.WithMessageIDKey("custom_key"),
			},
			expectedInput: &sqs.SendMessageInput{
				MessageBody: aws.String(string(msg.Payload())),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id": {DataType: aws.String("String"), StringValue: aws.String(msg.MsgMetadata["aggregate_id"])},
					metaKey:        {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					"custom_key":   {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				QueueUrl: aws.String(queueARN),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := require.New(t)
			ctx := context.Background()

			sqsMock := SQSClientMock{}

			pub := publisher.NewSQSPublisher(&sqsMock, queueARN, tc.opts...)

			r.NoError(pub.Publish(ctx, msg))

			r.Len(sqsMock.SendMessageCalls(), 1)
			r.Equal(tc.expectedInput, sqsMock.SendMessageCalls()[0].SendMessageInput)
		})
	}
}
