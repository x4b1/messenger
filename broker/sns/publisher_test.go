package sns_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
	publisher "github.com/x4b1/messenger/broker/sns"
)

const (
	topicARN = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

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

func TestPublish(t *testing.T) {
	t.Parallel()
	t.Run("fails", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		snsMock := ClientMock{
			PublishFunc: func(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error) {
				return nil, errAws
			},
		}

		pub, err := publisher.New(&snsMock, topicARN)
		require.NoError(t, err)

		require.ErrorIs(t, pub.Publish(ctx, msg), errAws)
	})

	for _, tc := range []struct {
		name          string
		expectedInput *sns.PublishInput
		opts          []publisher.Option
	}{
		{
			name: "no ordering key",
			expectedInput: &sns.PublishInput{
				MessageDeduplicationId: nil,
				Message:                aws.String(string(msg.Payload())),
				MessageGroupId:         nil,
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.Metadata()["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				TopicArn: aws.String(topicARN),
			},
		},
		{
			name: "default ordering key",
			opts: []publisher.Option{publisher.WithFifoQueue(true), publisher.WithDefaultOrderingKey(defaultOrdKey)},
			expectedInput: &sns.PublishInput{
				MessageDeduplicationId: aws.String(msg.ID()),
				Message:                aws.String(string(msg.Payload())),
				MessageGroupId:         aws.String(defaultOrdKey),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.Metadata()["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				TopicArn: aws.String(topicARN),
			},
		},
		{
			name: "metadata ordering key",
			opts: []publisher.Option{
				publisher.WithFifoQueue(true),
				publisher.WithDefaultOrderingKey(defaultOrdKey),
				publisher.WithMetaOrderingKey(metaKey),
			},
			expectedInput: &sns.PublishInput{
				MessageDeduplicationId: aws.String(msg.ID()),
				Message:                aws.String(string(msg.Payload())),
				MessageGroupId:         aws.String(orderingValue),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id":      {DataType: aws.String("String"), StringValue: aws.String(msg.Metadata()["aggregate_id"])},
					metaKey:             {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					broker.MessageIDKey: {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				TopicArn: aws.String(topicARN),
			},
		},
		{
			name: "custom message id key",
			opts: []publisher.Option{
				publisher.WithMessageIDKey("custom_key"),
			},
			expectedInput: &sns.PublishInput{
				Message: aws.String(string(msg.Payload())),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"aggregate_id": {DataType: aws.String("String"), StringValue: aws.String(msg.MsgMetadata["aggregate_id"])},
					metaKey:        {DataType: aws.String("String"), StringValue: aws.String(orderingValue)},
					"custom_key":   {DataType: aws.String("String"), StringValue: aws.String(msg.MsgID)},
				},
				TopicArn: aws.String(topicARN),
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := require.New(t)
			ctx := context.Background()

			snsMock := ClientMock{}

			pub, err := publisher.New(&snsMock, topicARN, tc.opts...)
			r.NoError(err)

			r.NoError(pub.Publish(ctx, msg))

			r.Len(snsMock.PublishCalls(), 1)
			r.Equal(tc.expectedInput, snsMock.PublishCalls()[0].Params)
		})
	}
}
