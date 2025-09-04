// Package aws provides abstractions and clients for interacting with AWS SNS (Simple Notification Service)
// and SQS (Simple Queue Service). It defines interfaces for the subset of SNS and SQS operations required
// by the application's publisher and subscriber components, enabling easier testing and mocking of AWS services.
// The package is intended to be used as a bridge between the application logic and AWS messaging infrastructure.
package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var awsStringDataType = aws.String("String") //nolint: gochecknoglobals // aws constant

//go:generate go tool moq -pkg aws_test -stub -out aws_mock_test.go . SNSClient SQSClient

// SNSClient defines the AWS SNS methods used by the Publisher. This is used for testing purposes.
type SNSClient interface {
	Publish(
		ctx context.Context,
		params *sns.PublishInput,
		optFns ...func(*sns.Options),
	) (*sns.PublishOutput, error)
}

// SQSClient defines the AWS SQS methods used by the Publisher. This is used for testing purposes.
type SQSClient interface {
	DeleteMessage(
		context.Context,
		*sqs.DeleteMessageInput,
		...func(*sqs.Options),
	) (*sqs.DeleteMessageOutput, error)
	ReceiveMessage(
		context.Context,
		*sqs.ReceiveMessageInput,
		...func(*sqs.Options),
	) (*sqs.ReceiveMessageOutput, error)
	SendMessage(
		context.Context,
		*sqs.SendMessageInput,
		...func(*sqs.Options),
	) (*sqs.SendMessageOutput, error)
	GetQueueUrl(
		context.Context,
		*sqs.GetQueueUrlInput,
		...func(*sqs.Options),
	) (*sqs.GetQueueUrlOutput, error)
}
