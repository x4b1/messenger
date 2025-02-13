// Package sqs exposes AWS SQS broker implementation
package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

//go:generate moq -pkg sqs_test -stub -out publisher_mock_test.go . Client

// Client defines the AWS SQS methods used by the Publisher. This is used for testing purposes.
type Client interface {
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueUrl(context.Context, *sqs.GetQueueUrlInput, ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}
