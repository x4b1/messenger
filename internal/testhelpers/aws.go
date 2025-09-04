package testhelpers

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

// CreateLocalStackContainer starts a local stack container with SNS and SQS services, and returns its instance.
func CreateLocalStackContainer(
	ctx context.Context,
) (aws.Config, *localstack.LocalStackContainer, error) {
	lsContainer, err := localstack.Run(ctx,
		"localstack/localstack:4.7.0",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{"SERVICES": "sns,sqs"},
			},
		}),
	)
	if err != nil {
		return aws.Config{}, nil, err
	}

	endpoint, err := lsContainer.PortEndpoint(ctx, "4566/tcp", "")
	if err != nil {
		panic(err)
	}
	if !strings.Contains(endpoint, "http://") {
		endpoint = "http://" + endpoint
	}

	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("eu-west-1"),
		config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					CanExpire:       false,
					AccessKeyID:     "test",
					SecretAccessKey: "test",
					AccountID:       "000000000000",
				}, nil
			}),
		),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		return aws.Config{}, nil, err
	}

	return awsCfg, lsContainer, nil
}
