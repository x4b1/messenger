package testhelpers

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

// LocalStackContainer contains a docker instance of local stack and the aws configuration where is exposed.
type LocalStackContainer struct {
	Config aws.Config

	*localstack.LocalStackContainer
}

// CreateLocalStackContainer starts a local stack container with SNS and SQS services, and returns its instance.
func CreateLocalStackContainer(ctx context.Context) (*LocalStackContainer, error) {
	lsContainer, err := localstack.Run(ctx,
		"localstack/localstack:4.7.0",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{"SERVICES": "sns,sqs"},
			},
		}),
	)
	if err != nil {
		return nil, err
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
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				CanExpire:       false,
				AccessKeyID:     "test",
				SecretAccessKey: "test",
			}, nil
		})),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		return nil, err
	}

	return &LocalStackContainer{
		awsCfg,
		lsContainer,
	}, nil
}
