package testhelpers

import (
	"context"
	"net"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/docker/go-connections/nat"
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
		"localstack/localstack:3.7.2",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{"SERVICES": "sns,sqs"},
			},
		}),
	)
	if err != nil {
		return nil, err
	}

	host, err := lsContainer.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := lsContainer.MappedPort(ctx, nat.Port("4566/tcp"))
	if err != nil {
		return nil, err
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-west-1"))
	if err != nil {
		return nil, err
	}

	awsCfg.BaseEndpoint = aws.String("http://" + net.JoinHostPort(host, port.Port()))

	return &LocalStackContainer{
		awsCfg,
		lsContainer,
	}, nil
}
