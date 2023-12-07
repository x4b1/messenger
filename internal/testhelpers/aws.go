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

type LocalStackContainer struct {
	Config aws.Config

	*localstack.LocalStackContainer
}

func CreateLocalStackContainer(ctx context.Context) (*LocalStackContainer, error) {
	lsContainer, err := localstack.RunContainer(ctx,
		testcontainers.WithImage("localstack/localstack:3.0.2"),
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

	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           "http://" + net.JoinHostPort(host, port.Port()),
				SigningRegion: region,
			}, nil
		})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-west-1"),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, err
	}

	return &LocalStackContainer{
		awsCfg,
		lsContainer,
	}, nil
}
