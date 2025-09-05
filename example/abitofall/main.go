//nolint:mnd // this is a file for testing
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_sns "github.com/aws/aws-sdk-go-v2/service/sns"
	aws_sqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/x4b1/messenger"
	awsbroker "github.com/x4b1/messenger/broker/aws"
	"github.com/x4b1/messenger/inspect"
	"github.com/x4b1/messenger/internal/testhelpers"
	"github.com/x4b1/messenger/store/postgres"
	"github.com/x4b1/messenger/store/postgres/pgx"
)

const (
	testTopic        = "test-topic"
	testSubscription = "test-subscription"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pgStore, closeStore, err := setupStore(ctx)
	if err != nil {
		return err
	}
	defer closeStore()

	awsBroker, err := NewAWS(ctx)
	if err != nil {
		return err
	}
	defer awsBroker.Close(ctx)

	awsSubscriber := awsbroker.NewSQSSubscriber(awsBroker.sqs)

	awsSubscriber.Register(messenger.NewSubscription(
		awsBroker.queueARN,
		func(_ context.Context, msg messenger.Message) error {
			bAtt, _ := json.Marshal(msg.Metadata())

			color.Yellow(
				"[AWS] Message received\n  ID: %s\n  Metadata: %s\n  Payload: %s\n",
				msg.ID(),
				string(bAtt),
				string(msg.Payload()),
			)

			return nil
		},
	))

	snsPublisher := awsbroker.NewSNSPublisher(awsBroker.sns, awsBroker.topicARN)

	msn := messenger.NewMessenger(pgStore, snsPublisher)

	http.Handle("/", inspect.NewInspector(pgStore))

	go func() {
		if err := publishEvents(ctx, pgStore); err != nil {
			log.Fatal(fmt.Errorf("publishing events: %w", err))
		}
	}()

	go func() {
		if err := msn.Start(ctx); err != nil {
			log.Fatal(fmt.Errorf("msn start: %w", err))
		}
	}()

	go func() {
		log.Println("listening at: http://localhost:8080")
		//nolint:gosec // leaving simple for the example.
		if err := http.ListenAndServe(net.JoinHostPort("", "8080"), nil); err != nil {
			log.Fatal(fmt.Errorf("serving http: %w", err))
		}
	}()

	go func() {
		if err := awsSubscriber.Listen(ctx); err != nil {
			log.Fatal(fmt.Errorf("listening events: %w", err))
		}
	}()

	<-ctx.Done()
	log.Println("got interruption signal")

	return nil
}

func publishEvents(ctx context.Context, s *pgx.Store[any]) error {
	if err := s.Store(ctx, nil, generateMessages()...); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()

			return nil
		case <-ticker.C:
			if err := s.Store(ctx, nil, generateMessages()...); err != nil {
				return err
			}
		}
	}
}

const messageBatch = 50

func generateMessages() []any {
	traceID := uuid.NewString()
	msgs := []any{}

	for i := range messageBatch {
		msg, _ := messenger.NewMessage(fmt.Appendf(nil, `{"message_number": "%d"}`, i+1))
		msg.SetMetadata("trace_id", traceID)
		msg.SetMetadata("num", strconv.Itoa(i+1))
		msgs = append(msgs, msg)
	}

	return msgs
}

func setupStore(ctx context.Context) (*pgx.Store[any], func(), error) {
	pgContainer, err := testhelpers.CreatePostgresContainer(ctx)
	if err != nil {
		return nil, nil, err
	}

	closeContainer := func() {
		_ = pgContainer.Terminate(ctx)
	}

	connPool, err := pgxpool.New(ctx, pgContainer.ConnectionString)
	if err != nil {
		closeContainer()
		return nil, nil, err
	}

	s, err := pgx.WithInstance[any](
		ctx, connPool,
		postgres.WithTableName("event"),
		postgres.WithJSONPayload(),
	)
	if err != nil {
		closeContainer()
		return nil, nil, err
	}

	return s, closeContainer, nil
}

func NewAWS(ctx context.Context) (*AWS, error) {
	aws := &AWS{}

	if err := aws.init(ctx); err != nil {
		return nil, err
	}

	return aws, nil
}

type AWS struct {
	container *localstack.LocalStackContainer

	config aws.Config

	sns *aws_sns.Client
	sqs *aws_sqs.Client

	topicARN string
	queueARN string
}

func (a *AWS) init(ctx context.Context) (err error) {
	a.config, a.container, err = testhelpers.CreateLocalStackContainer(ctx)
	if err != nil {
		return err
	}

	a.sns = aws_sns.NewFromConfig(a.config)
	a.sqs = aws_sqs.NewFromConfig(a.config)

	a.topicARN, err = a.createTopic(ctx, testTopic)
	if err != nil {
		return err
	}

	a.queueARN, err = a.createQueue(ctx, testSubscription)
	if err != nil {
		return err
	}

	return a.bindQueueTopic(ctx, a.queueARN, a.topicARN)
}

func (a *AWS) createTopic(ctx context.Context, topicName string) (string, error) {
	topic, err := a.sns.CreateTopic(ctx, &aws_sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(topic.TopicArn), err
}

func (a *AWS) createQueue(ctx context.Context, queueName string) (string, error) {
	queue, err := a.sqs.CreateQueue(ctx, &aws_sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	queueAtt, err := a.sqs.GetQueueAttributes(ctx, &aws_sqs.GetQueueAttributesInput{
		QueueUrl:       queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", err
	}

	return queueAtt.Attributes[string(types.QueueAttributeNameQueueArn)], nil
}

func (a *AWS) bindQueueTopic(ctx context.Context, queueARN, topicARN string) error {
	_, err := a.sns.Subscribe(ctx, &aws_sns.SubscribeInput{
		Protocol:   aws.String("sqs"),
		TopicArn:   aws.String(topicARN),
		Endpoint:   aws.String(queueARN),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	if err != nil {
		return err
	}

	return nil
}

func (a *AWS) Close(ctx context.Context) {
	_ = a.container.Terminate(ctx)
}
