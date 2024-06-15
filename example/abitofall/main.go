//nolint:mnd // this is a file for testing
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker/sns"
	"github.com/x4b1/messenger/broker/sqs"
	"github.com/x4b1/messenger/inspect"
	"github.com/x4b1/messenger/internal/testhelpers"
	"github.com/x4b1/messenger/store/postgres"
	"github.com/x4b1/messenger/store/postgres/pgx"
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

	awsContainer, err := testhelpers.CreateLocalStackContainer(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = awsContainer.Terminate(ctx) }()

	pubsub, err := initPubSub(ctx, awsContainer.Config)
	if err != nil {
		return err
	}

	pubsub.subscriber.Register(
		messenger.NewSubscription("test-queue", func(ctx context.Context, msg messenger.Message) error {
			bAtt, _ := json.Marshal(msg.Metadata())

			//nolint: forbidigo // need to print command line to show result
			fmt.Printf("\t - id: %s\n", msg.ID())
			//nolint: forbidigo // need to print command line to show result
			fmt.Printf("\t - attributes: %s\n", string(bAtt))
			//nolint: forbidigo // need to print command line to show result
			fmt.Printf("\t - body: %s\n", msg.Payload())

			i, _ := strconv.Atoi(msg.Metadata()["num"])
			if i%2 == 0 {
				return errors.New("some errr")
			}
			return nil
		}))

	http.Handle("/", inspect.NewInspector(pgStore))

	msn := messenger.NewMessenger(pgStore, pubsub.publisher)

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
		if err := pubsub.subscriber.Listen(ctx); err != nil {
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

const messageBatch = 3

func generateMessages() []any {
	traceID := uuid.NewString()
	msgs := []any{}

	for i := range messageBatch {
		msg, _ := messenger.NewMessage([]byte(`{"hello": "word"}`))
		msg.SetMetadata("trace_id", traceID)
		msg.SetMetadata("num", strconv.Itoa(i+1))
		msgs = append(msgs, msg)
	}

	msgs = append(msgs, struct {
		Message string `json:"message,omitempty"`
		TraceID string `json:"trace_id,omitempty"`
		Num     int    `json:"num,omitempty"`
	}{
		Message: "this is a struct to be published",
		TraceID: traceID,
		Num:     6,
	})

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

type pubsub struct {
	publisher *sns.Publisher

	subscriber *sqs.Subscriber
	topic      *aws_sns.CreateTopicOutput
	queue      *aws_sqs.CreateQueueOutput
}

func initPubSub(ctx context.Context, awsCfg aws.Config) (*pubsub, error) {
	snsClient := aws_sns.NewFromConfig(awsCfg)

	topic, err := snsClient.CreateTopic(ctx, &aws_sns.CreateTopicInput{
		Name: aws.String("test-topic"),
	})
	if err != nil {
		return nil, err
	}

	sqsClient := aws_sqs.NewFromConfig(awsCfg)

	queue, err := sqsClient.CreateQueue(ctx, &aws_sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})
	if err != nil {
		return nil, err
	}
	queueAtt, err := sqsClient.GetQueueAttributes(ctx, &aws_sqs.GetQueueAttributesInput{
		QueueUrl:       queue.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return nil, err
	}
	_, err = snsClient.Subscribe(ctx, &aws_sns.SubscribeInput{
		Protocol:   aws.String("sqs"),
		TopicArn:   topic.TopicArn,
		Endpoint:   aws.String(queueAtt.Attributes[string(types.QueueAttributeNameQueueArn)]),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	if err != nil {
		return nil, err
	}

	pub, err := sns.New(snsClient, aws.ToString(topic.TopicArn))
	if err != nil {
		return nil, err
	}

	sub := sqs.NewSubscriber(aws_sqs.NewFromConfig(awsCfg))

	return &pubsub{pub, sub, topic, queue}, nil
}
