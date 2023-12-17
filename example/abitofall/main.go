package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
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

//nolint:gocognit //main function
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
		sqsClient := aws_sqs.NewFromConfig(awsContainer.Config)
		for {
			msgs, err := sqsClient.ReceiveMessage(ctx, &aws_sqs.ReceiveMessageInput{
				QueueUrl:              pubsub.queue.QueueUrl,
				MessageAttributeNames: []string{"trace_id"},
				WaitTimeSeconds:       20, //nolint:gomnd // simplicity
			})
			if err != nil {
				log.Fatal(fmt.Errorf("receiving sqs messages: %w", err))
			}
			for _, msg := range msgs.Messages {
				att := make(map[string]string, len(msg.MessageAttributes))
				for key, v := range msg.MessageAttributes {
					att[key] = aws.ToString(v.StringValue)
				}

				bAtt, _ := json.Marshal(att)

				//nolint: forbidigo // need to print command line to show result
				fmt.Printf("\t - attributes: %s\n", string(bAtt))
				//nolint: forbidigo // need to print command line to show result
				fmt.Printf("\t - body: %s\n", aws.ToString(msg.Body))

				if _, err := sqsClient.DeleteMessage(ctx, &aws_sqs.DeleteMessageInput{
					QueueUrl:      pubsub.queue.QueueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				}); err != nil {
					log.Fatal(fmt.Errorf("deleting message: %w", err))
				}
			}
		}
	}()

	<-ctx.Done()
	log.Println("got interruption signal")

	return nil
}

func publishEvents(ctx context.Context, s *pgx.Store) error {
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

func generateMessages() []messenger.Message {
	traceID := uuid.NewString()
	msgs := make([]messenger.Message, messageBatch)

	for i := range msgs {
		msg, _ := messenger.NewMessage([]byte(`{"hello": "word"}`))
		msg.SetMetadata("trace_id", traceID)
		msgs[i] = msg
	}

	return msgs
}

func setupStore(ctx context.Context) (*pgx.Store, func(), error) {
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

	s, err := pgx.WithInstance(ctx, connPool, postgres.WithTableName("event"), postgres.WithJSONPayload())
	if err != nil {
		closeContainer()
		return nil, nil, err
	}

	return s, closeContainer, nil
}

type pubsub struct {
	publisher *sns.Publisher
	topic     *aws_sns.CreateTopicOutput
	queue     *aws_sqs.CreateQueueOutput
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

	return &pubsub{pub, topic, queue}, nil
}
