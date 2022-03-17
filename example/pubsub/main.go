package main

import (
	"context"
	"fmt"
	"log"
	"time"

	gpubsub "cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publish"
	"github.com/xabi93/messenger/publish/pubsub"
	store "github.com/xabi93/messenger/store/pgx"
)

const (
	dbUser = "user"
	dbPass = "pass"

	psTopic        = "topic"
	psSubscription = "subscription"
)

var dbConn *pgxpool.Pool

func main() {
	ctx := context.Background()
	closeDocker, err := initDocker(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer closeDocker()

	srv := pstest.NewServer()
	defer srv.Close()

	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect to gRPC server: %s", err)
	}
	defer conn.Close()

	client, err := gpubsub.NewClient(ctx, "project", option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("Could not connect to gRPC server: %s", err)
	}
	defer conn.Close()

	topic, err := client.CreateTopic(ctx, psTopic)
	if err != nil {
		log.Fatalf("Could not create topic: %s", err)
	}
	topic.EnableMessageOrdering = true

	subs, err := client.CreateSubscription(ctx, psSubscription, gpubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		log.Fatalf("Could not create subscription: %s", err)
	}

	go subs.Receive(ctx, func(ctx context.Context, m *gpubsub.Message) {
		fmt.Printf("message received %v, %s\n", m.Attributes, m.Data)
	})

	s := newStore(ctx)
	go storeMessages(ctx, s)

	pPublisher := pubsub.New(topic)

	if err := publish.NewPublisher("pubsub-publisher", s, pPublisher, 5).Start(ctx, time.NewTicker(1*time.Second)); err != nil {
		log.Fatal(err)
	}
}

func storeMessages(ctx context.Context, s *store.Store) {
	i := 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, _ := messenger.NewMessage([]byte(fmt.Sprintf("message 1 batch %d", i)))
			msg2, _ := messenger.NewMessage([]byte(fmt.Sprintf("message 2 batch %d", i)))

			if err := s.Store(ctx, nil, msg, msg2); err != nil {
				log.Fatalf("Could not publish message: %s", err)
			}
			time.Sleep(5 * time.Second)
			i++
		}
	}
}

func newStore(ctx context.Context) *store.Store {
	s, err := store.WithInstance(ctx, dbConn, store.Config{})
	if err != nil {
		log.Fatalf("Could not start store: %s", err)
	}

	return s
}

func initDocker(ctx context.Context) (func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	postgresContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "10",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%s", dbUser),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPass),
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		return nil, fmt.Errorf("Could not start postgres: %w", err)
	}

	postgresContainer.Expire(60)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		dbConn, err = pgxpool.Connect(ctx, fmt.Sprintf("postgres://%s:%s@localhost:%s/postgres", dbUser, dbPass, postgresContainer.GetPort("5432/tcp")))
		if err != nil {
			return err
		}

		return dbConn.Ping(ctx)
	}); err != nil {
		return nil, err
	}

	return func() {
		pool.Purge(postgresContainer)
	}, nil
}
