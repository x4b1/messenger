package pubsub_test

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pubsubpublish "github.com/xabi93/messenger/publish/pubsub"
	"github.com/xabi93/messenger/store"
)

const topic = "test-topic"

func initPubsub(ctx context.Context, t *testing.T) (*pstest.Server, *pubsub.Topic) {
	t.Helper()

	srv := pstest.NewServer()
	t.Cleanup(func() { srv.Close() })

	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client, err := pubsub.NewClient(ctx, "project", option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	topic, err := client.CreateTopic(ctx, topic)
	topic.EnableMessageOrdering = true
	require.NoError(t, err)

	return srv, topic
}

func TestPublishWithNoOrderingKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()

	srv, topic := initPubsub(ctx, t)

	m := &store.Message{
		ID:       uuid.Must(uuid.NewRandom()).String(),
		Metadata: map[string]string{"aggregate_id": "29a7556a-ae85-4c1d-8f04-d57ed3122586"},
		Payload:  []byte("some message"),
	}

	require.NoError(pubsubpublish.New(topic).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(m.Metadata, msgs[0].Attributes)
	require.Empty(msgs[0].OrderingKey)
}

func TestPublishWithDefaultOrderingKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()

	srv, topic := initPubsub(ctx, t)

	m := &store.Message{
		ID:       uuid.Must(uuid.NewRandom()).String(),
		Metadata: map[string]string{"aggregate_id": "29a7556a-ae85-4c1d-8f04-d57ed3122586"},
		Payload:  []byte("some message"),
	}

	ordKey := "default-ord-key"
	require.NoError(pubsubpublish.New(topic, pubsubpublish.WithDefaultOrderingKey(ordKey)).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(m.Metadata, msgs[0].Attributes)
	require.Equal(ordKey, msgs[0].OrderingKey)
}

func TestPublishWithMessageMetadataOrderingKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()

	srv, topic := initPubsub(ctx, t)

	metaKey := "meta-key"
	orderingValue := "value-1"

	m := &store.Message{
		ID:       uuid.Must(uuid.NewRandom()).String(),
		Metadata: map[string]string{metaKey: orderingValue},
		Payload:  []byte("some message"),
	}

	require.NoError(pubsubpublish.New(topic, pubsubpublish.WithMetaOrderingKey(metaKey)).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(map[string]string(m.Metadata), msgs[0].Attributes)
	require.Equal(orderingValue, msgs[0].OrderingKey)
}
