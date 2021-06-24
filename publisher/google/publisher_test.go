package google_test

import (
	"context"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publisher/google"
)

const topic = "test-topic"

func initPubsub(ctx context.Context, t *testing.T) (*pstest.Server, *pubsub.Topic) {
	t.Helper()

	srv := pstest.NewServer()
	t.Cleanup(func() { srv.Close() })

	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
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

	m, err := messenger.NewMessage([]byte("some message"))
	m.Metadata.Set("aggregate_id", "29a7556a-ae85-4c1d-8f04-d57ed3122586")
	require.NoError(err)

	require.NoError(google.New(topic).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(map[string]string(m.Metadata), msgs[0].Attributes)
	require.Empty(msgs[0].OrderingKey)
}

func TestPublishWithDefaultOrderingKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()

	srv, topic := initPubsub(ctx, t)

	m, err := messenger.NewMessage([]byte("some message"))
	m.Metadata.Set("aggregate_id", "29a7556a-ae85-4c1d-8f04-d57ed3122586")
	require.NoError(err)

	ordKey := "default-ord-key"
	require.NoError(google.New(topic, google.WithDefaultOrderingKey(ordKey)).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(map[string]string(m.Metadata), msgs[0].Attributes)
	require.Equal(ordKey, msgs[0].OrderingKey)
}

func TestPublishWithMessageMetadataOrderingKey(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()

	srv, topic := initPubsub(ctx, t)

	metaKey := "meta-key"
	orderingValue := "value-1"
	m, err := messenger.NewMessage([]byte("some message"))
	m.Metadata.Set(metaKey, orderingValue)
	require.NoError(err)

	require.NoError(google.New(topic, google.WithMetaOrderingKey(metaKey)).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(msgs, 1)
	require.Equal(m.Payload, msgs[0].Data)
	require.Equal(map[string]string(m.Metadata), msgs[0].Attributes)
	require.Equal(orderingValue, msgs[0].OrderingKey)
}
