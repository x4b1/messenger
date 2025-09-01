package pubsub_test

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
	pubsubpublish "github.com/x4b1/messenger/broker/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	topicID = "test-topic"
)

func initPubsub(ctx context.Context, t *testing.T) (*pubsub.Publisher, *pstest.Server) {
	t.Helper()

	srv := pstest.NewServer()
	//nolint:errcheck // test file
	t.Cleanup(func() { srv.Close() })

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	//nolint:errcheck // test file
	t.Cleanup(func() { conn.Close() })

	client, err := pubsub.NewClient(ctx, "project-id", option.WithGRPCConn(conn))
	require.NoError(t, err)
	//nolint:errcheck // test file
	t.Cleanup(func() { client.Close() })

	topic, err := srv.GServer.CreateTopic(ctx, &pubsubpb.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", client.Project(), topicID),
	})
	require.NoError(t, err)

	return client.Publisher(topic.GetName()), srv
}

func TestPublishWithNoOrderingKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	publisher, srv := initPubsub(ctx, t)

	m := &messenger.GenericMessage{
		MsgID:       uuid.NewString(),
		MsgMetadata: map[string]string{"aggregate_id": "29a7556a-ae85-4c1d-8f04-d57ed3122586"},
		MsgPayload:  []byte("some message"),
	}

	require.NoError(t, pubsubpublish.New(publisher).Publish(ctx, m))

	msgs := srv.Messages()
	require.Len(t, msgs, 1)
	require.Equal(t, m.Payload(), msgs[0].Data)
	require.EqualValues(t, map[string]string{
		"aggregate_id":      "29a7556a-ae85-4c1d-8f04-d57ed3122586",
		broker.MessageIDKey: m.MsgID,
	}, msgs[0].Attributes)
	require.Empty(t, msgs[0].OrderingKey)
}

func TestPublishWithDefaultOrderingKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	publisher, srv := initPubsub(ctx, t)

	m := &messenger.GenericMessage{
		MsgID:       uuid.NewString(),
		MsgMetadata: map[string]string{"aggregate_id": "29a7556a-ae85-4c1d-8f04-d57ed3122586"},
		MsgPayload:  []byte("some message"),
	}

	ordKey := "default-ord-key"
	require.NoError(
		t,
		pubsubpublish.New(publisher, pubsubpublish.WithDefaultOrderingKey(ordKey)).Publish(ctx, m),
	)

	msgs := srv.Messages()
	require.Len(t, msgs, 1)
	require.Equal(t, m.Payload(), msgs[0].Data)
	require.EqualValues(t, map[string]string{
		"aggregate_id":      "29a7556a-ae85-4c1d-8f04-d57ed3122586",
		broker.MessageIDKey: m.MsgID,
	}, msgs[0].Attributes)
	require.Equal(t, ordKey, msgs[0].OrderingKey)
}

func TestPublishWithMessageMetadataOrderingKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	publisher, srv := initPubsub(ctx, t)

	metaKey := "meta-key"
	orderingValue := "value-1"

	m := &messenger.GenericMessage{
		MsgID:       uuid.NewString(),
		MsgMetadata: map[string]string{metaKey: orderingValue},
		MsgPayload:  []byte("some message"),
	}

	require.NoError(
		t,
		pubsubpublish.New(publisher, pubsubpublish.WithMetaOrderingKey(metaKey)).Publish(ctx, m),
	)

	msgs := srv.Messages()
	require.Len(t, msgs, 1)
	require.Equal(t, m.Payload(), msgs[0].Data)
	require.EqualValues(t, map[string]string{
		metaKey:             orderingValue,
		broker.MessageIDKey: m.MsgID,
	}, msgs[0].Attributes)
	require.Equal(t, orderingValue, msgs[0].OrderingKey)
}

func TestPublishWithCustomMessageID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	publisher, srv := initPubsub(ctx, t)

	m, err := messenger.NewMessage([]byte("some message"))
	require.NoError(t, err)
	customKey := "custom_key"
	require.NoError(
		t,
		pubsubpublish.New(publisher, pubsubpublish.WithMessageIDKey(customKey)).Publish(ctx, m),
	)

	msgs := srv.Messages()
	require.Len(t, msgs, 1)
	require.Equal(t, m.Payload(), msgs[0].Data)
	require.EqualValues(t, map[string]string{
		customKey: m.MsgID,
	}, msgs[0].Attributes)
}
