package messenger_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
)

func TestGenericMessage(t *testing.T) {
	somePayload := "hello world"
	mdKey := "key"
	mdValue := "value"

	t.Run("invalid payload", func(t *testing.T) {
		_, err := messenger.NewMessage(nil)
		require.ErrorIs(t, err, messenger.ErrEmptyMessagePayload)
	})

	t.Run("success", func(t *testing.T) {
		msg, err := messenger.NewMessage([]byte(somePayload))
		require.NoError(t, err)
		msg.SetMetadata(mdKey, mdValue)

		require.NotEqual(t, uuid.Nil.String(), msg.ID())
		require.NotEmpty(t, msg.ID())
		require.Equal(t, msg.MsgID, msg.ID())

		require.Equal(t, map[string]string{mdKey: mdValue}, msg.Metadata())
		require.Equal(t, msg.MsgMetadata, msg.Metadata())

		require.Equal(t, msg.MsgPayload, []byte(somePayload))
		require.Equal(t, msg.MsgPayload, msg.Payload())

		require.Equal(t, msg.MsgPublished, msg.Published())
		require.Equal(t, msg.MsgAt, msg.At())
	})
}
