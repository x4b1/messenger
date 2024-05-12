package messenger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
)

func TestSubscription(t *testing.T) {
	inCtx := context.TODO()
	inMsg, _ := messenger.NewMessage([]byte("hello"))
	name := "sub-name"
	t.Run("handler success", func(t *testing.T) {
		h := messenger.NewSubscription(name, func(ctx context.Context, msg messenger.Message) error {
			require.Equal(t, inCtx, ctx)
			require.Equal(t, inMsg, msg)
			return nil
		})
		require.Equal(t, name, h.Name())
		err := h.Handle(inCtx, inMsg)

		require.NoError(t, err)
	})
	t.Run("handler fails", func(t *testing.T) {
		herr := errors.New("some err")
		err := messenger.NewSubscription(name, func(ctx context.Context, msg messenger.Message) error {
			require.Equal(t, inCtx, ctx)
			require.Equal(t, inMsg, msg)
			return herr
		}).Handle(inCtx, inMsg)

		require.ErrorIs(t, err, herr)
	})
}
