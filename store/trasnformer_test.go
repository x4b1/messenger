package store_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/store"
)

func TestTransformFunc(t *testing.T) {
	expectedCtx := context.TODO()
	expectedMsg, err := messenger.NewMessage([]byte(`some`))
	require.NoError(t, err)

	require.NoError(t,
		store.TransformerFunc(func(ctx context.Context, m messenger.Message) error {
			require.Equal(t, expectedCtx, ctx)
			require.Equal(t, expectedMsg, m)

			return nil
		}).
			Transform(expectedCtx, expectedMsg),
	)
}
