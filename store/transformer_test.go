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

	outmsg, err := store.TransformerFunc[*messenger.GenericMessage](func(ctx context.Context, m *messenger.GenericMessage) (messenger.Message, error) {
		require.Equal(t, expectedCtx, ctx)
		require.Equal(t, expectedMsg, m)

		return m, nil
	}).Transform(expectedCtx, expectedMsg)

	require.NoError(t, err)
	require.Equal(t, expectedMsg, outmsg)
}

func TestDefaultTransformer(t *testing.T) {
	aMsg, err := messenger.NewMessage([]byte(`some`))
	require.NoError(t, err)

	tf := store.DefaultTransformer[any]()
	for tname, tc := range map[string]struct {
		msg             any
		expectedPayload []byte
	}{
		"message": {
			msg:             aMsg,
			expectedPayload: aMsg.Payload(),
		},
		"string": {
			msg:             "hello string",
			expectedPayload: []byte("hello string"),
		},
		"bytes": {
			msg:             []byte("hello bytes"),
			expectedPayload: []byte("hello bytes"),
		},
		"struct": {
			msg: struct {
				Ping string `json:"ping,omitempty"`
			}{
				Ping: "pong",
			},
			expectedPayload: []byte(`{"ping":"pong"}`),
		},
	} {
		t.Run(tname, func(t *testing.T) {
			msg, err := tf.Transform(context.TODO(), tc.msg)
			require.NoError(t, err)

			require.Equal(t, tc.expectedPayload, msg.Payload(), string(msg.Payload()))
		})
	}
}
