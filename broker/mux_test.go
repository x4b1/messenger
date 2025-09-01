package broker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

func TestMux(t *testing.T) {
	t.Parallel()

	t.Run("missing metadata key", func(t *testing.T) {
		t.Parallel()
		_, err := broker.NewMux("")
		require.ErrorIs(t, err, broker.ErrEmptyTarMetadataKey)
	})

	mdKey := "some-key"
	mdVal := "some value"
	errUnexpected := errors.New("unexpected err")

	publishMsg := &messenger.GenericMessage{MsgMetadata: map[string]string{mdKey: mdVal}}

	for _, tc := range []struct {
		name       string
		msg        messenger.Message
		trgtBroker *broker.BrokerMock
		err        error
		asserts    func(t *testing.T, b *broker.BrokerMock)
	}{
		{
			name:       "message metadata key does not exist",
			msg:        &messenger.GenericMessage{},
			trgtBroker: &broker.BrokerMock{},
			err:        broker.ErrMessageDoesNotMatchWithBrokers,
			asserts: func(t *testing.T, b *broker.BrokerMock) {
				require.Empty(t, b.PublishCalls())
			},
		},
		{
			name:       "message metadata value does not match",
			msg:        &messenger.GenericMessage{MsgMetadata: map[string]string{mdKey: "other-value"}},
			trgtBroker: &broker.BrokerMock{},
			err:        broker.ErrMessageDoesNotMatchWithBrokers,
			asserts: func(t *testing.T, b *broker.BrokerMock) {
				require.Empty(t, b.PublishCalls())
			},
		},
		{
			name: "metadata matches, broker fails",
			msg:  publishMsg,
			trgtBroker: &broker.BrokerMock{
				PublishFunc: func(context.Context, messenger.Message) error {
					return errUnexpected
				},
			},
			err: errUnexpected,
			asserts: func(t *testing.T, b *broker.BrokerMock) {
				require.Len(t, b.PublishCalls(), 1)
			},
		},
		{
			name:       "metadata matches, broker success",
			msg:        publishMsg,
			trgtBroker: &broker.BrokerMock{},
			asserts: func(t *testing.T, b *broker.BrokerMock) {
				require.Len(t, b.PublishCalls(), 1)
				require.Equal(t, publishMsg, b.PublishCalls()[0].Msg)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mb, err := broker.NewMux(mdKey)
			require.NoError(t, err)

			mb.AddBroker(mdVal, tc.trgtBroker)

			require.ErrorIs(t, mb.Publish(context.Background(), tc.msg), tc.err)

			if tc.asserts != nil {
				tc.asserts(t, tc.trgtBroker)
			}
		})
	}
}
