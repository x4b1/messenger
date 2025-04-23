package nats

import "github.com/nats-io/nats.go"

//go:generate moq -pkg nats_test -stub -out mock_test.go . Conn

// Conn defines the Nats connection methods used. This is used for testing purposes.
type Conn interface {
	PublishMsg(m *nats.Msg) error
	Subscribe(subj string, cb nats.MsgHandler) (*nats.Subscription, error)
}
