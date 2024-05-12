package broker

import (
	"github.com/x4b1/messenger"
)

// MessageIDKey defines the key that will be send the message unique identifier.
const MessageIDKey = "message_id"

//go:generate moq -stub -out x_broker_mock_test.go . Broker

// Broker is the interface that wraps the basic message publishing.
type Broker interface {
	messenger.Publisher
}
