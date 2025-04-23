// Package broker provides implementations to publish and receieve messages for different sources.
package broker

import (
	"errors"

	"github.com/x4b1/messenger"
)

// MessageIDKey defines the key that will be send the message unique identifier.
const MessageIDKey = "message_id"

var DuplicatedSubscription = errors.New("duplicated subscription")

//go:generate go tool moq -stub -out x_broker_mock_test.go . Broker

// Broker is the interface that wraps the basic message publishing.
type Broker interface {
	messenger.Publisher
}
