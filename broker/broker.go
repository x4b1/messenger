package broker

import (
	"github.com/x4b1/messenger"
)

//go:generate moq -stub -out x_broker_mock_test.go . Broker

// Broker is the interface that wraps the basic message publishing.
type Broker interface {
	messenger.Publisher
}
