package broker

import (
	"context"
	"errors"

	"github.com/x4b1/messenger"
)

var (
	ErrEmptyTargetMetadataKey         = errors.New("empty target metadata key")
	ErrMessageDoesNotMatchWithBrokers = errors.New("message does not match with any broker")
)

var _ Broker = &Mux{}

// Mux returns and empty Mux,
// if the targetKey is empty it returns an error.
func NewMux(targetKey string) (*Mux, error) {
	if len(targetKey) == 0 {
		return nil, ErrEmptyTargetMetadataKey
	}

	mb := &Mux{
		mdKey:   targetKey,
		brokers: map[string]Broker{},
	}

	return mb, nil
}

// Mux contains multiple brokers and a metadata key to allow route messages
// depending on the metadata value of the message.
type Mux struct {
	mdKey string

	brokers map[string]Broker
}

// AddBroker registers the broker with the value filter.
func (mb *Mux) AddBroker(value string, b Broker) {
	mb.brokers[value] = b
}

// Publish routes the message to a broker depending if it matches the metadata key and metadata value.
// It it does not match it returns an error.
func (mb Mux) Publish(ctx context.Context, msg messenger.Message) error {
	mdVal, ok := msg.GetMetadata()[mb.mdKey]
	if !ok {
		return ErrMessageDoesNotMatchWithBrokers
	}

	b, ok := mb.brokers[mdVal]
	if !ok {
		return ErrMessageDoesNotMatchWithBrokers
	}

	return b.Publish(ctx, msg)
}
