package broker

import (
	"context"
	"errors"

	"github.com/x4b1/messenger"
)

var (
	ErrEmptyTargetMetadataKey         = errors.New("empty target metadata key")
	ErrEmptyTargetMetadataValue       = errors.New("empty target metadata value")
	ErrMessageDoesNotMatchWithBrokers = errors.New("message does not match with any broker")
)

var _ messenger.Broker = &MultiBroker{}

// MultiBroker returns and empty MultiBroker,
// if the targetKey is empty it returns an error.
func NewMultiBroker(targetKey string) (*MultiBroker, error) {
	if len(targetKey) == 0 {
		return nil, ErrEmptyTargetMetadataKey
	}

	return &MultiBroker{mdKey: targetKey}, nil
}

// MultiBroker contains multiple brokers and a metadata key to allow route messages
// depending on the metadata value of the message.
type MultiBroker struct {
	mdKey string

	brokers map[string]messenger.Broker
}

// AddBroker registers the broker with the value filter.
func (mb *MultiBroker) AddBroker(value string, b messenger.Broker) {
	mb.brokers[value] = b
}

// Publish routes the message to a broker depending if it matches the metadata key and metadata value.
// It it does not match it returns an error.
func (mb MultiBroker) Publish(ctx context.Context, msg messenger.Message) error {
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
