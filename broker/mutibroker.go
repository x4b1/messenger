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

var _ Broker = &MultiBroker{}

// MultiBrokerOption defines the optional parameters for MultiBroker.
type MultiBrokerOption func(*MultiBroker)

// MultiBrokerWithBroker adds a broker to the MultiBroker.
func MultiBrokerWithBroker(trgtValue string, b Broker) MultiBrokerOption {
	return func(mb *MultiBroker) {
		mb.AddBroker(trgtValue, b)
	}
}

// MultiBroker returns and empty MultiBroker,
// if the targetKey is empty it returns an error.
func NewMultiBroker(targetKey string, opts ...MultiBrokerOption) (*MultiBroker, error) {
	if len(targetKey) == 0 {
		return nil, ErrEmptyTargetMetadataKey
	}

	mb := &MultiBroker{
		mdKey:   targetKey,
		brokers: map[string]Broker{},
	}
	for _, opt := range opts {
		opt(mb)
	}

	return mb, nil
}

// MultiBroker contains multiple brokers and a metadata key to allow route messages
// depending on the metadata value of the message.
type MultiBroker struct {
	mdKey string

	brokers map[string]Broker
}

// AddBroker registers the broker with the value filter.
func (mb *MultiBroker) AddBroker(value string, b Broker) {
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
