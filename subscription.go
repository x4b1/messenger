package messenger

import "context"

// SubscriptionHandler defines a function to process a message, if something fails returns an error.
type SubscriptionHandler func(ctx context.Context, msg Message) error

// NewSubscription returns a Subscription handler.
func NewSubscription(name string, h SubscriptionHandler) Subscription {
	return &subscription{name, h}
}

type subscription struct {
	name string
	h    SubscriptionHandler
}

// Name returns subscription name.
func (s *subscription) Name() string {
	return s.name
}

// Handle process message from broker.
func (s *subscription) Handle(ctx context.Context, msg Message) error {
	return s.h(ctx, msg)
}

// Subscription defines the basic methods for a subscription broker.
type Subscription interface {
	Name() string
	Handle(ctx context.Context, msg Message) error
}
