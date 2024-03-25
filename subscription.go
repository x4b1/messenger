package messenger

import "context"

type SubscriptionHandler func(ctx context.Context, msg []byte, att map[string]string) error

func NewSubscription(name string, h SubscriptionHandler) Subscription {
	return &subscription{name, h}
}

type subscription struct {
	name string
	h    SubscriptionHandler
}

func (s *subscription) Name() string {
	return s.name
}

func (s *subscription) Handle(ctx context.Context, msg []byte, att map[string]string) error {
	return s.h(ctx, msg, att)
}

type Subscription interface {
	Name() string
	Handle(ctx context.Context, msg []byte, att map[string]string) error
}
