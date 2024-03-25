package local

import (
	"context"

	"github.com/x4b1/messenger"
)

type Store interface {
	PrePublish(ctx context.Context, msgs ...SubMessage) error
	GetBySubscriber(ctx context.Context, name string) SubMessage
}

type SubMessage struct {
	Subscription string

	messenger.Message
}

type PubSub struct {
	subs []messenger.Subscription

	store Store
}

func (s *PubSub) Register(subs ...messenger.Subscription) {
	s.subs = append(s.subs, subs...)
}

// Publish publishes the given message to the pubsub topic.
func (p PubSub) Publish(ctx context.Context, msg messenger.Message) error {
	subMsgs := make([]SubMessage, len(p.subs))
	for i, sub := range p.subs {
		subMsgs[i] = SubMessage{
			Subscription: sub.Name(),
			Message:      msg,
		}
	}

	return p.store.PrePublish(ctx, subMsgs...)
}

// Publish publishes the given message to the pubsub topic.
func (p *PubSub) Listen(ctx context.Context) error {
	subMsgs := make([]SubMessage, len(p.subs))
	for i, sub := range p.subs {
		subMsgs[i] = SubMessage{
			Subscription: sub.Name(),
			Message:      msg,
		}
	}

	return p.store.PrePublish(ctx, subMsgs...)
}
