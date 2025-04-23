package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

type subscription struct {
	subj string
	sub  messenger.Subscription
}

// NewSubscriber is a constructor for nats.Subscriber
func NewSubscriber(conn Conn) *Subscriber {
	return &Subscriber{
		nc:   conn,
		subs: make(map[string]*subscription),
	}
}

// Subscriber implements subscribe functionality to nats service.
type Subscriber struct {
	nc Conn

	errHandler messenger.ErrorHandler

	subs map[string]*subscription
}

// Subscribe adds a subscription to a Nats subject, if subscription name is aready registered
// it returns an error.
func (s *Subscriber) Subscribe(subj string, sub messenger.Subscription) error {
	if _, ok := s.subs[sub.Name()]; ok {
		return fmt.Errorf("%s: %w", sub.Name(), broker.DuplicatedSubscription)
	}

	s.subs[sub.Name()] = &subscription{
		subj: subj,
		sub:  sub,
	}

	return nil
}

// Listen starts listening for events.
func (s *Subscriber) Listen(ctx context.Context) error {
	for _, sub := range s.subs {
		_, err := s.nc.Subscribe(sub.subj, func(msg *nats.Msg) {
			inMsg := &messenger.GenericMessage{
				MsgPayload:  msg.Data,
				MsgID:       msg.Header.Get(nats.MsgIdHdr),
				MsgMetadata: make(messenger.Metadata, len(msg.Header)),
			}
			for k := range msg.Header {
				inMsg.MsgMetadata[k] = msg.Header.Get(k)
			}

			if err := sub.sub.Handle(ctx, inMsg); err != nil {
				msg.Nak()
				s.errHandler.Error(ctx, err)
				return
			}

			if err := msg.Ack(); err != nil {
				msg.Ack()
			}
		})
		if err != nil {
			return fmt.Errorf("%s(%s): %w", sub.sub.Name(), sub.subj, err)
		}
	}

	return nil
}
