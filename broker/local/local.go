package local

import (
	"context"
	"time"

	"github.com/x4b1/messenger"
	"golang.org/x/sync/errgroup"
)

type Store interface {
	// PublishToSubscriber adds given messages to subscribers queues.
	PublishToSubscriber(ctx context.Context, msgs ...SubMessage) error
	// MessagesBySubscriber returns a list of pending messages to process by subscription.
	MessagesBySubscriber(ctx context.Context, name string, batch int) ([]messenger.Message, error)
	// MessageProcessed marks the message as processed by subscription.
	MessageProcessed(ctx context.Context, msg messenger.Message) error
}

// SubMessage wraps messenger.Message adding subscription name.
type SubMessage struct {
	Subscription string

	messenger.Message
}

// PubSub implements publication and subscription using a store.
type PubSub struct {
	subs messenger.Subscriptions

	interval    time.Duration
	maxMessages int

	store      Store
	group      *errgroup.Group
	errHandler messenger.ErrorHandler
}

// Register adds subscription where will be published messages.
func (ps *PubSub) Register(subs ...messenger.Subscription) error {
	return ps.subs.Add(subs...)
}

// Publish publishes the given message to the pubsub topic.
func (ps *PubSub) Publish(ctx context.Context, msg messenger.Message) error {
	subMsgs := make([]SubMessage, len(ps.subs))
	var i int
	for _, sub := range ps.subs {
		subMsgs[i] = SubMessage{
			Subscription: sub.Name(),
			Message:      msg,
		}
		i++
	}

	return ps.store.PublishToSubscriber(ctx, subMsgs...)
}

// Listen iterates over all subscriptions and every defined interval tries to process messages for each sub.
func (ps *PubSub) Listen(ctx context.Context) error {
	for _, s := range ps.subs {
		ps.group.Go(func() error {
			t := time.NewTicker(ps.interval)
			for {
				select {
				case <-ctx.Done():
					t.Stop()

					return nil
				case <-t.C:
					if err := ps.listenSub(ctx, s); err != nil {
						return err
					}
				}
			}
		})
	}
	return ps.group.Wait()
}

func (ps *PubSub) listenSub(ctx context.Context, sub messenger.Subscription) error {
	msgs, err := ps.store.MessagesBySubscriber(ctx, sub.Name(), ps.maxMessages)
	if err != nil {
		// something went wrong in the store, fails everything.
		return err
	}

	for _, msg := range msgs {
		if err := sub.Handle(ctx, msg); err != nil {
			// We don't want for fail everything this is just the messages cannot be processed by this sub.
			ps.errHandler.Error(ctx, err)
			return nil
		}
		if err := ps.store.MessageProcessed(ctx, msg); err != nil {
			// something went wrong in the store, fails everything.
			return err
		}
	}

	return nil
}
