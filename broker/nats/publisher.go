package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/broker"
)

var _ broker.Broker = &Publisher{}

// NewPublisher is a constructor for nats.Publisher
func NewPublisher(conn Conn, subjectKey string) *Publisher {
	return &Publisher{
		nc:         conn,
		subjectKey: subjectKey,
	}
}

// Publisher implements publish functionality to nats service.
type Publisher struct {
	nc         Conn
	subjectKey string
}

// Publish implements broker.Broker.
func (p *Publisher) Publish(_ context.Context, msg messenger.Message) error {
	nMsg := nats.NewMsg(msg.Metadata().Get(p.subjectKey))

	for k, md := range msg.Metadata() {
		nMsg.Header.Set(k, md)
	}
	nMsg.Header.Set(nats.MsgIdHdr, msg.ID())

	nMsg.Data = msg.Payload()

	if err := p.nc.PublishMsg(nMsg); err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}

	return nil
}
