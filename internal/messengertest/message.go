package messengertest

import (
	"time"

	"github.com/x4b1/messenger"
)

const (
	MsgMDKey   = "key"
	MsgMDValue = "value"
)

var msgAt = time.Now()

func NewMessageBuilder() MessageBuilder {
	return MessageBuilder{}
}

type MessageBuilder struct{}

func (mb MessageBuilder) Build() *messenger.GenericMessage {
	return &messenger.GenericMessage{
		MsgID:        "9e8d605d-7de4-41e2-a99e-18f7ff59b944",
		MsgPayload:   []byte("Hello World!"),
		MsgAt:        msgAt,
		MsgPublished: false,
		MsgMetadata: messenger.Metadata{
			MsgMDKey: MsgMDValue,
		},
	}
}
