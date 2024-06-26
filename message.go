package messenger

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrEmptyMessagePayload is the error returned when the message payload is empty.
var ErrEmptyMessagePayload = errors.New("empty message payload")

// NewMessage returns a new Message given a payload.
func NewMessage(payload []byte) (*GenericMessage, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyMessagePayload
	}

	return &GenericMessage{
		MsgID:       uuid.NewString(),
		MsgPayload:  payload,
		MsgMetadata: Metadata{},
		MsgAt:       time.Now(),
	}, nil
}

// A Message represents a message to be sent to message message queue.
type Message interface {
	ID() string
	Metadata() Metadata
	Payload() []byte
	Published() bool
	At() time.Time
}

// GenericMessage represents a message to be sent to message message queue.
// It implements the Message interface.
type GenericMessage struct {
	// Unique identifier for the message.
	MsgID string
	// Contains the message header to be sent by the messenger to the message queue.
	MsgMetadata Metadata
	// Payload is the message payload. Must not be empty
	MsgPayload []byte
	// Message is published to broker or not.
	MsgPublished bool
	// At represent the moment of the message creation.
	MsgAt time.Time
}

// ID returns the unique identifier of the message.
func (m *GenericMessage) ID() string {
	return m.MsgID
}

// SetMetadata sets the given key-value pair to the message metadata.
// If the key already exists, it replaces the value.
func (m *GenericMessage) SetMetadata(key, value string) *GenericMessage {
	m.MsgMetadata[key] = value

	return m
}

// Metadata returns the message metadata.
func (m *GenericMessage) Metadata() Metadata {
	return m.MsgMetadata
}

// Payload returns the message payload.
func (m *GenericMessage) Payload() []byte {
	return m.MsgPayload
}

// Published returns if message is published to broker.
func (m *GenericMessage) Published() bool {
	return m.MsgPublished
}

// At returns message creation moment.
func (m *GenericMessage) At() time.Time {
	return m.MsgAt
}
