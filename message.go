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
		Id:       uuid.Must(uuid.NewRandom()).String(),
		Payload:  payload,
		Metadata: map[string]string{},
		At:       time.Now(),
	}, nil
}

// A Message represents a message to be sent to message message queue.
type Message interface {
	ID() string
	GetMetadata() map[string]string
	GetPayload() []byte
}

// GenericMessage represents a message to be sent to message message queue.
// It implements the Message interface.
type GenericMessage struct {
	// Unique identifier for the message.
	Id string //nolint:revive,stylecheck // conflicts with method name
	// Contains the message header to be sent by the messenger to the message queue.
	Metadata map[string]string
	// Payload is the message payload. Must not be empty
	Payload []byte
	// At represent the moment of the message creation.
	At time.Time
}

// ID adds the given key-value pair to the message metadata.
func (m *GenericMessage) ID() string {
	return m.Id
}

// SetMetadata sets the given key-value pair to the message metadata.
// If the key already exists, it replaces the value.
func (m *GenericMessage) SetMetadata(key, value string) *GenericMessage {
	m.Metadata[key] = value

	return m
}

// Metadata returns the message metadata.
func (m *GenericMessage) GetMetadata() map[string]string {
	return m.Metadata
}

// Payload returns the message payload.
func (m *GenericMessage) GetPayload() []byte {
	return m.Payload
}
