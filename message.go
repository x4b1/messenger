package messenger

import (
	"errors"
)

// ErrEmptyMessagePayload is the error returned when the message payload is empty.
var ErrEmptyMessagePayload = errors.New("empty message payload")

// NewMessage returns a new Message given a payload.
func NewMessage(payload []byte) (*GenericMessage, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyMessagePayload
	}

	return &GenericMessage{
		payload:  payload,
		metadata: map[string]string{},
	}, nil
}

// A Message represents a message to be sent to message message queue.
type Message interface {
	Metadata() map[string]string
	Payload() interface{}
}

// GenericMessage represents a message to be sent to message message queue.
// It implements the Message interface.
type GenericMessage struct {
	// Metadata contains the message header to be sent by the messenger to the message queue.
	metadata map[string]string
	// Payload is the message payload.
	// Must not be empty
	payload interface{}
}

// AddMetadata adds the given key-value pair to the message metadata.
func (m *GenericMessage) AddMetadata(key, value string) {
	m.metadata[key] = value
}

// Metadata returns the message metadata.
func (m *GenericMessage) Metadata() map[string]string {
	return m.metadata
}

// Payload returns the message payload.
func (m *GenericMessage) Payload() interface{} {
	return m.payload
}
