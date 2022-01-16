package messenger

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Metadata maps a string key to a string value.
// It is used to add contextual data to the message, like for events, the event name.
type Metadata map[string]string

// Get returns the value of the given key if exists, if not returns empty.
func (m Metadata) Get(key string) string {
	return m[key]
}

// Set adds or replaces the value of the metadata for the given key.
func (m Metadata) Set(key, value string) {
	m[key] = value
}

// NewMessage returns a new Message given a payload.
func NewMessage(payload []byte) (Message, error) {
	if len(payload) == 0 {
		return Message{}, errors.New("payload cannot be empty")
	}

	return Message{
		ID:        uuid.Must(uuid.NewRandom()),
		Payload:   payload,
		Metadata:  Metadata{},
		CreatedAt: time.Now().UTC(),
	}, nil
}

// A Message represents a message to be sent to message message queue.
type Message struct {
	// ID is the unique identifier for the message, it is used to store the message and to set in
	// message queue
	ID uuid.UUID
	// Metadata contains the message header to be sent by the messenger to the message queue.
	//
	// typical message headers:
	// - message type
	// - aggregate id
	Metadata Metadata
	// Payload is the message payload.
	//
	// Payload must not be empty
	Payload []byte
	// CreatedAt represents the moment when it is created the message in UTC.
	// this field is used to publish the messages in order of creation
	CreatedAt time.Time
}
