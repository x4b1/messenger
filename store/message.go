// Package store the message for internal use that should not be exported
package store

import "time"

// Message represents retrieved from store and to be published to the bus
type Message struct {
	ID       string
	Payload  []byte
	Metadata map[string]string
	At       time.Time
}
