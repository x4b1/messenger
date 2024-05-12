package messenger

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Metadata defines a key value field sent with every message to add more context
// without the need of decoding the payload.
type Metadata map[string]string

// Get returns the metadata value for the given key.
// If the key is not found, an empty string is returned.
func (m Metadata) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}

	return ""
}

// Set sets the metadata key to value.
func (m Metadata) Set(key, value string) {
	m[key] = value
}

// Value implements sql.Valuer. Transforms Metadata into json.
func (m Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

// Scan implements the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (m *Metadata) Scan(value any) error {
	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, &m)
	case string:
		return json.Unmarshal([]byte(v), &m)
	}

	return fmt.Errorf("scanning metadata: unknown type %T", value)
}
