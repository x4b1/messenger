package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type metadata map[string]string

func (m metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

// Make the Attrs struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (m *metadata) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &m)
}
