package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type metadata map[string]string

func (m metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

// Make the Attrs struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (m *metadata) Scan(value any) error {
	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, &m)
	case string:
		return json.Unmarshal([]byte(v), &m)
	}

	return fmt.Errorf("scanning metadata: unknown type %T", value)
}
