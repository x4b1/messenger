package postgres

// Option is a function to set options to Publisher.
type Option func(*Storer)

// WithSchema setups schema name.
func WithSchema(s string) Option {
	return func(c *Storer) {
		c.schema = s
	}
}

// WithTableName setups table name.
func WithTableName(t string) Option {
	return func(c *Storer) {
		c.table = t
	}
}

// WithJSONPayload creates payload column as JSONB.
func WithJSONPayload() Option {
	return func(c *Storer) {
		c.jsonPayload = true
	}
}
