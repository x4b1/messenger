package postgres

import "github.com/x4b1/messenger/store"

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

// WithTransformer applies sets a custom message transformer.
func WithTransformer(tr store.Transformer) Option {
	return func(c *Storer) {
		c.transformer = tr
	}
}
