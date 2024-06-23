package postgres

import "github.com/x4b1/messenger/store"

// Option is a function to set options to Publisher.
type Option func(any)

// WithSchema setups schema name.
func WithSchema(s string) Option {
	return func(c any) {
		cfg, ok := c.(*config)
		if !ok {
			return
		}
		cfg.schema = s
	}
}

// WithTableName setups schema name.
func WithTableName(t string) Option {
	return func(c any) {
		cfg, ok := c.(*config)
		if !ok {
			return
		}
		cfg.table = t
	}
}

// WithJSONPayload creates payload column as JSONB.
func WithJSONPayload() Option {
	return func(c any) {
		cfg, ok := c.(*config)
		if !ok {
			return
		}
		cfg.jsonPayload = true
	}
}

// WithTransformer applies sets a custom message transformer.
func WithTransformer[M any, T Storer[M]](tr store.Transformer[M]) Option {
	return func(c any) {
		s, ok := c.(*Storer[M])
		if !ok {
			return
		}
		s.transformer = tr
	}
}
