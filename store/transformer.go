package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/x4b1/messenger"
)

// The TransformerFunc type is an adapter to allow the use of
// ordinary functions as Transformer. If f is a function
// with the appropriate signature, TransformerFunc(f) is a
// [Transformer] that calls f.
type TransformerFunc func(context.Context, any) (messenger.Message, error)

// Transform calls f(ctx, msg).
func (f TransformerFunc) Transform(ctx context.Context, msg any) (messenger.Message, error) {
	return f(ctx, msg)
}

// Transformer allows modify a message before store it. This function can be used to add some
// additional metadata.
type Transformer interface {
	Transform(context.Context, any) (messenger.Message, error)
}

func NewDefaultTransformer() TransformerFunc {
	return func(ctx context.Context, in any) (messenger.Message, error){
		if v, ok := in.(messenger.Message); ok {
			return v, nil
		}
		switch v := in.(type) {
		case string:
			return messenger.NewMessage([]byte(v))
		case []byte:
			return messenger.NewMessage(v)
		}

		payload, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("encoding payload: %w", err)
		}

		return messenger.NewMessage(payload)
	}
}