package store

import (
	"context"

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


var ErrUnknownMessagePayload = errors.New("unknown message type")

func NewDefaultTransformer() TransformerFunc {
	return func(ctx context.Context, in any) (messenger.Message, error){
		if v, ok := in.(messenger.Message); ok {
			return v, nil
		}
		switch v := in.(type) {
		case string:
			return messenger.NewMessage(v)
		case []byte:
			return messenger.NewMessage(v)
		}

		return nil, ErrUnknownMessagePayload
	}
}