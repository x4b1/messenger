package store

import (
	"context"

	"github.com/x4b1/messenger"
)

// The TransformerFunc type is an adapter to allow the use of
// ordinary functions as Transformer. If f is a function
// with the appropriate signature, TransformerFunc(f) is a
// [Transformer] that calls f.
type TransformerFunc func(context.Context, messenger.Message) error

// ServeHTTP calls f(w, r).
func (f TransformerFunc) Transform(ctx context.Context, msg messenger.Message) error {
	return f(ctx, msg)
}

// Transformer allows modify a message before store it. This function can be used to add some
// additional metadata.
type Transformer interface {
	Transform(context.Context, messenger.Message) error
}
