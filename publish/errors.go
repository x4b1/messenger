package publish

import "errors"

// FatalError defines an error that should stop publishing process
type FatalError struct{ error }

// Error implements error internface
func (e *FatalError) Error() string {
	return e.error.Error()
}

// Unwrap implements errors.Unwrap functionality
func (e *FatalError) Unwrap() error { return e.error }

// WrapFatalError wraps given error as FatalError
func WrapFatalError(err error) error {
	return &FatalError{err}
}

// IsFatalError returns true if the give error is a FatalError
func IsFatalError(err error) bool {
	var target *FatalError

	return errors.As(err, &target)
}
