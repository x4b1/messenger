package log

import "fmt"

// NewDefault returns an instance of default logger.
func NewDefault() *Default {
	return &Default{}
}

// Default is the default implementation of the error logger.
type Default struct{}

// Error prints the given error to stdout.
func (d *Default) Error(err error) {
	//nolint:forbidigo // the implementation needs to print to stdout
	fmt.Println(err.Error())
}
