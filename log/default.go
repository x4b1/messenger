package log

import "fmt"

func NewDefault() *Default {
	return &Default{}
}

type Default struct{}

func (d *Default) Error(err error) {
	fmt.Println(err.Error()) //nolint:forbidigo // I need this
}
