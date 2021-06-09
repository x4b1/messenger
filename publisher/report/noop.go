package report

import "github.com/xabi93/messenger"

type Noop struct{}

func (Noop) Error(error)                     {}
func (Noop) Init(int)                        {}
func (Noop) Failed(messenger.Message, error) {}
func (Noop) Success(messenger.Message)       {}
func (Noop) End()                            {}
