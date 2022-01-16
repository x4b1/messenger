package report

type Noop struct{}

func (Noop) Error(error)             {}
func (Noop) Init()                   {}
func (Noop) TotalMessages(total int) {}
func (Noop) Finish()                 {}
