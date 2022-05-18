package publish

import (
	"fmt"
	"strings"

	"github.com/xabi93/messenger/store"
)

// NewErrors returns a new PublishErrors.
func NewErrors() Errors {
	return make(Errors, 0)
}

// Errors is a collection messages that failed on publish.
type Errors []struct {
	msg *store.Message
	err error
}

// Error returns a string representation of the error.
func (errs Errors) Error() string {
	var b strings.Builder
	b.WriteString("publishing errors:\n")
	for _, err := range errs {
		fmt.Fprintf(&b, "\t%s: %s\n", err.msg.ID, err.err)
	}

	return b.String()
}

func (errs *Errors) Add(msg *store.Message, err error) {
	*errs = append(*errs, struct {
		msg *store.Message
		err error
	}{msg, err})
}
