package publish

import (
	"fmt"
	"strings"

	"github.com/xabi93/messenger"
)

// NewPublishErrors returns a new PublishErrors.
func NewPublishErrors() PublishErrors {
	return make(PublishErrors, 0)
}

// PublishErrors is a collection messages that failed on publish.
type PublishErrors []struct {
	msg messenger.Message
	err error
}

// Error returns a string representation of the error.
func (errs PublishErrors) Error() string {
	var b strings.Builder
	b.WriteString("publishing errors:\n")
	for _, err := range errs {
		fmt.Fprintf(&b, "\t%s: %s\n", err.msg.ID, err.err)
	}

	return b.String()
}

func (errs *PublishErrors) Add(msg messenger.Message, err error) {
	*errs = append(*errs, struct {
		msg messenger.Message
		err error
	}{msg, err})
}
