package publish_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/x4b1/messenger/publish"
	"github.com/x4b1/messenger/store"
)

func TestPublishErrors(t *testing.T) {
	t.Parallel()

	msg1 := &store.Message{ID: "cfa4d3b8-721d-4ce4-b71f-a84cab3e4471"}
	err1 := errors.New("publishing error 1")
	msg2 := &store.Message{ID: "e0932677-a085-4b18-bfce-ee0699e01c4c"}
	err2 := errors.New("publishing error 2")

	errs := publish.NewErrors()
	errs.Add(msg1, err1)
	errs.Add(msg2, err2)

	require.Equal(t, `publishing errors:
	cfa4d3b8-721d-4ce4-b71f-a84cab3e4471: publishing error 1
	e0932677-a085-4b18-bfce-ee0699e01c4c: publishing error 2
`,
		errs.Error(),
	)
}
