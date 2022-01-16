package publish_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publish"
)

func TestPublishErrors(t *testing.T) {
	t.Parallel()

	msg1 := messenger.Message{ID: uuid.MustParse("cfa4d3b8-721d-4ce4-b71f-a84cab3e4471")}
	err1 := errors.New("publishing error 1")
	msg2 := messenger.Message{ID: uuid.MustParse("e0932677-a085-4b18-bfce-ee0699e01c4c")}
	err2 := errors.New("publishing error 2")

	errs := publish.NewPublishErrors()
	errs.Add(msg1, err1)
	errs.Add(msg2, err2)

	require.Equal(t, `publishing errors:
	cfa4d3b8-721d-4ce4-b71f-a84cab3e4471: publishing error 1
	e0932677-a085-4b18-bfce-ee0699e01c4c: publishing error 2
`,
		errs.Error(),
	)
}
