package nats_test

import (
	"testing"

	"github.com/x4b1/messenger/broker/nats"
)

const subjectKey = "test-key"

type publisher struct {
	*nats.Publisher

	*ConnMock
}

func newPublisher() *publisher {
	pub := publisher{
		ConnMock: &ConnMock{},
	}

	pub.Publisher = nats.NewPublisher(pub.ConnMock, subjectKey)

	return &pub
}

func TestPublish(t *testing.T) {

}
