package publisher_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publisher"
)

type workerSuite struct {
	suite.Suite

	worker publisher.Worker

	sourceMock    *publisher.SourceMock
	publisherMock *publisher.PublisherMock
	reportMock    *publisher.ReporterMock

	batchSize int64
	ticker    chan time.Time

	messages []*messenger.Message
}

func (s *workerSuite) SetupTest() {
	s.sourceMock = &publisher.SourceMock{}
	s.publisherMock = &publisher.PublisherMock{}
	s.reportMock = &publisher.ReporterMock{}
	s.batchSize = 10
	s.ticker = make(chan time.Time)
	s.messages = []*messenger.Message{
		{ID: uuid.Must(uuid.NewRandom())},
		{ID: uuid.Must(uuid.NewRandom())},
		{ID: uuid.Must(uuid.NewRandom())},
	}

	s.worker = publisher.NewWorker(
		s.sourceMock,
		s.publisherMock,
		s.batchSize,
		&time.Ticker{C: s.ticker},
		publisher.WithReport(s.reportMock),
	)
}

func (s workerSuite) runSync() {
	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		s.worker.Start(ctx)
		wg.Done()
	}()

	s.ticker <- time.Now()
	cancel()
	wg.Wait()
}

func (s *workerSuite) TestPublishMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]*messenger.Message, error) {
		return s.messages, nil
	}

	publishError := errors.New("publishing error")

	s.publisherMock.PublishFunc = func(_ context.Context, msg *messenger.Message) error {
		if msg == s.messages[1] {
			return publishError
		}

		return nil
	}

	s.runSync()

	s.Len(s.sourceMock.MessagesCalls(), 1)
	s.Equal(s.sourceMock.MessagesCalls()[0].Batch, s.batchSize)

	s.Len(s.publisherMock.PublishCalls(), 3)
	for i, c := range s.publisherMock.PublishCalls() {
		s.Equal(s.messages[i], c.Msg)
	}

	s.Len(s.sourceMock.PublishedCalls(), 1)
	s.Equal(s.sourceMock.PublishedCalls()[0].Msg, []*messenger.Message{s.messages[0], s.messages[2]})

	s.Empty(s.reportMock.ErrorCalls())

	s.Len(s.reportMock.InitCalls(), 1)
	s.Equal(3, s.reportMock.InitCalls()[0].TotalMsgs)
	s.Len(s.reportMock.SuccessCalls(), 2)
	s.Equal(*s.messages[0], s.reportMock.SuccessCalls()[0].Msg)
	s.Equal(*s.messages[2], s.reportMock.SuccessCalls()[1].Msg)
	s.Len(s.reportMock.FailedCalls(), 1)
	s.Equal(*s.messages[1], s.reportMock.FailedCalls()[0].Msg)
	s.ErrorIs(s.reportMock.FailedCalls()[0].Err, publishError)
}

func (s *workerSuite) TestFailsGettingMessages() {
	gettingMessagesErr := errors.New("getting messages")
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]*messenger.Message, error) {
		return nil, gettingMessagesErr
	}

	s.runSync()

	s.Len(s.reportMock.ErrorCalls(), 1)
	s.ErrorIs(s.reportMock.ErrorCalls()[0].Err, gettingMessagesErr)
}

func (s *workerSuite) TestFailsSavingPublishedMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]*messenger.Message, error) {
		return s.messages, nil
	}

	savingMessagesErr := errors.New("saving messages")
	s.sourceMock.PublishedFunc = func(context.Context, ...*messenger.Message) error {
		return savingMessagesErr
	}

	s.runSync()

	s.Len(s.reportMock.ErrorCalls(), 1)
	s.ErrorIs(s.reportMock.ErrorCalls()[0].Err, savingMessagesErr)
}

func (s *workerSuite) TestNotCallSavePublishedMessagesWhenNoMessages() {
	s.runSync()
	s.Empty(s.sourceMock.PublishedCalls())
}

func TestWorker(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(workerSuite))
}
