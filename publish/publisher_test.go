package publish_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publish"
)

type publisherSuite struct {
	suite.Suite

	publisher *publish.Publisher

	sourceMock  *publish.SourceMock
	publishMock *publish.QueueMock
	reportMock  *publish.ReporterMock

	batchSize int64

	messages []messenger.Message
}

func (s *publisherSuite) SetupTest() {
	s.sourceMock = &publish.SourceMock{}
	s.publishMock = &publish.QueueMock{}
	s.reportMock = &publish.ReporterMock{}
	s.batchSize = 10
	s.messages = []messenger.Message{
		{ID: uuid.Must(uuid.NewRandom())},
		{ID: uuid.Must(uuid.NewRandom())},
		{ID: uuid.Must(uuid.NewRandom())},
	}

	s.publisher = publish.NewPublisher(
		s.sourceMock,
		s.publishMock,
		s.batchSize,
		publish.WithReport(s.reportMock),
	)
}

func (s *publisherSuite) TestPublishMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		return s.messages, nil
	}

	publisherror := errors.New("publishing error")

	s.publishMock.PublishFunc = func(_ context.Context, msg messenger.Message) error {
		if msg.ID == s.messages[1].ID {
			return publisherror
		}

		return nil
	}

	expectedError := publish.NewPublishErrors()
	expectedError.Add(s.messages[1], publisherror)

	err := s.publisher.Publish(context.Background())
	s.Error(err)
	s.Equal(expectedError, err)

	s.Len(s.sourceMock.MessagesCalls(), 1)
	s.Equal(s.sourceMock.MessagesCalls()[0].Batch, s.batchSize)

	s.Len(s.publishMock.PublishCalls(), 3)
	for i, c := range s.publishMock.PublishCalls() {
		s.Equal(s.messages[i], c.Msg)
	}

	s.Len(s.sourceMock.PublishedCalls(), 2)
	for i, c := range []messenger.Message{s.messages[0], s.messages[2]} {
		s.Equal(c, s.sourceMock.PublishedCalls()[i].Msg[0])
	}

	s.Len(s.reportMock.InitCalls(), 1)
	s.Len(s.reportMock.FinishCalls(), 1)
	s.Len(s.reportMock.ErrorCalls(), 1)
	s.Equal(s.reportMock.ErrorCalls()[0].Err, expectedError)
}

func (s *publisherSuite) TestFailsGettingMessages() {
	gettingMessagesErr := errors.New("getting messages")
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		return nil, gettingMessagesErr
	}

	s.ErrorIs(s.publisher.Publish(context.Background()), gettingMessagesErr)
}

func (s *publisherSuite) TestFailsSavingPublishedMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		return s.messages, nil
	}

	savingMessagesErr := errors.New("saving messages")
	s.sourceMock.PublishedFunc = func(context.Context, ...messenger.Message) error {
		return savingMessagesErr
	}

	expectedErr := publish.NewPublishErrors()
	for _, msg := range s.messages {
		expectedErr.Add(msg, savingMessagesErr)
	}

	s.Equal(s.publisher.Publish(context.Background()), expectedErr)
}

func (s *publisherSuite) TestNotCallSavePublishedMessagesWhenNoMessages() {
	s.NoError(s.publisher.Publish(context.Background()))

	s.Empty(s.publishMock.PublishCalls())
	s.Empty(s.sourceMock.PublishedCalls())
}

func (s *publisherSuite) TestStartsAndStopsWithContext() {
	ctx, cancel := context.WithCancel(context.Background())
	runTimes := 0
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		runTimes++
		if runTimes >= 3 {
			cancel()
		}

		return []messenger.Message{}, nil
	}

	s.NoError(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
}

func (s *publisherSuite) TestStartsMessagesReturnsError() {
	ctx := context.Background()
	messagesErr := errors.New("messages error")
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		return nil, messagesErr
	}

	s.Error(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
	s.Len(s.reportMock.ErrorCalls(), 1)
	s.ErrorIs(messagesErr, s.reportMock.ErrorCalls()[0].Err)
}

func (s *publisherSuite) TestStartsPublishReturnsError() {
	ctx, cancel := context.WithCancel(context.Background())
	s.sourceMock.MessagesFunc = func(context.Context, int64) ([]messenger.Message, error) {
		return s.messages, nil
	}
	runTimes := 0
	publishErr := errors.New("publishing error")
	s.sourceMock.PublishedFunc = func(context.Context, ...messenger.Message) error {
		runTimes++
		if runTimes >= 3 {
			cancel()
		}

		return publishErr
	}

	s.NoError(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
	s.Len(s.reportMock.ErrorCalls(), 1)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(publisherSuite))
}
