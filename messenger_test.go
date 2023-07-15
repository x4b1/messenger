package messenger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/x4b1/messenger"
)

type publisherSuite struct {
	suite.Suite

	publisher *messenger.Messenger

	sourceMock    *StoreMock
	publishMock   *BrokerMock
	errLoggerMock *ErrorLoggerMock

	batchSize int

	messages []messenger.Message
}

func (s *publisherSuite) SetupTest() {
	s.sourceMock = &StoreMock{}
	s.publishMock = &BrokerMock{}
	s.errLoggerMock = &ErrorLoggerMock{}

	s.batchSize = 10
	s.messages = []messenger.Message{
		&messenger.GenericMessage{Id: "87935650-9d6c-4752-80a0-8bcdf321680e"},
		&messenger.GenericMessage{Id: "6d91abdd-561d-4d56-959f-f060b4c866ad"},
		&messenger.GenericMessage{Id: "e6b11966-5b4a-4d3a-84c0-446fb78c616d"},
	}

	s.publisher = messenger.NewMessenger(
		s.sourceMock,
		s.publishMock,
		messenger.WithErrorLogger(s.errLoggerMock),
		messenger.WithPublishBatchSize(s.batchSize),
	)
}

func (s *publisherSuite) TestPublishMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
		return s.messages, nil
	}

	publisherror := errors.New("publishing error")

	s.publishMock.PublishFunc = func(_ context.Context, msg messenger.Message) error {
		if msg.ID() == s.messages[1].ID() {
			return publisherror
		}

		return nil
	}

	err := s.publisher.Publish(context.Background())
	s.Error(err)
	s.ErrorIs(err, publisherror)

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
}

func (s *publisherSuite) TestFailsGettingMessages() {
	gettingMessagesErr := errors.New("getting messages")
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
		return nil, gettingMessagesErr
	}

	s.ErrorIs(s.publisher.Publish(context.Background()), gettingMessagesErr)
}

func (s *publisherSuite) TestFailsSavingPublishedMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
		return s.messages, nil
	}

	savingMessagesErr := errors.New("saving messages")
	s.sourceMock.PublishedFunc = func(context.Context, ...messenger.Message) error {
		return savingMessagesErr
	}

	err := s.publisher.Publish(context.Background())
	errors := err.(interface{ Unwrap() []error }).Unwrap()
	s.Len(errors, 3)

	for _, err := range errors {
		s.ErrorIs(err, savingMessagesErr)
	}
}

func (s *publisherSuite) TestNotCallSavePublishedMessagesWhenNoMessages() {
	s.NoError(s.publisher.Publish(context.Background()))

	s.Empty(s.publishMock.PublishCalls())
	s.Empty(s.sourceMock.PublishedCalls())
}

func (s *publisherSuite) TestStartsAndStopsWithContext() {
	ctx, cancel := context.WithCancel(context.Background())
	runTimes := 0
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
		runTimes++
		if runTimes >= 3 {
			cancel()
		}

		return []messenger.Message{}, nil
	}

	s.NoError(s.publisher.Start(ctx))
}

func (s *publisherSuite) TestStartsMessagesReturnsError() {
	ctx := context.Background()
	messagesErr := errors.New("messages error")
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
		return nil, messagesErr
	}

	s.Error(s.publisher.Start(ctx))
}

func (s *publisherSuite) TestStartsPublishReturnsError() {
	ctx, cancel := context.WithCancel(context.Background())
	s.sourceMock.MessagesFunc = func(context.Context, int) ([]messenger.Message, error) {
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

	s.NoError(s.publisher.Start(ctx))
	s.Len(s.errLoggerMock.ErrorCalls(), 1)
	s.ErrorIs(s.errLoggerMock.ErrorCalls()[0].Err, publishErr)
}

func (s *publisherSuite) TestStartWithoutCleanSetupNotStartsProcess() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	s.NoError(s.publisher.Start(ctx))

	s.Empty(s.sourceMock.DeletePublishedByExpirationCalls())
}

func (s *publisherSuite) TestStartWithCleanSetupStartsProcess() {
	expectedExpiration := time.Hour
	s.publisher = messenger.NewMessenger(
		s.sourceMock,
		s.publishMock,
		messenger.WithErrorLogger(s.errLoggerMock),
		messenger.WithCleanUp(expectedExpiration),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second * 2)
		cancel()
	}()
	s.NoError(s.publisher.Start(ctx))

	s.GreaterOrEqual(len(s.sourceMock.DeletePublishedByExpirationCalls()), 1)
	s.Equal(
		expectedExpiration,
		s.sourceMock.DeletePublishedByExpirationCalls()[0].Exp,
	)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(publisherSuite))
}
