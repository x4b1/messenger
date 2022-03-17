package publish_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/xabi93/messenger/publish"
	"github.com/xabi93/messenger/store"
)

type publisherSuite struct {
	suite.Suite

	publisher *publish.Publisher

	sourceMock  *publish.SourceMock
	publishMock *publish.QueueMock
	reportMock  *publish.ReporterMock

	publisherName string
	batchSize     int

	messages []*store.Message
}

func (s *publisherSuite) SetupTest() {
	s.sourceMock = &publish.SourceMock{}
	s.publishMock = &publish.QueueMock{}
	s.reportMock = &publish.ReporterMock{}
	s.publisherName = "my-publisher"
	s.batchSize = 10
	s.messages = []*store.Message{
		{ID: "87935650-9d6c-4752-80a0-8bcdf321680e"},
		{ID: "6d91abdd-561d-4d56-959f-f060b4c866ad"},
		{ID: "e6b11966-5b4a-4d3a-84c0-446fb78c616d"},
	}

	s.publisher = publish.NewPublisher(
		s.publisherName,
		s.sourceMock,
		s.publishMock,
		s.batchSize,
		publish.WithReport(s.reportMock),
	)
}

func (s *publisherSuite) TestPublishMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		return s.messages, nil
	}

	publisherror := errors.New("publishing error")

	s.publishMock.PublishFunc = func(_ context.Context, msg *store.Message) error {
		if msg.ID == s.messages[1].ID {
			return publisherror
		}

		return nil
	}

	err := s.publisher.Publish(context.Background())
	s.Error(err)
	s.ErrorIs(err, publisherror)

	s.Len(s.sourceMock.MessagesCalls(), 1)
	s.Equal(s.sourceMock.MessagesCalls()[0].PublisherName, s.publisherName)
	s.Equal(s.sourceMock.MessagesCalls()[0].Batch, s.batchSize)

	s.Len(s.publishMock.PublishCalls(), 2)
	for i, c := range s.publishMock.PublishCalls() {
		s.Equal(s.messages[i], c.Msg)
	}

	s.Len(s.sourceMock.SaveLastPublishedCalls(), 1)
	s.Equal(s.publisherName, s.sourceMock.SaveLastPublishedCalls()[0].PublisherName)
	s.Equal(s.messages[0], s.sourceMock.SaveLastPublishedCalls()[0].Msg)

	s.Len(s.reportMock.ReportCalls(), 1)
	s.False(s.reportMock.ReportCalls()[0].R.StartTime.IsZero())
	s.False(s.reportMock.ReportCalls()[0].R.EndTime.IsZero())
	s.Equal(3, s.reportMock.ReportCalls()[0].R.Total)
	s.Equal(1, s.reportMock.ReportCalls()[0].R.Success)
	s.Equal(publisherror, s.reportMock.ReportCalls()[0].R.Error)
}

func (s *publisherSuite) TestFailsGettingMessages() {
	gettingMessagesErr := errors.New("getting messages")
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		return nil, gettingMessagesErr
	}

	s.ErrorIs(s.publisher.Publish(context.Background()), gettingMessagesErr)
	s.Len(s.reportMock.ReportCalls(), 1)
	s.False(s.reportMock.ReportCalls()[0].R.StartTime.IsZero())
	s.False(s.reportMock.ReportCalls()[0].R.EndTime.IsZero())
	s.Equal(0, s.reportMock.ReportCalls()[0].R.Total)
	s.Equal(0, s.reportMock.ReportCalls()[0].R.Success)
	s.ErrorIs(s.reportMock.ReportCalls()[0].R.Error, gettingMessagesErr)
}

func (s *publisherSuite) TestFailsSavingPublishedMessages() {
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		return s.messages, nil
	}

	savingMessagesErr := errors.New("saving messages")
	s.sourceMock.SaveLastPublishedFunc = func(context.Context, string, *store.Message) error {
		return savingMessagesErr
	}

	s.ErrorIs(s.publisher.Publish(context.Background()), savingMessagesErr)
}

func (s *publisherSuite) TestNotCallSavePublishedMessagesWhenNoMessages() {
	s.NoError(s.publisher.Publish(context.Background()))

	s.Empty(s.publishMock.PublishCalls())
	s.Empty(s.sourceMock.SaveLastPublishedCalls())
}

func (s *publisherSuite) TestStartsAndStopsWithContext() {
	ctx, cancel := context.WithCancel(context.Background())
	runTimes := 0
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		runTimes++
		if runTimes >= 3 {
			cancel()
		}

		return []*store.Message{}, nil
	}

	s.NoError(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
}

func (s *publisherSuite) TestStartMessagesErrorReturnsError() {
	ctx := context.Background()
	messagesErr := errors.New("messages error")
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		return nil, messagesErr
	}

	s.Error(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
	s.Len(s.reportMock.ReportCalls(), 1)
	s.ErrorIs(s.reportMock.ReportCalls()[0].R.Error, messagesErr)
}

func (s *publisherSuite) TestStartsPublishReturnsError() {
	ctx, cancel := context.WithCancel(context.Background())
	s.sourceMock.MessagesFunc = func(context.Context, string, int) ([]*store.Message, error) {
		return s.messages, nil
	}
	runTimes := 0
	publishErr := errors.New("publishing error")
	s.publishMock.PublishFunc = func(context.Context, *store.Message) error {
		runTimes++
		if runTimes >= 3 {
			cancel()
		}

		return publishErr
	}

	s.NoError(s.publisher.Start(ctx, time.NewTicker(time.Millisecond)))
	s.Len(s.reportMock.ReportCalls(), 3)
	s.ErrorIs(s.reportMock.ReportCalls()[0].R.Error, publishErr)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(publisherSuite))
}
