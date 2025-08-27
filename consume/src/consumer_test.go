package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockReader struct {
	mock.Mock
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafka.Message), args.Error(1)
}

func (m *mockReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	args := m.Called(ctx, messages)
	return args.Error(0)
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) Write(ctx context.Context, messages []kafka.Message) error {
	args := m.Called(ctx, messages)
	return args.Error(0)
}

func TestBufferedConsumerBufferFull(t *testing.T) {
	type testCase struct {
		BufferIdx int
		Expected  bool
	}
	testCases := []testCase{
		{BufferIdx: 0, Expected: false},
		{BufferIdx: 6, Expected: false},
		{BufferIdx: 10, Expected: true},
	}
	for _, testCase := range testCases {
		consumer := BufferedConsumer{
			BufferSize:    10,
			FlushInterval: time.Duration(0),
			reader:        nil,
			writer:        nil,
			buffer:        make([]kafka.Message, 10),
			bufferIdx:     testCase.BufferIdx,
		}
		actual := consumer.BufferFull()
		assert.Equal(t, testCase.Expected, actual)
	}
}

func TestBufferedConsumerFetch(t *testing.T) {
	ctx := context.Background()
	flushInterval := time.Hour

	mockR := new(mockReader)
	mockR.On("FetchMessage", ctx).Return(kafka.Message{}, nil)

	mockW := new(mockWriter)

	consumer := NewBufferedConsumer(10, flushInterval, mockR, mockW)

	err := consumer.Fetch(ctx)

	assert.Nil(t, err)
	mockR.AssertCalled(t, "FetchMessage", ctx)
}

func TestBufferedConsumerFetchWhenErrorFetchingMessage(t *testing.T) {
	ctx := context.Background()
	flushInterval := time.Hour
	mockFetchErr := fmt.Errorf("Error")

	mockR := new(mockReader)
	mockR.On("FetchMessage", ctx).Return(kafka.Message{}, mockFetchErr)

	mockW := new(mockWriter)

	consumer := NewBufferedConsumer(10, flushInterval, mockR, mockW)

	err := consumer.Fetch(ctx)

	assert.ErrorIs(t, mockFetchErr, err)
	mockR.AssertCalled(t, "FetchMessage", ctx)
}

func TestBufferedConsumerFetchWhenBufferFull(t *testing.T) {
	ctx := context.Background()
	flushInterval := time.Hour
	bufferSize := 3

	// Create a consumer with a full buffer.
	consumer := BufferedConsumer{
		BufferSize:    bufferSize,
		FlushInterval: flushInterval,
		reader:        nil,
		writer:        nil,
		buffer:        []kafka.Message{{}, {}, {}},
		bufferIdx:     3,
	}

	err := consumer.Fetch(ctx)
	assert.ErrorIs(t, ErrBufferFull, err)
}

func TestBufferedConsumerNumBufferedMessages(t *testing.T) {
	consumer := BufferedConsumer{
		BufferSize: 2,
		buffer:     make([]kafka.Message, 2),
		bufferIdx:  1,
	}
	actual := consumer.numBufferedMessages()
	assert.Equal(t, 1, actual)
}

func TestBufferedConsumerNumBufferedMessagesWhenFull(t *testing.T) {
	consumer := BufferedConsumer{
		BufferSize: 2,
		buffer:     make([]kafka.Message, 2),
		bufferIdx:  2,
	}
	actual := consumer.numBufferedMessages()
	assert.Equal(t, 2, actual)
}

func TestBufferedConsumerFlushWhenBufferEmpty(t *testing.T) {
	ctx := context.Background()
	bufferSize := 10

	mockR := new(mockReader)
	mockR.On("CommitMessages", ctx, mock.Anything).Return(nil)

	mockW := new(mockWriter)
	mockW.On("Write", ctx, mock.Anything).Return(nil)

	consumer := BufferedConsumer{
		BufferSize: bufferSize,
		buffer:     make([]kafka.Message, bufferSize),
		bufferIdx:  0,
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, uint(0), actual)
	assert.Equal(t, 0, consumer.bufferIdx)
}

func TestBufferedConsumerFlushWhenBufferFull(t *testing.T) {
	ctx := context.Background()
	flushInterval := time.Hour
	bufferSize := 3

	mockR := new(mockReader)
	mockR.On("CommitMessages", ctx, mock.Anything).Return(nil)

	mockW := new(mockWriter)
	mockW.On("Write", ctx, mock.Anything).Return(nil)

	buffer := []kafka.Message{{}, {}, {}}
	consumer := BufferedConsumer{
		BufferSize:    bufferSize,
		FlushInterval: flushInterval,
		reader:        mockR,
		writer:        mockW,
		buffer:        buffer,
		bufferIdx:     bufferSize,
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, uint(3), actual)
	assert.Equal(t, 0, consumer.bufferIdx)

	mockW.AssertCalled(t, "Write", ctx, buffer)
	mockR.AssertCalled(t, "CommitMessages", ctx, buffer)
}

func TestBufferedConsumerFlushWhenBufferPartial(t *testing.T) {
	ctx := context.Background()
	flushInterval := time.Hour
	bufferSize := 3

	mockR := new(mockReader)
	mockR.On("CommitMessages", ctx, mock.Anything).Return(nil)

	mockW := new(mockWriter)
	mockW.On("Write", ctx, mock.Anything).Return(nil)

	unflushedMessage := kafka.Message{Value: []byte("unflushed")}

	consumer := BufferedConsumer{
		BufferSize:    bufferSize,
		FlushInterval: flushInterval,
		reader:        mockR,
		writer:        mockW,
		// Buffer has one unflushed message (at first position) and two messages
		// which were previously flushed.
		buffer:    []kafka.Message{unflushedMessage, {}, {}},
		bufferIdx: 1,
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, uint(1), actual)
	assert.Equal(t, 0, consumer.bufferIdx)

	mockW.AssertCalled(t, "Write", ctx, []kafka.Message{unflushedMessage})
	mockR.AssertCalled(t, "CommitMessages", ctx, []kafka.Message{unflushedMessage})
}
