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

func TestBufferedConsumerBufferEmpty(t *testing.T) {
	type testCase struct {
		Buffer   []kafka.Message
		Expected bool
	}
	testCases := []testCase{
		{Buffer: []kafka.Message{}, Expected: true},
		{Buffer: []kafka.Message{{}, {}, {}}, Expected: false},
		{Buffer: []kafka.Message{{}, {}, {}, {}, {}}, Expected: false},
	}
	for _, testCase := range testCases {
		consumer := BufferedConsumer{
			BufferSize:    5,
			FlushInterval: time.Duration(0),
			reader:        nil,
			writer:        nil,
			buffer:        testCase.Buffer,
		}
		actual := consumer.BufferEmpty()
		assert.Equal(t, testCase.Expected, actual)
	}
}

func TestBufferedConsumerBufferFull(t *testing.T) {
	type testCase struct {
		Buffer   []kafka.Message
		Expected bool
	}
	testCases := []testCase{
		{Buffer: []kafka.Message{}, Expected: false},
		{Buffer: []kafka.Message{{}, {}, {}}, Expected: false},
		{Buffer: []kafka.Message{{}, {}, {}, {}, {}}, Expected: true},
	}
	for _, testCase := range testCases {
		consumer := BufferedConsumer{
			BufferSize:    5,
			FlushInterval: time.Duration(0),
			reader:        nil,
			writer:        nil,
			buffer:        testCase.Buffer,
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
	}

	err := consumer.Fetch(ctx)
	assert.ErrorIs(t, ErrBufferFull, err)
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
		buffer:     make([]kafka.Message, 0, bufferSize),
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, 0, actual)
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
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, 3, actual)

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
	buffer := make([]kafka.Message, 0, bufferSize)
	buffer = append(buffer, unflushedMessage)

	consumer := BufferedConsumer{
		BufferSize:    bufferSize,
		FlushInterval: flushInterval,
		reader:        mockR,
		writer:        mockW,
		buffer:        buffer,
	}
	actual, err := consumer.Flush(ctx)

	assert.Nil(t, err)
	assert.Equal(t, 1, actual)

	mockW.AssertCalled(t, "Write", ctx, []kafka.Message{unflushedMessage})
	mockR.AssertCalled(t, "CommitMessages", ctx, []kafka.Message{unflushedMessage})
}
