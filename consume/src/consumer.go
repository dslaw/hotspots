package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Readable provides methods for consuming messages from Kafka.
type Readable interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

// Writable provides a method for writing consumed messages to a data sink.
type Writable interface {
	Write(context.Context, []kafka.Message) error
}

// BufferedConsumer consumes messages from a Kafka topic. Messages are buffered
// and written according to the BufferSize and FlushInterval parameters.
type BufferedConsumer struct {
	// Maximum number of messages that may be held before flushing.
	BufferSize int
	// Maximum time to wait between flushing.
	FlushInterval time.Duration

	reader Readable
	writer Writable
	buffer []kafka.Message
	// bufferIdx gives the next writable index into the buffer. Messages in
	// the buffer prior to bufferIdx are waiting to be flushed, while any
	// messages at or after bufferIdx are have already been flushed and should
	// not be reprocessed.
	bufferIdx int
}

func NewBufferedConsumer(bufferSize int, flushInterval time.Duration, reader Readable, writer Writable) *BufferedConsumer {
	if bufferSize <= 0 {
		panic("Buffer size must be positive")
	}
	if flushInterval <= 0 {
		panic("Flush interval must be positive")
	}

	buffer := make([]kafka.Message, bufferSize)
	return &BufferedConsumer{BufferSize: bufferSize, FlushInterval: flushInterval, reader: reader, writer: writer, buffer: buffer, bufferIdx: 0}
}

// BufferFull returns whether the buffer is full of unprocessed messages, i.e.
// whether the buffer needs to be flushed.
func (r *BufferedConsumer) BufferFull() bool {
	// Previously flushed messages may linger in the buffer, so `len(r.buffer)`
	// cannot be directly used to get the number of buffered messages.
	return r.bufferIdx >= cap(r.buffer)
}

// Fetch fetches a message and buffers it. If the buffer is full, no message is
// fetched and ErrBufferFull is returned.
func (r *BufferedConsumer) Fetch(ctx context.Context) error {
	if r.BufferFull() {
		return ErrBufferFull
	}

	msg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	r.buffer[r.bufferIdx] = msg
	r.bufferIdx += 1
	return nil
}

func (r *BufferedConsumer) numBufferedMessages() int {
	if r.BufferFull() {
		return cap(r.buffer)
	}
	return r.bufferIdx
}

// Flush flushes buffered messages out and marks them as committed in Kafka.
// Note that messages may be processed more than once.
func (r *BufferedConsumer) Flush(ctx context.Context) (uint, error) {
	bufferEndIdx := r.numBufferedMessages()
	if bufferEndIdx == 0 {
		// No unprocessed messages.
		return 0, nil
	}

	messages := r.buffer[:bufferEndIdx]

	if err := r.writer.Write(ctx, messages); err != nil {
		slog.Error("Unable to write data", "error", err)
		return 0, err
	}

	// XXX: The commit to Kafka may fail after the writer was successful, in
	// which case the uncommitted messages will be reprocessed.
	if err := r.reader.CommitMessages(ctx, messages...); err != nil {
		slog.Error("Unable to commit messages", "error", err)
		return 0, err
	}

	// TODO: Could definitely clear the buffer out. Should be fast and might
	// make things less brittle.
	r.bufferIdx = 0
	return uint(bufferEndIdx), nil
}

func (r *BufferedConsumer) Process(ctx context.Context) error {
	for {
		// Fetch and buffer messages until either the buffer is full, or the
		// flush interval has been met. As the underlying Kafka client blocks
		// when fetching a message, a timeout is used to cancel fetches in the
		// event that that flush interval has been met. This is used to handle
		// the case where there are buffered messages which should be flushed
		// due to the flush interval, but the Kafka reader is blocking due to no
		// new messages.
		fetchCtx, cancel := context.WithTimeout(ctx, r.FlushInterval)

		for {
			err := r.Fetch(fetchCtx)
			flush := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBufferFull)

			if err != nil && !flush {
				slog.Info("Terminating without flushing", "error", err)
				cancel() // Cleanup `fetchCtx`.
				panic(err.Error())
			}

			if flush {
				numFlushed, flushErr := r.Flush(ctx)
				if flushErr != nil {
					cancel() // Cleanup `fetchCtx`.
					return flushErr
				}

				slog.Info("Flushed buffer", "n_flushed_messages", numFlushed)
				break
			}
		}

		cancel() // Cleanup `fetchCtx`.
	}
}
