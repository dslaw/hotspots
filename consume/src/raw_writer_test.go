package main

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockClickhouseConn struct {
	mock.Mock
}

func (m *mockClickhouseConn) PrepareBatch(ctx context.Context, sql string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	args := m.Called(ctx, sql, opts)
	return args.Get(0).(driver.Batch), args.Error(1)
}

type mockBatch struct {
	mock.Mock
}

func (m *mockBatch) Abort() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockBatch) Append(v ...any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *mockBatch) AppendStruct(v any) error {
	args := m.Called(v)
	return args.Error(0)
}

func (m *mockBatch) Column(i int) driver.BatchColumn {
	args := m.Called(i)
	return args.Get(0).(driver.BatchColumn)
}

func (m *mockBatch) Flush() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockBatch) Send() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockBatch) IsSent() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *mockBatch) Rows() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockBatch) Columns() []column.Interface {
	args := m.Called()
	return args.Get(0).([]column.Interface)
}

func TestRawWriterWrite(t *testing.T) {
	batch := new(mockBatch)
	batch.On("Append", mock.Anything).Return(nil)
	batch.On("Flush").Return(nil)

	conn := new(mockClickhouseConn)
	conn.On("PrepareBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(batch, nil)

	record := &FireEmsCall{}
	payload, _ := record.Marshal()

	messages := []kafka.Message{
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
		// Message with unrecognized schema is skipped.
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte("Unknown")}}, Value: []byte("abc")},
		// Message without schema name header is skipped.
		{Headers: []kafka.Header{}, Value: []byte("abc")},
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
	}

	ctx := context.Background()
	writer := NewRawWriter(conn)
	err := writer.Write(ctx, messages)

	assert.Nil(t, err)
	conn.AssertCalled(t, "PrepareBatch", ctx, mock.Anything, mock.Anything, mock.Anything)
	batch.AssertNumberOfCalls(t, "Append", 2)
	batch.AssertCalled(t, "Flush")
}
