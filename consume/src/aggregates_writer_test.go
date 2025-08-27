package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAggregateWriterAggregate(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)

	record := &FireEmsCall{
		ReceivedDttm: time.Date(2025, 1, 1, 13, 14, 15, 0, time.UTC),
		Lat:          37.786358,
		Long:         -122.41983,
	}
	payload, _ := record.Marshal()
	expectedGeohash := "9q8yyqb97"
	expectedTimestamp := time.Date(2025, 1, 1, 13, 15, 0, 0, time.UTC)

	recordWithoutLocation := &PoliceIncident{
		IncidentDatetime: time.Date(2025, 1, 1, 13, 14, 15, 0, time.UTC),
		Latitude:         nil,
		Longitude:        nil,
	}
	payloadWithoutLocation, _ := recordWithoutLocation.Marshal()

	writer := NewAggregateWriter(nil, NewBucketer(timePrecision, geohashPrecision))
	messages := []kafka.Message{
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
		// Message with unrecognized schema is skipped.
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte("Unknown")}}, Value: []byte("abc")},
		// Message without schema name header is skipped.
		{Headers: []kafka.Header{}, Value: []byte("abc")},
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
		// Message without location information is decoded, but not aggregated.
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNamePoliceIncident)}}, Value: payloadWithoutLocation},
	}

	expected := make(map[Bucket]int)
	expected[Bucket{Timestamp: expectedTimestamp, Geohash: expectedGeohash}] = 2

	actual := writer.Aggregate(messages)

	assert.Equal(t, expected, actual)
}

type mockPostgresConn struct {
	mock.Mock
}

func (m *mockPostgresConn) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	args := m.Called(ctx, batch)
	return args.Get(0).(pgx.BatchResults)
}

type mockBatchResults struct {
	mock.Mock
}

func (m *mockBatchResults) Exec() (pgconn.CommandTag, error) {
	args := m.Called()
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *mockBatchResults) Query() (pgx.Rows, error) {
	args := m.Called()
	return args.Get(0).(pgx.Rows), args.Error(1)
}

func (m *mockBatchResults) QueryRow() pgx.Row {
	args := m.Called()
	return args.Get(0).(pgx.Rows)
}

func (m *mockBatchResults) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestAggregateWriterWrite(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)

	batchResults := new(mockBatchResults)
	batchResults.On("Close").Return(nil)
	conn := new(mockPostgresConn)
	conn.On("SendBatch", mock.Anything, mock.Anything).Return(batchResults)

	record := &FireEmsCall{
		ReceivedDttm: time.Date(2025, 1, 1, 13, 14, 15, 0, time.UTC),
		Lat:          37.786358,
		Long:         -122.41983,
	}
	payload, _ := record.Marshal()

	messages := []kafka.Message{
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
		{Headers: []kafka.Header{{Key: SchemaNameHeader, Value: []byte(SchemaNameFireEMSCall)}}, Value: payload},
	}

	writer := NewAggregateWriter(conn, NewBucketer(timePrecision, geohashPrecision))
	err := writer.Write(context.Background(), messages)

	assert.Nil(t, err)
	conn.AssertNumberOfCalls(t, "SendBatch", 1)
	batchResults.AssertNumberOfCalls(t, "Close", 1)
}
