package main

import (
	// "context"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testRecord struct{}

const expectedGeohash = "u178ke77e"         // Precision of 9.
const expectedTimestamp = int64(1735736700) // Minute precision.

func (r *testRecord) Coordinates() *Coordinates {
	return &Coordinates{Longitude: 5.124242, Latitude: 52.09367}
}

func (r *testRecord) Timestamp() time.Time {
	t, _ := time.Parse(time.DateTime, "2025-01-01 13:04:05")
	return t
}

func (r *testRecord) Unmarshal(b []byte) error {
	return nil
}

func (r *testRecord) Schema() avro.Schema {
	return schemaA311Case // Arbitrary schema.
}

func (r *testRecord) SchemaName() string {
	return ""
}

type mockMessageDecoder struct {
	mock.Mock
}

func (m *mockMessageDecoder) DecodeMessage(message kafka.Message, schemaNameHeader string) (ProcessableRecord, error) {
	args := m.Called(message, schemaNameHeader)
	return args.Get(0).(*testRecord), args.Error(1)
}

/*
func TestRawWriterWrite(t *testing.T) {
	message := kafka.Message{Headers: []kafka.Header{}, Value: []byte("abc")}

	mockDecoder := new(mockMessageDecoder)
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrNoSchemaNameHeader).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrUnrecognizedSchema).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()

	writer := NewRawWriter(mockDecoder)
	ctx := context.Background()
	messages := []kafka.Message{message, message, message, message}

	err := writer.Write(ctx, messages)

	assert.Nil(t, err)
	mockDecoder.AssertNumberOfCalls(t, "DecodeMessage", len(messages))
}
*/

func TestBucketTime(t *testing.T) {
	precision, _ := time.ParseDuration("1m")

	type testCase struct {
		Datetime string
		Expected string
	}

	testCases := []testCase{
		{Datetime: "2025-01-01 13:04:00", Expected: "2025-01-01 13:04:00"},
		{Datetime: "2025-01-01 13:04:05", Expected: "2025-01-01 13:05:00"},
		{Datetime: "2025-01-01 13:04:59", Expected: "2025-01-01 13:05:00"},
	}
	for _, testCase := range testCases {
		timestamp, _ := time.Parse(time.DateTime, testCase.Datetime)
		expected, _ := time.Parse(time.DateTime, testCase.Expected)
		actual := BucketTime(timestamp, precision)
		assert.Equal(t, expected, actual)
	}
}

func TestMakeBucket(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)

	record := &testRecord{}
	actual := MakeBucket(record, timePrecision, geohashPrecision)
	assert.Equal(t, expectedTimestamp, actual.Timestamp)
	assert.Equal(t, expectedGeohash, actual.Geohash)
}

/*
func TestAggregateWriterAggregate(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)

	message := kafka.Message{Headers: []kafka.Header{}, Value: []byte("abc")}

	mockDecoder := new(mockMessageDecoder)
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrNoSchemaNameHeader).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrUnrecognizedSchema).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()

	writer := NewAggregateWriter(timePrecision, geohashPrecision, mockDecoder)
	messages := []kafka.Message{message, message, message, message}

	expected := make(map[Bucket]int)
	expected[Bucket{Timestamp: expectedTimestamp, Geohash: expectedGeohash}] = 2

	actual := writer.Aggregate(messages)

	assert.Equal(t, expected, actual)
	mockDecoder.AssertNumberOfCalls(t, "DecodeMessage", len(messages))
}

func TestAggregateWriterWrite(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)

	message := kafka.Message{Headers: []kafka.Header{}, Value: []byte("abc")}

	mockDecoder := new(mockMessageDecoder)
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrNoSchemaNameHeader).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, ErrUnrecognizedSchema).Once()
	mockDecoder.On("DecodeMessage", mock.Anything, SchemaNameHeader).Return(&testRecord{}, nil).Once()

	writer := NewAggregateWriter(timePrecision, geohashPrecision, mockDecoder)
	ctx := context.Background()
	messages := []kafka.Message{message, message, message, message}

	err := writer.Write(ctx, messages)

	assert.Nil(t, err)
	mockDecoder.AssertNumberOfCalls(t, "DecodeMessage", len(messages))
}
*/
