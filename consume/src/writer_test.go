package main

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
	kafkaProtocol "github.com/segmentio/kafka-go/protocol"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetSchemaName(t *testing.T) {
	type testCase struct {
		Headers        []kafkaProtocol.Header
		ExpectedHeader string
		ExpectedError  error
	}

	testCases := []testCase{
		{
			Headers:        []kafkaProtocol.Header{{Key: "schema_name_header", Value: []byte("schema_name")}},
			ExpectedHeader: "schema_name",
			ExpectedError:  nil,
		}, {
			Headers:        []kafkaProtocol.Header{{Key: "something_else", Value: []byte("other_thing")}},
			ExpectedHeader: "",
			ExpectedError:  ErrNoSchemaNameHeader,
		},
	}
	for _, testCase := range testCases {
		actualValue, err := GetSchemaName(testCase.Headers, "schema_name_header")
		if testCase.ExpectedError != nil {
			assert.ErrorIs(t, testCase.ExpectedError, err)
		} else {
			assert.Nil(t, err)
		}
		assert.Equal(t, testCase.ExpectedHeader, actualValue)
	}
}

func TestNewRecord(t *testing.T) {
	type testCase struct {
		SchemaName    string
		ExpectedError bool
		ExpectedType  interface{}
	}

	testCases := []testCase{
		{
			SchemaName:    "unknown",
			ExpectedError: true,
		}, {
			SchemaName:    "a311_case",
			ExpectedError: false,
			ExpectedType:  &A311Case{},
		}, {
			SchemaName:    "fire_incident",
			ExpectedError: false,
			ExpectedType:  &FireIncident{},
		},
	}
	for _, testCase := range testCases {
		actualValue, err := NewRecord(testCase.SchemaName)
		if testCase.ExpectedError {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
			assert.Empty(t, actualValue)
			assert.IsType(t, testCase.ExpectedType, actualValue)
		}
	}
}

type testRecord struct{}

const expectedGeohash = "u178ke77e"         // Precision of 9.
const expectedTimestamp = int64(1735736700) // Minute precision.

func (r *testRecord) Coordinates() (float64, float64) {
	return 5.124242, 52.09367
}

func (r *testRecord) Timestamp() time.Time {
	t, _ := time.Parse(time.DateTime, "2025-01-01 13:04:05")
	return t
}

func (r *testRecord) Unmarshal(b []byte) error {
	return nil
}

type mockMessageDecoder struct {
	mock.Mock
}

func (m *mockMessageDecoder) DecodeMessage(message kafka.Message, schemaNameHeader string) (Consumable, error) {
	args := m.Called(message, schemaNameHeader)
	return args.Get(0).(*testRecord), args.Error(1)
}

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
