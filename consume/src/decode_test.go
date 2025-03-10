package main

import (
	"testing"

	"github.com/segmentio/kafka-go"
	kafkaProtocol "github.com/segmentio/kafka-go/protocol"
	"github.com/stretchr/testify/assert"
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
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Empty(t, actualValue)
			assert.IsType(t, testCase.ExpectedType, actualValue)
		}
	}
}

func TestDecodeMessage(t *testing.T) {
	record := &A311Case{
		ServiceRequestID: 100,
		Address:          "123 Front Street",
	}
	data, _ := record.Marshal()

	actual, err := DecodeMessage(data, SchemaName311Case)
	assert.Nil(t, err)
	assert.EqualValues(t, record, actual)
}

func TestDecodeMessages(t *testing.T) {
	record := &A311Case{
		ServiceRequestID: 100,
		Address:          "123 Front Street",
	}
	data, _ := record.Marshal()

	messages := []kafka.Message{
		// Processable message.
		{
			Headers: []kafkaProtocol.Header{{Key: SchemaNameHeader, Value: []byte(SchemaName311Case)}},
			Value:   data,
		},
		// Message without a schema name header. Should be dropped.
		{
			Headers: []kafkaProtocol.Header{},
			Value:   []byte(""),
		},
		// Message with an unrecognized schema name. Should be dropped.
		{
			Headers: []kafkaProtocol.Header{{Key: SchemaNameHeader, Value: []byte("unknown")}},
			Value:   []byte(""),
		},
		// Another processable message.
		{
			Headers: []kafkaProtocol.Header{{Key: SchemaNameHeader, Value: []byte(SchemaName311Case)}},
			Value:   data,
		},
	}

	actual := make([]*A311Case, 0)
	for r := range DecodeMessages(SchemaNameHeader, messages) {
		actual = append(actual, r.(*A311Case))
	}

	assert.Equal(t, 2, len(actual))
	assert.EqualValues(t, record, actual[0])
	assert.EqualValues(t, record, actual[1])
}
