package main

import (
	"iter"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/segmentio/kafka-go"
	kafkaProtocol "github.com/segmentio/kafka-go/protocol"
)

const (
	SchemaNameHeader         = "schema_name"
	SchemaName311Case        = "a311_case"
	SchemaNameFireEMSCall    = "fire_ems_call"
	SchemaNameFireIncident   = "fire_incident"
	SchemaNamePoliceIncident = "police_incident"
	SchemaNameTrafficCrash   = "traffic_crash"
)

// GetSchemaName extracts the name of message's schema and returns it if found.
func GetSchemaName(headers []kafkaProtocol.Header, schemaNameHeader string) (string, error) {
	for _, header := range headers {
		if header.Key == schemaNameHeader {
			return string(header.Value), nil
		}
	}
	return "", ErrNoSchemaNameHeader
}

// ProcessableRecord provides methods to unmarshal a Kafka message and get data
// relevant for aggregation.
type ProcessableRecord interface {
	// Schema returns the record's Avro schema.
	Schema() avro.Schema
	// SchemaName returns the name of the record's Avro schema.
	SchemaName() string
	// Unmarshal decodes the message into the receiver.
	Unmarshal([]byte) error
	// Coordinates returns the coordinates of where the incident occurred.
	Coordinates() *Coordinates
	// Timestamp returns the Unix time of when the incident occurred.
	// NB: The returned time is timezone-naive.
	Timestamp() time.Time
}

// NewRecord creates an empty record based on the given schema name.
func NewRecord(schemaName string) (ProcessableRecord, error) {
	var record ProcessableRecord

	switch schemaName {
	case SchemaName311Case:
		record = &A311Case{}
	case SchemaNameFireEMSCall:
		record = &FireEmsCall{}
	case SchemaNameFireIncident:
		record = &FireIncident{}
	case SchemaNamePoliceIncident:
		record = &PoliceIncident{}
	case SchemaNameTrafficCrash:
		record = &TrafficCrash{}
	default:
		return record, ErrUnrecognizedSchema
	}

	return record, nil
}

// DecodeMessage decodes the message payload and returns a populated record.
func DecodeMessage(data []byte, schemaName string) (ProcessableRecord, error) {
	record, err := NewRecord(schemaName)
	if err != nil {
		return nil, err
	}

	err = record.Unmarshal(data)
	return record, err
}

// DecodeMessages decodes Kafka messages, using each message's headers to determine
// how to decode the message. Messages which cannot be decoded will be dropped.
func DecodeMessages(schemaNameHeader string, messages []kafka.Message) iter.Seq[ProcessableRecord] {
	return func(yield func(ProcessableRecord) bool) {
		for _, message := range messages {
			schemaName, err := GetSchemaName(message.Headers, schemaNameHeader)
			if err != nil {
				continue
			}

			record, err := DecodeMessage(message.Value, schemaName)
			if err != nil {
				continue
			}

			if !yield(record) {
				return
			}
		}
	}
}
