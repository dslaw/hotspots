package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/mmcloughlin/geohash"
	kafka "github.com/segmentio/kafka-go"
	kafkaProtocol "github.com/segmentio/kafka-go/protocol"
)

const SchemaNameHeader = "schema_name"

// GetSchemaName extracts the name of message's schema and returns it if found.
func GetSchemaName(headers []kafkaProtocol.Header, schemaNameHeader string) (string, error) {
	for _, header := range headers {
		if header.Key == schemaNameHeader {
			return string(header.Value), nil
		}
	}
	return "", ErrNoSchemaNameHeader
}

// Consumable provides methods to unmarshal a Kafka message and get data
// relevant for aggregation.
type Consumable interface {
	// Unmarshal decodes the message into the receiver.
	Unmarshal([]byte) error
	// Coordinates returns the latitude and longitude, respectively.
	Coordinates() (float64, float64)
	// Timestamp returns the Unix time of when the incident occurred.
	// NB: The returned time is timezone-naive.
	Timestamp() time.Time
}

// NewRecord creates an empty record based on the given schema name.
func NewRecord(schemaName string) (Consumable, error) {
	var record Consumable

	if schemaName == "a311_case" {
		record = &A311Case{}
	} else if schemaName == "fire_incident" {
		record = &FireIncident{}
	} else {
		return record, ErrUnrecognizedSchema
	}

	return record, nil
}

// Decodable provides a method to decode a Kafka message into a pointer to
// a record.
type Decodable interface {
	DecodeMessage(kafka.Message, string) (Consumable, error)
}

type MessageDecoder struct{}

// DecodeMessage decodes the message and returns a populated record, using the
// message headers to interpret the payload.
func (d *MessageDecoder) DecodeMessage(message kafka.Message, schemaNameHeader string) (Consumable, error) {
	schemaName, err := GetSchemaName(message.Headers, SchemaNameHeader)
	if err != nil {
		return nil, err
	}

	record, err := NewRecord(schemaName)
	if err != nil {
		return nil, err
	}

	err = record.Unmarshal(message.Value)
	return record, err
}

// RawWriter writes data directly (i.e. as it is received) to a data sink.
type RawWriter struct {
	decoder Decodable
}

func NewRawWriter(decoder Decodable) *RawWriter {
	return &RawWriter{decoder: decoder}
}

// Write writes messages to the data sink.
// NB: Any message which cannot be decoded will be dropped.
func (w *RawWriter) Write(_ context.Context, messages []kafka.Message) error {
	for _, message := range messages {
		record, err := w.decoder.DecodeMessage(message, SchemaNameHeader)
		if err != nil {
			slog.Error("Error decoding message", "message_headers", message.Headers, "error", err)
			continue
		}

		slog.Info("Message", "message", record)
	}

	return nil
}

// BucketTime rounds the given time to `precision`, such that the given time
// occurred no later than the returned time.
func BucketTime(t time.Time, precision time.Duration) time.Time {
	truncated := t.Truncate(precision)
	if truncated == t {
		return truncated
	}
	return truncated.Add(precision)
}

type Bucket struct {
	Timestamp int64
	Geohash   string
}

// AggregateWriter aggregates/buckets messages by time and location of incident
// and writes the counts to the data sink.
type AggregateWriter struct {
	TimePrecision    time.Duration
	GeohashPrecision uint
	decoder          Decodable
}

func NewAggregateWriter(timePrecision time.Duration, geohashPrecision uint, decoder Decodable) *AggregateWriter {
	if timePrecision <= 0 {
		return nil
	}
	if geohashPrecision == 0 {
		return nil
	}
	return &AggregateWriter{TimePrecision: timePrecision, GeohashPrecision: geohashPrecision, decoder: decoder}
}

// MakeBucket instantiates a Bucket for the given record and precision
// parameters.
func MakeBucket(record Consumable, timePrecision time.Duration, geohashPrecision uint) Bucket {
	long, lat := record.Coordinates()
	ts := record.Timestamp()
	geohash := geohash.EncodeWithPrecision(lat, long, geohashPrecision)
	timestamp := BucketTime(ts, timePrecision).Unix()
	return Bucket{Timestamp: timestamp, Geohash: geohash}
}

// Aggregate aggregates/buckets messages by time and location of incident and
// returns the counts by bucket.
func (w *AggregateWriter) Aggregate(messages []kafka.Message) map[Bucket]int {
	bucketCounts := make(map[Bucket]int)
	for _, message := range messages {
		record, err := w.decoder.DecodeMessage(message, SchemaNameHeader)
		if err != nil {
			slog.Error("Error decoding message", "message_headers", message.Headers, "error", err)
			continue
		}

		bucket := MakeBucket(record, w.TimePrecision, w.GeohashPrecision)

		count, ok := bucketCounts[bucket]
		if !ok {
			count = 0
		}

		bucketCounts[bucket] = count + 1
	}

	return bucketCounts
}

// Write aggregates/buckets messages by time and location of incident and writes
// the counts to the data sink.
// NB: Any message which cannot be decoded will be dropped.
func (w *AggregateWriter) Write(ctx context.Context, messages []kafka.Message) error {
	bucketCounts := w.Aggregate(messages)
	slog.Info("Aggregated counts", "counts", bucketCounts)
	return nil
}
