package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mmcloughlin/geohash"
	"github.com/segmentio/kafka-go"
)

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
	Timestamp time.Time
	Geohash   string
}

type SendBatcher interface {
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

// AggregateWriter aggregates/buckets messages by time and location of incident
// and writes the counts to the data sink.
type AggregateWriter struct {
	TimePrecision    time.Duration
	GeohashPrecision uint
	conn             SendBatcher
}

func NewAggregateWriter(timePrecision time.Duration, geohashPrecision uint, conn SendBatcher) *AggregateWriter {
	if timePrecision <= 0 {
		panic("Time precision must be positive")
	}
	if geohashPrecision == 0 {
		panic("Geohash precision must be positive")
	}
	return &AggregateWriter{TimePrecision: timePrecision, GeohashPrecision: geohashPrecision, conn: conn}
}

// MakeBucket instantiates a Bucket for the given record and precision
// parameters.
func MakeBucket(record ProcessableRecord, timePrecision time.Duration, geohashPrecision uint) *Bucket {
	coordinates := record.Coordinates()
	if coordinates == nil {
		return nil
	}

	ts := record.Timestamp()
	geohash := geohash.EncodeWithPrecision(float64(coordinates.Latitude), float64(coordinates.Longitude), geohashPrecision)
	timestamp := BucketTime(ts, timePrecision)
	return &Bucket{Timestamp: timestamp, Geohash: geohash}
}

// Aggregate aggregates/buckets messages by time and location of incident and
// returns the counts by bucket.
func (w *AggregateWriter) Aggregate(messages []kafka.Message) map[Bucket]int {
	bucketCounts := make(map[Bucket]int)
	for record := range DecodeMessages(SchemaNameHeader, messages) {
		bucket := MakeBucket(record, w.TimePrecision, w.GeohashPrecision)
		if bucket == nil {
			continue
		}

		count, ok := bucketCounts[*bucket]
		if !ok {
			count = 0
		}

		bucketCounts[*bucket] = count + 1
	}

	return bucketCounts
}

const insertAggregateRecordStmt = "insert into aggregate_buckets (occurred_at, geo_id, incident_count) values ($1, $2, $3)"

// WriteAggregateRecords writes the given counts to the data sink.
func (w *AggregateWriter) WriteAggregateRecords(ctx context.Context, bucketCounts map[Bucket]int) error {
	batch := &pgx.Batch{}
	for bucket, count := range bucketCounts {
		batch.Queue(insertAggregateRecordStmt, bucket.Timestamp, bucket.Geohash, count)
	}
	br := w.conn.SendBatch(ctx, batch)
	return br.Close()
}

// Write aggregates/buckets messages by time and location of incident and writes
// the counts to the data sink.
// NB: Any message which cannot be decoded will be dropped.
func (w *AggregateWriter) Write(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}

	bucketCounts := w.Aggregate(messages)
	return w.WriteAggregateRecords(ctx, bucketCounts)
}
