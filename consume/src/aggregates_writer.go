package main

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

type SendBatcher interface {
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

// AggregateWriter aggregates/buckets messages by time and location of incident
// and writes the counts to the data sink.
type AggregateWriter struct {
	conn     SendBatcher
	bucketer *Bucketer
}

func NewAggregateWriter(conn SendBatcher, bucketer *Bucketer) *AggregateWriter {
	return &AggregateWriter{conn: conn, bucketer: bucketer}
}

// Aggregate aggregates/buckets messages by time and location of incident and
// returns the counts by bucket.
func (w *AggregateWriter) Aggregate(messages []kafka.Message) map[Bucket]int {
	bucketCounts := make(map[Bucket]int)
	for record := range DecodeMessages(SchemaNameHeader, messages) {
		bucket, ok := w.bucketer.MakeBucket(record)
		if !ok {
			continue
		}

		count, ok := bucketCounts[bucket]
		if !ok {
			count = 0
		}

		bucketCounts[bucket] = count + 1
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
