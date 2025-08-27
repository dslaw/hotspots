package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Poster interface {
	PostAggregates(context.Context, map[Bucket]int) error
}

// AggregateWriter aggregates/buckets messages by time and location of incident
// and writes the counts to the data sink.
type AggregateWriter struct {
	client   Poster
	bucketer *Bucketer
}

func NewAggregateWriter(client Poster, bucketer *Bucketer) *AggregateWriter {
	return &AggregateWriter{client: client, bucketer: bucketer}
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

// WriteAggregateRecords writes the given counts to the data sink.
func (w *AggregateWriter) WriteAggregateRecords(ctx context.Context, bucketCounts map[Bucket]int) error {
	return w.client.PostAggregates(ctx, bucketCounts)
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
