package main

import (
	"time"

	"github.com/mmcloughlin/geohash"
)

type Bucket struct {
	Timestamp time.Time
	Geohash   string
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

// BucketLocation assigns the geographic coordinates to a spatial bucket.
func BucketLocation(longitude, latitude float32, geohashPrecision uint) string {
	return geohash.EncodeWithPrecision(float64(latitude), float64(longitude), geohashPrecision)
}

// Bucketer provides a method to assign a record to temporal and spatial
// buckets.
type Bucketer struct {
	TimePrecision    time.Duration
	GeohashPrecision uint
}

func NewBucketer(timePrecision time.Duration, geohashPrecision uint) *Bucketer {
	if timePrecision <= 0 {
		panic("Time precision must be positive")
	}
	if geohashPrecision == 0 {
		panic("Geohash precision must be positive")
	}
	return &Bucketer{TimePrecision: timePrecision, GeohashPrecision: geohashPrecision}
}

// BucketRecord assigns temporal and spatial buckets to the given record.
func (b *Bucketer) MakeBucket(record ProcessableRecord) (Bucket, bool) {
	coordinates := record.Coordinates()
	if coordinates == nil {
		return Bucket{}, false
	}

	ts := record.Timestamp()
	geohash := BucketLocation(coordinates.Longitude, coordinates.Latitude, b.GeohashPrecision)
	timestamp := BucketTime(ts, b.TimePrecision)
	return Bucket{Timestamp: timestamp, Geohash: geohash}, true
}
