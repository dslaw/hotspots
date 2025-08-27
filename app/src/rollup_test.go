package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBucketTime(t *testing.T) {
	precision := time.Hour
	expected := time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC)

	for _, timestamp := range []time.Time{
		time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC),
	} {
		actual := BucketTime(timestamp, precision)
		assert.Equal(t, expected, actual)
	}
}

func TestBucketGeo(t *testing.T) {
	geohash := "abcdefg"
	precision := 5
	actual := BucketGeo(geohash, precision)
	assert.Equal(t, "abcde", actual)
}

func TestRollup(t *testing.T) {
	timePrecision := time.Hour
	geoPrecision := 6
	records := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 13, 8, 0, 0, time.UTC), Geohash: "abcde21", Count: 2},
		{OccurredAt: time.Date(2025, 1, 1, 13, 9, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 15, 0, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
	}
	expected := []Aggregate{
		{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcde1", Count: 2},
		{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcde2", Count: 2},
		{OccurredAt: time.Date(2025, 1, 1, 15, 0, 0, 0, time.UTC), Geohash: "abcde1", Count: 1},
	}

	actual := Rollup(records, timePrecision, geoPrecision)
	assert.Equal(t, expected, actual)
}
