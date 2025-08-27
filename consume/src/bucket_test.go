package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

func TestBucketLocation(t *testing.T) {
	geohashPrecision := uint(9)
	latitude := float32(52.09367)
	longitude := float32(5.124242)
	expected := "u178ke77e"

	actual := BucketLocation(longitude, latitude, geohashPrecision)
	assert.Equal(t, expected, actual)
}

func TestBucketerMakeBucket(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)
	bucketer := NewBucketer(timePrecision, geohashPrecision)

	record := &FireEmsCall{
		ReceivedDttm: time.Date(2025, 1, 1, 13, 4, 5, 0, time.UTC),
		Lat:          52.09367,
		Long:         5.124242,
	}
	expectedGeohash := "u178ke77e"
	expectedTimestamp := time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC)

	actual, ok := bucketer.MakeBucket(record)
	assert.True(t, ok)
	assert.Equal(t, expectedTimestamp, actual.Timestamp)
	assert.Equal(t, expectedGeohash, actual.Geohash)
}

func TestBucketerMakeBucketWhenNoCoordinates(t *testing.T) {
	timePrecision, _ := time.ParseDuration("1m")
	geohashPrecision := uint(9)
	bucketer := NewBucketer(timePrecision, geohashPrecision)

	record := &PoliceIncident{
		IncidentDatetime: time.Date(2025, 1, 1, 13, 4, 5, 0, time.UTC),
		Latitude:         nil,
		Longitude:        nil,
	}

	_, ok := bucketer.MakeBucket(record)
	assert.False(t, ok)
}
