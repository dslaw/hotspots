package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMakeEndTimeParam(t *testing.T) {
	endTime := time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC)
	actual := MakeEndTimeParam(endTime)
	assert.Equal(t, endTime, actual)
}

func TestMakeEndTimeParamWhenMissing(t *testing.T) {
	endTime := time.Time{}
	now := time.Now()
	out := MakeEndTimeParam(endTime)
	assert.Greater(t, out, now)
}

func TestMapAggregateRows(t *testing.T) {
	rows := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
	}
	expected := []Aggregate{
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
	}

	actual := MapAggregateRows(rows)
	assert.Equal(t, expected, actual)
}
