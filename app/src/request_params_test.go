package main

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseTimestampQueryParam(t *testing.T) {
	expected := time.Date(2025, 1, 1, 5, 5, 0, 0, time.UTC)
	actual, err := ParseTimestampQueryParam("2025-01-01T05:05:00Z")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestParseTimestampQueryParamWhenEmpty(t *testing.T) {
	actual, err := ParseTimestampQueryParam("")
	assert.Nil(t, err)
	assert.Equal(t, time.Time{}, actual)
}

func TestParseTimestampQueryParamWhenInvalidFormat(t *testing.T) {
	_, err := ParseTimestampQueryParam("2025-01-01 12:00:00")
	assert.NotNil(t, err)
}

func TestGetTimeQueryParams(t *testing.T) {
	params := url.Values{}
	params.Set("start_time", "2025-01-01T13:00:00Z")
	params.Set("end_time", "2025-01-01T13:00:00Z")

	actualStartTime, actualEndTime, err := GetTimeQueryParams(params)

	assert.Nil(t, err)
	assert.Equal(t, time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), actualStartTime)
	assert.Equal(t, time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), actualEndTime)
}

func TestGetTimeQueryParamsWhenEmpty(t *testing.T) {
	params := url.Values{}

	actualStartTime, actualEndTime, err := GetTimeQueryParams(params)
	assert.Nil(t, err)
	assert.True(t, actualStartTime.IsZero())
	assert.True(t, actualEndTime.IsZero())
}
