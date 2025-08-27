package main

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetParam(t *testing.T) {
	params := url.Values{}
	params.Set("key", "value")

	actual, err := GetParam(params, "key", "default", func(s string) (string, error) {
		return s, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, "value", actual)
}

func TestGetParamWhenMissingKey(t *testing.T) {
	params := url.Values{}

	actual, err := GetParam(params, "key", "default", func(s string) (string, error) {
		return s, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, "default", actual)
}

func TestParseTimestamp(t *testing.T) {
	expected := time.Date(2025, 1, 1, 5, 5, 0, 0, time.UTC)
	actual, err := ParseTimestamp("2025-01-01T05:05Z")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestParseTimestampWhenInvalidFormat(t *testing.T) {
	_, err := ParseTimestamp("2025-01-01 12:00:00")
	assert.NotNil(t, err)
}

func TestParseTimePrecision(t *testing.T) {
	actual, err := ParseTimePrecision("1m")
	assert.Nil(t, err)
	assert.Equal(t, time.Minute, actual)
}

func TestParseTimePrecisionWhenUnacceptedValue(t *testing.T) {
	_, err := ParseTimePrecision("-66m")
	assert.ErrorIs(t, ErrInvalidTimePrecision, err)
}

func TestParseGeoPrecision(t *testing.T) {
	expected := 1
	actual, err := ParseGeoPrecision("1")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestParseGeoPrecisionWhenUnparsableValue(t *testing.T) {
	_, err := ParseGeoPrecision("one")
	assert.NotNil(t, err)
}

func TestParseGeoPrecisionWhenUnacceptedValue(t *testing.T) {
	for _, s := range []string{"0", "8"} {
		_, err := ParseGeoPrecision(s)
		assert.ErrorIs(t, ErrInvalidGeoPrecision, err)
	}
}

func TestSetDefaultEndTime(t *testing.T) {
	endTime := time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC)
	actual := SetDefaultEndTime(endTime, time.Now)
	assert.Equal(t, endTime, actual)
}

func TestSetDefaultEndTimeWhenMissing(t *testing.T) {
	endTime := time.Time{}
	now := time.Date(2025, 1, 1, 13, 5, 45, 1, time.UTC)
	expected := time.Date(2025, 1, 1, 13, 6, 0, 0, time.UTC)

	actual := SetDefaultEndTime(endTime, func() time.Time { return now })
	assert.Equal(t, expected, actual)
}

func TestGetAggregatesReqParams(t *testing.T) {
	params := url.Values{}
	params.Set("start_time", "2025-01-01T13:00Z")
	params.Set("end_time", "2025-01-01T13:00Z")
	params.Set("time_precision", "15m")
	params.Set("geo_precision", "5")

	actual, err := GetAggregatesReqParams(params)

	assert.Nil(t, err)
	assert.Equal(t, time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), actual.StartTime)
	assert.Equal(t, time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), actual.EndTime)
	assert.Equal(t, time.Duration(15)*time.Minute, actual.TimePrecision)
	assert.Equal(t, 5, actual.GeoPrecision)
}

func TestGetAggregatesReqParamsWhenEmpty(t *testing.T) {
	params := url.Values{}
	actual, err := GetAggregatesReqParams(params)

	assert.Nil(t, err)
	assert.True(t, actual.StartTime.IsZero())
	assert.False(t, actual.EndTime.IsZero())
	assert.Equal(t, DefaultTimePrecision, actual.TimePrecision)
	assert.Equal(t, DefaultGeoPrecision, actual.GeoPrecision)
}
