package main

import (
	"net/url"
	"time"
)

const timestampLayout = "2006-01-02T15:04:05Z"

func ParseTimestampQueryParam(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}

	return time.Parse(timestampLayout, s)
}

func GetTimeQueryParams(params url.Values) (startTime time.Time, endTime time.Time, err error) {
	startTime, err = ParseTimestampQueryParam(params.Get("start_time"))
	if err != nil {
		return
	}

	endTime, err = ParseTimestampQueryParam(params.Get("end_time"))
	return
}
