package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"
	"time"
)

const (
	DefaultTimePrecision = time.Minute
	DefaultGeoPrecision  = 7
	MinGeoPrecision      = 1
	MaxGeoPrecision      = 7
	timestampLayout      = "2006-01-02T15:04Z"
)

var (
	ErrInvalidTimePrecision = errors.New("Invalid time precision")
	ErrInvalidGeoPrecision  = errors.New("Invalid geohash precision")
)

func GetParam[T any](params url.Values, name string, defaultValue T, parse func(string) (T, error)) (T, error) {
	value := params.Get(name)
	if value == "" {
		return defaultValue, nil
	}
	return parse(value)
}

func ParseTimestamp(s string) (time.Time, error) {
	return time.Parse(timestampLayout, s)
}

func ParseTimePrecision(s string) (precision time.Duration, err error) {
	acceptedValues := map[string]time.Duration{
		"1m":  time.Minute,
		"15m": time.Duration(15) * time.Minute,
		"1h":  time.Hour,
		"6h":  time.Duration(6) * time.Hour,
		"12h": time.Duration(12) * time.Hour,
		"24h": time.Duration(24) * time.Hour,
	}

	precision, ok := acceptedValues[s]
	if !ok {
		err = ErrInvalidTimePrecision
	}

	return
}

func ParseGeoPrecision(s string) (int, error) {
	precision, err := strconv.Atoi(s)
	if err != nil {
		return precision, err
	}

	if precision < MinGeoPrecision || precision > MaxGeoPrecision {
		return precision, ErrInvalidGeoPrecision
	}

	return precision, nil
}

type AggregatesReqParams struct {
	StartTime     time.Time
	EndTime       time.Time
	TimePrecision time.Duration
	GeoPrecision  int
}

func SetDefaultEndTime(t time.Time, now func() time.Time) time.Time {
	if t.IsZero() {
		return now().Round(time.Minute).UTC()
	}
	return t
}

func GetAggregatesReqParams(params url.Values) (p AggregatesReqParams, err error) {
	p.StartTime, err = GetParam(params, "start_time", time.Time{}, ParseTimestamp)
	if err != nil {
		return
	}

	endTime, err := GetParam(params, "end_time", time.Time{}, ParseTimestamp)
	if err != nil {
		return
	}

	p.EndTime = SetDefaultEndTime(endTime, time.Now)

	p.TimePrecision, err = GetParam(params, "time_precision", DefaultTimePrecision, ParseTimePrecision)
	if err != nil {
		return
	}

	p.GeoPrecision, err = GetParam(params, "geo_precision", DefaultGeoPrecision, ParseGeoPrecision)
	return
}

type Aggregate struct {
	OccurredAt time.Time `json:"occurred_at"`
	Geohash    string    `json:"geohash"`
	Count      int32     `json:"count"`
}

func EncodeAggregates(records []Aggregate, w io.Writer) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(records)
}

func DecodeAggregates(s string) ([]Aggregate, error) {
	var records []Aggregate
	err := json.Unmarshal([]byte(s), &records)
	return records, err
}

func DecodeAggregatesFromReader(r io.Reader) ([]Aggregate, error) {
	var records []Aggregate
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&records)
	return records, err
}
