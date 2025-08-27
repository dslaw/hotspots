package main

import (
	"os"
	"strconv"
	"time"
)

const (
	RawConsumerType       = "raw"
	AggregateConsumerType = "aggregates"
)

func LookupDuration(name string) (time.Duration, bool) {
	s, ok := os.LookupEnv(name)
	if !ok {
		return time.Duration(0), false
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return d, false
	}

	return d, true
}

func LookupInt(name string) (int, bool) {
	s, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}

	value, err := strconv.ParseInt(s, 10, 32)
	return int(value), err == nil
}

func LookupUint(name string) (uint, bool) {
	s, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}

	value, err := strconv.ParseUint(s, 10, 32)
	return uint(value), err == nil
}

type Config struct {
	ConsumerType           string
	Topic                  string
	BrokerURL              string
	ConsumerGroupID        string
	BufferSize             int
	FlushInterval          time.Duration
	BucketTimePrecision    time.Duration
	BucketGeohashPrecision uint
	AggregatesDatabaseURL  string
	WarehouseURL           string
	AppURL                 string
	HttpRequestTimeout     time.Duration
	HttpRequestRetries     int
	HttpRequestBackoff     time.Duration
}

func NewConfig() (*Config, bool) {
	config := &Config{}
	ok := true

	config.Topic, ok = os.LookupEnv("KAFKA_TOPIC")
	if !ok {
		return nil, false
	}

	config.BrokerURL, ok = os.LookupEnv("KAFKA_URL")
	if !ok {
		return nil, false
	}

	config.ConsumerGroupID, ok = os.LookupEnv("CONSUMER_GROUP_ID")
	if !ok {
		return nil, false
	}

	config.BufferSize, ok = LookupInt("BUFFER_SIZE")
	if !ok {
		return nil, false
	}

	config.FlushInterval, ok = LookupDuration("FLUSH_INTERVAL")
	if !ok {
		return nil, false
	}

	config.BucketTimePrecision, ok = LookupDuration("BUCKET_TIME_PRECISION")
	if !ok {
		return nil, false
	}

	config.BucketGeohashPrecision, ok = LookupUint("BUCKET_GEOHASH_PRECISION")
	if !ok {
		return nil, false
	}

	config.ConsumerType, ok = os.LookupEnv("CONSUMER_TYPE")
	if !ok {
		return nil, false
	}

	config.AggregatesDatabaseURL, ok = os.LookupEnv("AGGREGATES_DB_URL")
	if !ok {
		return nil, false
	}

	config.WarehouseURL, ok = os.LookupEnv("WAREHOUSE_URL")
	if !ok {
		return nil, false
	}

	config.AppURL, ok = os.LookupEnv("APP_URL")
	if !ok {
		return nil, false
	}

	config.HttpRequestTimeout, ok = LookupDuration("HTTP_REQUEST_TIMEOUT")
	if !ok {
		return nil, false
	}

	config.HttpRequestRetries, ok = LookupInt("HTTP_REQUEST_RETRIES")
	if !ok {
		return nil, false
	}

	config.HttpRequestBackoff, ok = LookupDuration("HTTP_REQUEST_BACKOFF")
	if !ok {
		return nil, false
	}

	return config, true
}
