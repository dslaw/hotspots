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
	AcquiredDataTmpDir     string
	AcquiredDataDstDir     string
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

	bufferSizeString, ok := os.LookupEnv("BUFFER_SIZE")
	if !ok {
		return nil, false
	}
	bufferSize, err := strconv.ParseInt(bufferSizeString, 10, 32)
	if err != nil {
		return nil, false
	}
	config.BufferSize = int(bufferSize)

	config.FlushInterval, ok = LookupDuration("FLUSH_INTERVAL")
	if !ok {
		return nil, false
	}

	config.BucketTimePrecision, ok = LookupDuration("BUCKET_TIME_PRECISION")
	if !ok {
		return nil, false
	}

	bucketGeohashPrecisionString, ok := os.LookupEnv("BUCKET_GEOHASH_PRECISION")
	bucketGeohashPrecision, err := strconv.ParseUint(bucketGeohashPrecisionString, 10, 32)
	config.BucketGeohashPrecision = uint(bucketGeohashPrecision)

	config.ConsumerType, ok = os.LookupEnv("CONSUMER_TYPE")
	if !ok {
		return nil, false
	}

	config.AggregatesDatabaseURL, ok = os.LookupEnv("AGGREGATES_DB_URL")
	if !ok {
		return nil, false
	}

	config.AcquiredDataTmpDir, ok = os.LookupEnv("ACQUIRED_DATA_TMP_DIR")
	if !ok {
		return nil, false
	}

	config.AcquiredDataDstDir, ok = os.LookupEnv("ACQUIRED_DATA_DST_DIR")
	if !ok {
		return nil, false
	}

	return config, true
}
