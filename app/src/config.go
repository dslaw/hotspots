package main

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	CachePrefix string
	CacheTTL    time.Duration
	CacheURL    string
	DatabaseURL string
	Port        string
}

func NewConfig() (*Config, error) {
	config := &Config{}
	ok := true

	config.CachePrefix, ok = os.LookupEnv("CACHE_AGGREGATES_PREFIX")
	if !ok {
		return config, fmt.Errorf("Unable to read cache prefix")
	}

	cacheTTLString, ok := os.LookupEnv("CACHE_AGGREGATES_TTL")
	if !ok {
		return config, fmt.Errorf("Unable to read cache ttl")
	}

	cacheTTL, err := time.ParseDuration(cacheTTLString)
	if err != nil {
		return config, err
	}
	config.CacheTTL = cacheTTL

	config.CacheURL, ok = os.LookupEnv("REDIS_URL")
	if !ok {
		return config, fmt.Errorf("Unable to read cache url")
	}

	config.DatabaseURL, ok = os.LookupEnv("AGGREGATES_DB_URL")
	if !ok {
		return config, fmt.Errorf("Unable to read database url")
	}

	config.Port, ok = os.LookupEnv("APP_PORT")
	if !ok {
		return config, fmt.Errorf("Unable to read app port")
	}

	return config, nil
}
