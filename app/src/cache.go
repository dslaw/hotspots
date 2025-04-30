package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNoSuchKey = errors.New("No such key exists")

type Cache struct {
	Prefix string
	TTL    time.Duration
	conn   *redis.Client
}

func NewCacheFromURL(url, prefix string, ttl time.Duration) *Cache {
	opts, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}

	conn := redis.NewClient(opts)
	return &Cache{Prefix: prefix, TTL: ttl, conn: conn}
}

func (c *Cache) Close() error {
	return c.conn.Close()
}

func (c *Cache) MakeKey(params AggregatesReqParams) string {
	return fmt.Sprintf("%s:%s|%s|%s|%d", c.Prefix, params.StartTime, params.EndTime, params.TimePrecision, params.GeoPrecision)
}

func (c *Cache) Get(ctx context.Context, params AggregatesReqParams) ([]Aggregate, error) {
	key := c.MakeKey(params)
	value, err := c.conn.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		return []Aggregate{}, ErrNoSuchKey
	}

	return DecodeAggregates(value)
}

func (c *Cache) Set(ctx context.Context, params AggregatesReqParams, records []Aggregate) error {
	key := c.MakeKey(params)

	var buff bytes.Buffer
	if err := EncodeAggregates(records, &buff); err != nil {
		return err
	}

	return c.conn.Set(ctx, key, buff.String(), c.TTL).Err()
}
