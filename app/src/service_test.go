package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAggregatesServiceGetAggregatesWhenCacheHit(t *testing.T) {
	records := []Aggregate{
		{OccurredAt: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
	}

	repo := new(mockRepo)

	cache := new(mockCache)
	cache.On("Get", mock.Anything, mock.Anything).Return(records, nil)

	service := NewAggregatesService(repo, cache)
	ctx := context.Background()
	params := AggregatesReqParams{TimePrecision: DefaultTimePrecision, GeoPrecision: DefaultGeoPrecision}
	actual, err := service.GetAggregates(ctx, params)

	assert.Nil(t, err)
	assert.Equal(t, records, actual)
	cache.AssertCalled(t, "Get", ctx, params)
}

func TestAggregatesServiceGetAggregatesWhenCacheMissOrUnavailable(t *testing.T) {
	rows := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
	}
	records := []Aggregate{
		{OccurredAt: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
	}

	repo := new(mockRepo)
	repo.On("GetAggregateRows", mock.Anything, mock.Anything, mock.Anything).Return(rows, nil)

	cache := new(mockCache)
	cache.On("Get", mock.Anything, mock.Anything).Return([]Aggregate{}, ErrNoSuchKey)
	cache.On("Set", mock.Anything, mock.Anything, records).Return(nil)

	service := NewAggregatesService(repo, cache)

	ctx := context.Background()
	params := AggregatesReqParams{TimePrecision: DefaultTimePrecision, GeoPrecision: DefaultGeoPrecision}
	actual, err := service.GetAggregates(ctx, params)

	assert.Nil(t, err)
	assert.Equal(t, records, actual)
	cache.AssertCalled(t, "Get", ctx, params)
	repo.AssertCalled(t, "GetAggregateRows", ctx, params.StartTime, params.EndTime)
	cache.AssertCalled(t, "Set", ctx, params, records)
}

func TestAggregatesServiceGetAggregatesWhenDatabaseUnavailable(t *testing.T) {
	databaseErr := errors.New("Database error")

	repo := new(mockRepo)
	repo.On("GetAggregateRows", mock.Anything, mock.Anything, mock.Anything).Return([]AggregateRow{}, databaseErr)

	cache := new(mockCache)
	cache.On("Get", mock.Anything, mock.Anything).Return([]Aggregate{}, ErrNoSuchKey)
	cache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	service := NewAggregatesService(repo, cache)

	ctx := context.Background()
	params := AggregatesReqParams{TimePrecision: DefaultTimePrecision, GeoPrecision: DefaultGeoPrecision}
	_, err := service.GetAggregates(ctx, params)

	assert.ErrorIs(t, databaseErr, err)
	cache.AssertCalled(t, "Get", ctx, params)
	repo.AssertCalled(t, "GetAggregateRows", ctx, params.StartTime, params.EndTime)
	cache.AssertNotCalled(t, "Set")
}
