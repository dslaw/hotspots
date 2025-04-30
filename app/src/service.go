package main

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type Repoer interface {
	GetAggregateRows(context.Context, time.Time, time.Time) ([]AggregateRow, error)
}

type Cacher interface {
	Get(context.Context, AggregatesReqParams) ([]Aggregate, error)
	Set(context.Context, AggregatesReqParams, []Aggregate) error
}

type AggregatesService struct {
	repo  Repoer
	cache Cacher
}

func NewAggregatesService(repo Repoer, cache Cacher) *AggregatesService {
	return &AggregatesService{repo: repo, cache: cache}
}

func (s *AggregatesService) GetAggregates(ctx context.Context, params AggregatesReqParams) ([]Aggregate, error) {
	cachedRecords, err := s.cache.Get(ctx, params)

	if err == nil {
		return cachedRecords, nil
	}

	if !errors.Is(err, ErrNoSuchKey) {
		slog.Error("Error reading from cache", "error", err, "params", params)
	}

	rows, err := s.repo.GetAggregateRows(ctx, params.StartTime, params.EndTime)
	if err != nil {
		return []Aggregate{}, err
	}

	records := Rollup(rows, params.TimePrecision, params.GeoPrecision)

	if err := s.cache.Set(ctx, params, records); err != nil {
		slog.Error("Error updating cache", "error", err, "params", params)
	}

	return records, nil
}
