package main

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) GetAggregateRows(ctx context.Context, startTime, endTime time.Time) ([]AggregateRow, error) {
	args := m.Called(ctx, startTime, endTime)
	return args.Get(0).([]AggregateRow), args.Error(1)
}

type mockCache struct {
	mock.Mock
}

func (m *mockCache) Get(ctx context.Context, params AggregatesReqParams) ([]Aggregate, error) {
	args := m.Called(ctx, params)
	return args.Get(0).([]Aggregate), args.Error(1)
}

func (m *mockCache) Set(ctx context.Context, params AggregatesReqParams, records []Aggregate) error {
	args := m.Called(ctx, params, records)
	return args.Error(0)
}
