package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRepo struct{}

func (m *mockRepo) GetAggregateRows(ctx context.Context, startTime, endtime time.Time) ([]AggregateRow, error) {
	return []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
		{OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
	}, nil
}

func TestGetAggregatesHandler(t *testing.T) {
	repo := &mockRepo{}
	handler := MakeGetAggregatesHandler(context.Background(), repo)

	req := httptest.NewRequest(http.MethodGet, "/aggregates", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	result := w.Result()
	defer result.Body.Close()

	require.Equal(t, http.StatusOK, result.StatusCode)

	data, err := ioutil.ReadAll(result.Body)
	require.Nil(t, err)

	var actual []Aggregate
	err = json.Unmarshal(data, &actual)
	require.Nil(t, err)

	assert.EqualValues(t, []Aggregate{
		{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
		{OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
	}, actual)
}

func TestGetAggregatesHandlerWhenIncorrectMethod(t *testing.T) {
	repo := &mockRepo{}
	handler := MakeGetAggregatesHandler(context.Background(), repo)

	req := httptest.NewRequest(http.MethodPost, "/aggregates", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	result := w.Result()
	defer result.Body.Close()

	require.Equal(t, http.StatusMethodNotAllowed, result.StatusCode)
}
