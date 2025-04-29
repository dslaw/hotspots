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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) GetAggregateRows(ctx context.Context, startTime, endTime time.Time) ([]AggregateRow, error) {
	args := m.Called(ctx, startTime, endTime)
	return args.Get(0).([]AggregateRow), args.Error(1)
}

func TestGetAggregatesHandlerWhenIncorrectMethod(t *testing.T) {
	repo := new(mockRepo)
	handler := MakeGetAggregatesHandler(context.Background(), repo)

	req := httptest.NewRequest(http.MethodPost, "/aggregates", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	result := w.Result()
	defer result.Body.Close()

	require.Equal(t, http.StatusMethodNotAllowed, result.StatusCode)
}

func TestGetAggregatesHandler(t *testing.T) {
	testCases := []struct {
		RequestURL string
		Records    []AggregateRow
		Expected   []Aggregate
	}{
		{
			// No filtering or further aggregation.
			RequestURL: "/aggregates",
			Records: []AggregateRow{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
				{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
				{OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
			},
			Expected: []Aggregate{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
				{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
				{OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
			},
			//}, {
			//    // XXX: Mocking doesn't really work, since the filter is pushed to the database query.
			//    // Filter to a time window.
			//    RequestURL: "/aggregates?start_time=2025-01-02T00:00:00Z&end_time=2025-01-06T00:00:00Z",
			//    Records: []AggregateRow{
			//        {OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
			//        {OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
			//        {OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
			//        {OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
			//    },
			//    Expected: []Aggregate{ {OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 4} },
		}, {
			// Rollup spatial dimension.
			RequestURL: "/aggregates?geo_precision=6",
			Records: []AggregateRow{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde12", Count: 1},
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde21", Count: 1},
			},
			Expected: []Aggregate{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde1", Count: 2},
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde2", Count: 1},
			},
		}, {
			// Rollup temporal dimension.
			RequestURL: "/aggregates?time_precision=1h",
			Records: []AggregateRow{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
				{OccurredAt: time.Date(2025, 1, 1, 13, 8, 0, 0, time.UTC), Geohash: "abcde12", Count: 3},
				{OccurredAt: time.Date(2025, 1, 1, 13, 9, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
			},
			Expected: []Aggregate{
				{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcde11", Count: 2},
				{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcde12", Count: 3},
			},
		}, {
			// Rollup spatial and temporal dimensions.
			RequestURL: "/aggregates?time_precision=1h&geo_precision=6",
			Records: []AggregateRow{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
				{OccurredAt: time.Date(2025, 1, 1, 13, 8, 0, 0, time.UTC), Geohash: "abcde12", Count: 2},
				{OccurredAt: time.Date(2025, 1, 1, 15, 0, 0, 0, time.UTC), Geohash: "abcde11", Count: 1},
			},
			Expected: []Aggregate{
				{OccurredAt: time.Date(2025, 1, 1, 14, 0, 0, 0, time.UTC), Geohash: "abcde1", Count: 3},
				{OccurredAt: time.Date(2025, 1, 1, 15, 0, 0, 0, time.UTC), Geohash: "abcde1", Count: 1},
			},
		},
	}

	for _, testCase := range testCases {
		repo := new(mockRepo)
		repo.On("GetAggregateRows", mock.Anything, mock.Anything, mock.Anything).Return(testCase.Records, nil)
		handler := MakeGetAggregatesHandler(context.Background(), repo)

		req := httptest.NewRequest(http.MethodGet, testCase.RequestURL, nil)
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

		assert.Equal(t, testCase.Expected, actual)
	}
}
