package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type HandlersTestSuite struct {
	suite.Suite
	Conn *pgxpool.Pool
}

func (suite *HandlersTestSuite) SetupSuite() {
	ctx := context.Background()
	testDatabaseURL := os.Getenv("AGGREGATES_TEST_DB_URL_LOCAL")
	dbConfig, _ := pgxpool.ParseConfig(testDatabaseURL)
	dbConfig.MaxConns = 1
	conn, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		assert.FailNow(suite.T(), fmt.Sprintf("Unable to connect to test database: %s", err))
	}

	suite.Conn = conn
}

func (suite *HandlersTestSuite) TearDownSuite() {
	defer suite.Conn.Close()

	if err := DeleteTestData(context.Background(), suite.Conn); err != nil {
		assert.FailNow(suite.T(), fmt.Sprintf("Error deleting test data: %s", err))
	}
}

func WriteTestData(ctx context.Context, conn *pgxpool.Pool, records []AggregateRow) error {
	for _, record := range records {
		if _, err := conn.Exec(
			ctx,
			"insert into aggregate_buckets (occurred_at, geo_id, incident_count) values ($1, $2, $3)",
			record.OccurredAt,
			record.Geohash,
			record.Count,
		); err != nil {
			return err
		}
	}

	return nil
}

func DeleteTestData(ctx context.Context, conn *pgxpool.Pool) error {
	_, err := conn.Exec(ctx, "delete from aggregate_buckets")
	return err
}

func (suite *HandlersTestSuite) TestGetAggregatesHandler() {
	t := suite.T()
	repo := &Repo{conn: suite.Conn}

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
		}, {
			// Filter to a time window.
			RequestURL: "/aggregates?start_time=2025-01-02T00:00Z&end_time=2025-01-06T00:00Z",
			Records: []AggregateRow{
				{OccurredAt: time.Date(2025, 1, 1, 13, 5, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
				{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
				{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
				{OccurredAt: time.Date(2025, 1, 7, 15, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 3},
			},
			Expected: []Aggregate{
				{OccurredAt: time.Date(2025, 1, 3, 4, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 4},
			},
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

	for idx, testCase := range testCases {
		WriteTestData(context.Background(), suite.Conn, testCase.Records)

		cache := new(mockCache)
		cache.On("Get", mock.Anything, mock.Anything).Return([]Aggregate{}, ErrNoSuchKey)
		cache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		service := NewAggregatesService(repo, cache)
		handler := MakeGetAggregatesHandler(context.Background(), service)

		req := httptest.NewRequest(http.MethodGet, testCase.RequestURL, nil)
		w := httptest.NewRecorder()
		handler(w, req)

		result := w.Result()
		defer result.Body.Close()

		require.Equal(t, http.StatusOK, result.StatusCode)

		data, err := ioutil.ReadAll(result.Body)
		assert.Nil(t, err)

		var actual []Aggregate
		err = json.Unmarshal(data, &actual)
		assert.Nil(t, err)

		assert.Equal(t, testCase.Expected, actual, idx)

		DeleteTestData(context.Background(), suite.Conn)
	}
}

func (suite *HandlersTestSuite) TestInsertAggregatesHandler() {
	t := suite.T()
	repo := &Repo{conn: suite.Conn}

	records := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 13, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 14, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
	}

	WriteTestData(context.Background(), suite.Conn, records)
	defer DeleteTestData(context.Background(), suite.Conn)

	cache := new(mockCache)
	cache.On("Get", mock.Anything, mock.Anything).Return([]Aggregate{}, ErrNoSuchKey)
	cache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	service := NewAggregatesService(repo, cache)
	handler := MakeInsertAggregatesHandler(context.Background(), service)

	body := strings.NewReader(`[{"occurred_at": "2025-01-14T00:00:00Z", "geohash": "abcdefg", "count": 2}]`)
	req := httptest.NewRequest(http.MethodPost, "/aggregates", body)
	w := httptest.NewRecorder()
	handler(w, req)

	result := w.Result()
	defer result.Body.Close()

	require.Equal(t, http.StatusOK, result.StatusCode)

	rows, err := suite.Conn.Query(context.Background(), "select occurred_at, geo_id, incident_count from aggregate_buckets")
	require.Nil(t, err)
	defer rows.Close()

	actual, err := pgx.CollectRows(rows, pgx.RowToStructByName[AggregateRow])
	require.Nil(t, err)
	expected := append(records,
		AggregateRow{OccurredAt: time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
	)
	assert.Equal(t, expected, actual)
}

func (suite *HandlersTestSuite) TestUpsertAggregatesHandler() {
	t := suite.T()
	repo := &Repo{conn: suite.Conn}

	records := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 13, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 13, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
		{OccurredAt: time.Date(2025, 1, 14, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
	}

	WriteTestData(context.Background(), suite.Conn, records)
	defer DeleteTestData(context.Background(), suite.Conn)

	cache := new(mockCache)
	cache.On("Get", mock.Anything, mock.Anything).Return([]Aggregate{}, ErrNoSuchKey)
	cache.On("Set", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	service := NewAggregatesService(repo, cache)
	handler := MakeUpsertAggregatesHandler(context.Background(), service)

	body := strings.NewReader(`[{"occurred_at": "2025-01-13T01:00:00Z", "geohash": "abcdefg", "count": 2}]`)
	req := httptest.NewRequest(http.MethodPut, "/aggregates", body)
	w := httptest.NewRecorder()
	handler(w, req)

	result := w.Result()
	defer result.Body.Close()

	require.Equal(t, http.StatusOK, result.StatusCode)

	rows, err := suite.Conn.Query(context.Background(), "select occurred_at, geo_id, incident_count from aggregate_buckets")
	require.Nil(t, err)
	defer rows.Close()

	actual, err := pgx.CollectRows(rows, pgx.RowToStructByName[AggregateRow])
	require.Nil(t, err)
	expected := []AggregateRow{
		{OccurredAt: time.Date(2025, 1, 14, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 13, 1, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
	}
	assert.Equal(t, expected, actual)
}

func TestHandlersTestSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping testing in short mode")
	}
	suite.Run(t, new(HandlersTestSuite))
}
