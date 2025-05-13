package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFlattenBucketCounts(t *testing.T) {
	bucketCounts := map[Bucket]int{
		{Timestamp: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg"}: 1,
		{Timestamp: time.Date(2025, 1, 2, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg"}: 2,
	}
	expected := []AggregateItem{
		{OccurredAt: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 1},
		{OccurredAt: time.Date(2025, 1, 2, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg", Count: 2},
	}

	actual := FlattenBucketCounts(bucketCounts)
	assert.Equal(t, expected, actual)
}

type mockHttpClient struct {
	mock.Mock
}

func (m *mockHttpClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

func TestAggregatesServiceClientPostAggregates(t *testing.T) {
	bucketCounts := map[Bucket]int{
		{Timestamp: time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg"}: 1,
		{Timestamp: time.Date(2025, 1, 2, 13, 0, 0, 0, time.UTC), Geohash: "abcdefg"}: 2,
	}

	var (
		payload string
		method  string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err == nil {
			payload = string(data)
		}
		method = r.Method
		defer r.Body.Close()
	}))
	defer ts.Close()

	client := NewAggregatesServiceClient(ts.URL, time.Second, 0, time.Second)
	err := client.PostAggregates(context.Background(), bucketCounts)
	assert.Nil(t, err)

	assert.Equal(t, `[{"occurred_at":"2025-01-01T13:00:00Z","geohash":"abcdefg","count":1},{"occurred_at":"2025-01-02T13:00:00Z","geohash":"abcdefg","count":2}]`, payload)
	assert.Equal(t, http.MethodPost, method)
}
