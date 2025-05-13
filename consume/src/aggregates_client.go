package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"
)

type AggregateItem struct {
	OccurredAt time.Time `json:"occurred_at"`
	Geohash    string    `json:"geohash"`
	Count      int       `json:"count"`
}

func FlattenBucketCounts(bucketCounts map[Bucket]int) []AggregateItem {
	records := make([]AggregateItem, len(bucketCounts))
	idx := 0
	for bucket, count := range bucketCounts {
		records[idx] = AggregateItem{
			OccurredAt: bucket.Timestamp,
			Geohash:    bucket.Geohash,
			Count:      count,
		}
		idx++
	}

	// Sort for testability.
	slices.SortFunc(records, func(a, b AggregateItem) int {
		if n := a.OccurredAt.Compare(b.OccurredAt); n != 0 {
			return n
		}
		if n := cmp.Compare(a.Geohash, b.Geohash); n != 0 {
			return n
		}
		return cmp.Compare(a.Count, b.Count)
	})

	return records
}

type HttpRequestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// AggregatesServiceClient is an HTTP client for interacting with the aggregates
// service (i.e. `app`).
type AggregatesServiceClient struct {
	client  HttpRequestDoer
	url     string
	retries int
	backoff time.Duration
}

func NewAggregatesServiceClient(url string, timeout time.Duration, retries int, backoff time.Duration) *AggregatesServiceClient {
	client := &http.Client{Timeout: timeout}
	return &AggregatesServiceClient{client: client, url: url, retries: retries, backoff: backoff}
}

func NewAggregatesServiceClientFromHttpClient(httpClient HttpRequestDoer, url string, timeout time.Duration, retries int, backoff time.Duration) *AggregatesServiceClient {
	return &AggregatesServiceClient{client: httpClient, url: url, retries: retries, backoff: backoff}
}

// Dispatch sends a prepared request using the configured client. The request is
// retried with linear backoff if the server responds with a 429 or 5xx status
// code.
func (c *AggregatesServiceClient) Dispatch(request *http.Request) error {
	for attemptNumber := range c.retries + 1 {
		response, err := c.client.Do(request)
		if err != nil {
			return err
		}

		if response.StatusCode == http.StatusOK {
			break
		}

		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			// TODO: Jitter.
			failures := 1 + attemptNumber
			time.Sleep(time.Duration(failures) * c.backoff)
		} else {
			return fmt.Errorf("HTTP error: %s", response.Status)
		}
	}

	return nil
}

// PostAggregates sends a POST request to the aggregates service to write
// aggregates.
func (c *AggregatesServiceClient) PostAggregates(ctx context.Context, bucketCounts map[Bucket]int) error {
	records := FlattenBucketCounts(bucketCounts)
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}

	body := bytes.NewBuffer(data)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "application/json")

	return c.Dispatch(request)
}
