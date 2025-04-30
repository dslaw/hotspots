package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func MakeGetAggregatesHandler(ctx context.Context, service *AggregatesService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		params, err := GetAggregatesReqParams(query)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		records, err := service.GetAggregates(ctx, params)
		if err != nil {
			slog.Error("Unable to fetch data", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := EncodeAggregates(records, w); err != nil {
			slog.Error("Unable to encode response data", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		return
	}
}

func main() {
	databaseURL, ok := os.LookupEnv("AGGREGATES_DB_URL")
	if !ok {
		slog.Error("Unable to read database url")
		os.Exit(1)
	}
	cacheURL, ok := os.LookupEnv("REDIS_URL")
	if !ok {
		slog.Error("Unable to read cache url")
		os.Exit(1)
	}
	port, ok := os.LookupEnv("APP_PORT")
	if !ok {
		slog.Error("Unable to read app port")
		os.Exit(1)
	}
	cachePrefix, ok := os.LookupEnv("CACHE_AGGREGATES_PREFIX")
	if !ok {
		slog.Error("Unable to read cache prefix")
		os.Exit(1)
	}
	cacheTTLString, ok := os.LookupEnv("CACHE_AGGREGATES_TTL")
	if !ok {
		slog.Error("Unable to read cache ttl")
		os.Exit(1)
	}
	cacheTTL, err := time.ParseDuration(cacheTTLString)
	if err != nil {
		slog.Error("Unable to parse cache ttl", "error", err)
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		slog.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := &Repo{conn: pool}

	cache := NewCacheFromURL(cacheURL, cachePrefix, cacheTTL)
	defer cache.Close()

	service := NewAggregatesService(repo, cache)

	getAggregatesHandler := http.HandlerFunc(MakeGetAggregatesHandler(context.Background(), service))
	http.Handle("/aggregates", getAggregatesHandler)

	slog.Info(fmt.Sprintf("Listening on port %s...", port))
	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}
