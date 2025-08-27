package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	config, err := NewConfig()
	if err != nil {
		slog.Error("Unable to read config variables", "error", err)
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), config.DatabaseURL)
	if err != nil {
		slog.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := &Repo{conn: pool}

	cache := NewCacheFromURL(config.CacheURL, config.CachePrefix, config.CacheTTL)
	defer cache.Close()

	service := NewAggregatesService(repo, cache)

	getAggregatesHandler := http.HandlerFunc(MakeGetAggregatesHandler(context.Background(), service))
	http.Handle("GET /aggregates", getAggregatesHandler)

	insertAggregatesHandler := http.HandlerFunc(MakeInsertAggregatesHandler(context.Background(), service))
	http.Handle("POST /aggregates", insertAggregatesHandler)

	upsertAggregatesHandler := http.HandlerFunc(MakeUpsertAggregatesHandler(context.Background(), service))
	http.Handle("PUT /aggregates", upsertAggregatesHandler)

	slog.Info(fmt.Sprintf("Listening on port %s...", config.Port))
	http.ListenAndServe(fmt.Sprintf(":%s", config.Port), nil)
}
