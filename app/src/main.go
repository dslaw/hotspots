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

type Repoer interface {
	GetAggregateRows(context.Context, time.Time, time.Time) ([]AggregateRow, error)
}

func MakeGetAggregatesHandler(ctx context.Context, repo Repoer) func(http.ResponseWriter, *http.Request) {
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

		rows, err := repo.GetAggregateRows(ctx, params.StartTime, params.EndTime)
		if err != nil {
			slog.Error("Unable to read from database", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		records := Rollup(rows, params.TimePrecision, params.GeoPrecision)

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
	port, ok := os.LookupEnv("APP_PORT")
	if !ok {
		slog.Error("Unable to read app port")
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		slog.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := &Repo{conn: pool}

	getAggregatesHandler := http.HandlerFunc(MakeGetAggregatesHandler(context.Background(), repo))
	http.Handle("/aggregates", getAggregatesHandler)

	slog.Info(fmt.Sprintf("Listening on port %s...", port))
	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}
