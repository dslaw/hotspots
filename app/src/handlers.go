package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
)

func MakeGetAggregatesHandler(ctx context.Context, service *AggregatesService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
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

func MakeInsertAggregatesHandler(ctx context.Context, service *AggregatesService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		records, err := DecodeAggregatesFromReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		if err := service.InsertAggregates(ctx, records); err != nil {
			slog.Error("Unable to write records", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		return
	}
}

func MakeUpsertAggregatesHandler(ctx context.Context, service *AggregatesService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		records, err := DecodeAggregatesFromReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		if err := service.UpsertAggregates(ctx, records); err != nil {
			slog.Error("Unable to write records", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		return
	}
}
