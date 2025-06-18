package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/segmentio/kafka-go"
)

func main() {
	config, ok := NewConfig()
	if !ok {
		slog.Error("Unable to read config from environment")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.BrokerURL},
		Topic:    config.Topic,
		GroupID:  config.ConsumerGroupID,
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	bucketer := NewBucketer(config.BucketTimePrecision, config.BucketGeohashPrecision)

	var writer Writable

	if config.ConsumerType == RawConsumerType {
		warehouseConnOptions, err := clickhouse.ParseDSN(config.WarehouseURL)
		if err != nil {
			slog.Error("Unable to parse warehouse url", "error", err)
			os.Exit(1)
		}
		conn, err := clickhouse.Open(warehouseConnOptions)
		if err != nil {
			slog.Error("Unable to connect to warehouse", "error", err)
			os.Exit(1)
		}
		defer conn.Close()
		writer = NewRawWriter(conn, bucketer)
	} else if config.ConsumerType == AggregateConsumerType {
		client := NewAggregatesServiceClient(
			config.AppURL,
			config.HttpRequestTimeout,
			config.HttpRequestRetries,
			config.HttpRequestBackoff,
		)
		writer = NewAggregateWriter(client, bucketer)
	} else {
		slog.Error("Unknown consumer type", "consumer_type", config.ConsumerType)
		os.Exit(1)
	}
	if writer == nil {
		slog.Error("Error initiating writer")
		os.Exit(1)
	}

	consumer := NewBufferedConsumer(config.BufferSize, config.FlushInterval, reader, writer)
	if consumer == nil {
		slog.Error("Error initiating consumer")
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go consumer.Process(ctx)

	<-signalChan
	slog.Info("Shutting down...")
}
