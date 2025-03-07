package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	kafka "github.com/segmentio/kafka-go"
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

	decoder := &MessageDecoder{}
	var writer Writable

	if config.ConsumerType == RawConsumerType {
		writer = NewRawWriter(decoder)
	} else if config.ConsumerType == AggregateConsumerType {
		writer = NewAggregateWriter(config.BucketTimePrecision, config.BucketGeohashPrecision, decoder)
	} else {
		slog.Error("Unknown consumer type", "consumer_type", config.ConsumerType)
		os.Exit(1)
	}
	if writer == nil {
		slog.Error("Error initiating reader")
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
