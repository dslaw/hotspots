package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// RawWriter writes data directly (i.e. as it is received) to a data sink.
type RawWriter struct {
	store *RawDataStore
}

func NewRawWriter(tmpdir, dstdir string) *RawWriter {
	store := NewRawDataStore(tmpdir, dstdir)
	return &RawWriter{store: store}
}

func WriteRawRecords(store *RawDataStore, messages []kafka.Message) error {
	writers := make(map[string]*OCFWriter)

	tx, err := store.Setup()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for record := range DecodeMessages(SchemaNameHeader, messages) {
		schemaName := record.SchemaName()

		var writer *OCFWriter
		writer, ok := writers[schemaName]
		if !ok {
			filename := tx.MakeStagingPath(fmt.Sprintf("%s.avro", schemaName))
			writer, err = NewOCFWriter(filename, record.Schema())
			if err != nil {
				return err
			}
			defer writer.Close()
			writers[schemaName] = writer
		}

		if err := writer.Encode(record); err != nil {
			continue
		}
	}

	for _, writer := range writers {
		if err := writer.encoder.Flush(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Write writes messages to the data sink.
// NB: Malformed messages will be dropped.
func (w *RawWriter) Write(_ context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	return WriteRawRecords(w.store, messages)
}
