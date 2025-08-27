package main

import (
	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// OCFWriter writes records to an Avro OCF.
type OCFWriter struct {
	encoder *ocf.Encoder
	file    *os.File
}

func NewOCFWriter(filename string, schema avro.Schema) (*OCFWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	encoder, err := ocf.NewEncoderWithSchema(schema, file)
	if err != nil {
		return nil, err
	}

	return &OCFWriter{encoder: encoder, file: file}, nil
}

func (w *OCFWriter) Encode(v any) error {
	return w.encoder.Encode(v)
}

func (w *OCFWriter) Close() error {
	defer w.file.Close()
	return w.encoder.Close()
}
