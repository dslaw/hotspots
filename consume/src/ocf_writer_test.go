package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOCFWriter(t *testing.T) {
	// Setup.
	tmpdir := os.TempDir()
	filename := filepath.Join(tmpdir, "test.avro")
	defer os.Remove(filename)

	// Use OCFWriter to write Avro encodable records to the file.
	writer, err := NewOCFWriter(filename, schemaA311Case)
	require.Nil(t, err)

	records := []*A311Case{{}, {}, {}}
	for _, record := range records {
		writer.Encode(record)
	}

	err = writer.Close()
	require.Nil(t, err)

	// Should be able to read the written records back.
	file, err := os.Open(filename)
	require.Nil(t, err)
	defer file.Close()

	decoder, err := ocf.NewDecoder(file)
	require.Nil(t, err)

	actual := make([]A311Case, 0)
	for decoder.HasNext() {
		var record A311Case
		err = decoder.Decode(&record)
		require.Nil(t, err)
		actual = append(actual, record)
	}

	assert.Equal(t, len(records), len(actual))
}
