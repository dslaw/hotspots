package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawDataStoreTxMakeStagingPath(t *testing.T) {
	tx := &RawDataStoreTx{Stagingdir: "/somewhere/staging/batch-id", Dstdir: "/somewhere/acquired/batch-id"}
	actual := tx.MakeStagingPath("data.avro")
	assert.Equal(t, "/somewhere/staging/batch-id/data.avro", actual)
}

func TestRawDataStoreSetup(t *testing.T) {
	tmpdir := os.TempDir()
	dstdir, err := os.MkdirTemp(tmpdir, "*")
	if err != nil {
		assert.FailNow(t, "Unable to create directory in test setup", err)
	}
	defer os.RemoveAll(dstdir)

	store := NewRawDataStore(tmpdir, dstdir)

	tx, err := store.Setup()
	assert.NotNil(t, tx)
	assert.Nil(t, err)

	// Staging directory was created and was created under `tmpdir`.
	assert.DirExists(t, tx.Stagingdir)
	assert.Equal(t, tmpdir, filepath.Dir(tx.Stagingdir))
	// Destination directory was _not_ created, but is set to be created under
	// `dstdir`.
	assert.NoDirExists(t, tx.Dstdir)
	assert.Equal(t, dstdir, filepath.Dir(tx.Dstdir))
}

func TestRawDataStoreSetupCommit(t *testing.T) {
	tmpdir := os.TempDir()
	dstdir, err := os.MkdirTemp(tmpdir, "*")
	if err != nil {
		assert.FailNow(t, "Unable to create directory in test setup", err)
	}
	defer os.RemoveAll(dstdir)

	store := NewRawDataStore(tmpdir, dstdir)

	tx, err := store.Setup()
	require.Nil(t, err)

	dataFilename := tx.MakeStagingPath("data.txt")
	file, err := os.Create(dataFilename)
	require.Nil(t, err)
	defer file.Close()

	err = tx.Commit()
	assert.Nil(t, err)

	// Staging directory is implicitly cleaned up.
	assert.NoDirExists(t, tx.Stagingdir)
	// Destination directory exists and has the created file in it.
	assert.DirExists(t, tx.Dstdir)
	assert.FileExists(t, filepath.Join(tx.Dstdir, "data.txt"))
}

func TestRawDataStoreSetupRollback(t *testing.T) {
	tmpdir := os.TempDir()
	dstdir, err := os.MkdirTemp(tmpdir, "*")
	if err != nil {
		assert.FailNow(t, "Unable to create directory in test setup", err)
	}
	defer os.RemoveAll(dstdir)

	store := NewRawDataStore(tmpdir, dstdir)

	tx, err := store.Setup()
	require.Nil(t, err)

	dataFilename := tx.MakeStagingPath("data.txt")
	file, err := os.Create(dataFilename)
	require.Nil(t, err)
	defer file.Close()

	err = tx.Rollback()
	assert.Nil(t, err)

	// Staging directory is explicitly removed.
	assert.NoDirExists(t, tx.Stagingdir)
	// Destination directory does not exist.
	assert.NoDirExists(t, tx.Dstdir)
}
