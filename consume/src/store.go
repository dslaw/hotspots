package main

import (
	"os"
	"path/filepath"
	"time"
)

// MakeBatchID returns a batch id. The current time as a ISO-8601 timestamp is
// used.
func MakeBatchID() string {
	t := time.Now().UTC()
	return t.Format("2006-01-02T04:05.99999Z")
}

type RawDataStoreTx struct {
	Stagingdir string
	Dstdir     string
}

// Setup creates the temporary directory where data files can be staged and
// returns a RawDataStoreTx struct that operates on the created temporary
// directory.
func (s *RawDataStore) Setup() (*RawDataStoreTx, error) {
	batchID := MakeBatchID()
	stagingdir := filepath.Join(s.stagingdir, batchID)
	dstdir := filepath.Join(s.dstdir, batchID)
	err := os.Mkdir(stagingdir, 0750)
	return &RawDataStoreTx{Stagingdir: stagingdir, Dstdir: dstdir}, err
}

func (tx *RawDataStoreTx) MakeStagingPath(filename string) string {
	return filepath.Join(tx.Stagingdir, filename)
}

// Commit sets the temporary directory to the destination directory, effectively
// causing staged data files to become "finalized" data files.
func (tx *RawDataStoreTx) Commit() error {
	return os.Rename(tx.Stagingdir, tx.Dstdir)
}

// Rollback deletes the temporary directory. Its contents are not preserved.
func (tx *RawDataStoreTx) Rollback() error {
	return os.RemoveAll(tx.Stagingdir)
}

// RawDataStore provides methods for writing to the local file system as a data
// store.
//
// When used, RawDataStore and RawDataStoreTx provide functionality to stage
// data to multiple files that should be logically grouped together (e.g. they
// are part of the same batch or partition) and atomically (on *nix systems)
// save them, once all data is written. For example, when there is a partition
// of data that cuts across logical record types, and all data within the
// partition should be processed together:
//
// store := &RawDataStore{stagingdir: "/tmp", dstdir: "/data"}
// tx, err := store.Setup()  // Automatically creates a batch id.
// if err != nil { ... }
// tx.Stagingdir  // Unique directory under /tmp (uniqueness won't hold under concurrency).
// tx.Dstdir      // Unique directory under /data (uniqueness won't hold under concurrency).
// writeDataFilesUnder(dataA, tx.Stagingdir, "a.txt")
// writeDataFilesUnder(dataB, tx.Stagingdir, "b.txt")
// tx.Commit()    // Moves data from /tmp/{batchID} to /data/{batchID}.
type RawDataStore struct {
	// The parent directory for data to be staged to.
	stagingdir string
	// The parent directory for data to be committed to.
	dstdir string
}

// NewRawDataStore creates a new RawDataStore. The given staging and dst
// directories should already exist.
func NewRawDataStore(stagingdir, dstdir string) *RawDataStore {
	return &RawDataStore{stagingdir: stagingdir, dstdir: dstdir}
}
