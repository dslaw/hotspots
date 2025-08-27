package main

import "errors"

var (
	ErrNoSchemaNameHeader = errors.New("Unable to get schema name")
	ErrUnrecognizedSchema = errors.New("Unrecognized schema")
	ErrBufferFull         = errors.New("Buffer is full")
)
