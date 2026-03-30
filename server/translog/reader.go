package translog

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// TranslogReader reads operations sequentially from a translog file.
type TranslogReader struct {
	file *os.File
}

// NewTranslogReader opens the file at path for reading.
func NewTranslogReader(path string) (*TranslogReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open translog: %w", err)
	}
	return &TranslogReader{file: f}, nil
}

// ReadAll reads all operations from the translog file sequentially.
func (r *TranslogReader) ReadAll() ([]Operation, error) {
	var ops []Operation
	for {
		op, err := ReadOperation(r.file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read operation: %w", err)
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// Close closes the underlying file.
func (r *TranslogReader) Close() error {
	return r.file.Close()
}
