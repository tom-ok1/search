package translog

import (
	"fmt"
	"io"
	"os"
)

// BaseTranslogReader provides shared read logic for translog files.
type BaseTranslogReader struct {
	file       *os.File
	generation int64
	header     TranslogHeader
	checkpoint Checkpoint
}

// Read reads a single operation at the given file offset.
func (br *BaseTranslogReader) Read(position int64) (Operation, error) {
	if _, err := br.file.Seek(position, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to %d: %w", position, err)
	}
	op, err := ReadOperation(br.file)
	if err != nil {
		return nil, fmt.Errorf("read op at %d: %w", position, err)
	}
	return op, nil
}

// HeaderSize returns the byte size of this reader's header.
func (br *BaseTranslogReader) HeaderSize() int64 {
	return HeaderSizeInBytes(&br.header)
}

// TranslogReader is an immutable reader for a rolled translog generation.
type TranslogReader struct {
	BaseTranslogReader
}

// NewTranslogReader opens a translog file, validates the header, and returns
// an immutable reader.
func NewTranslogReader(path string, generation int64, expectedUUID string, checkpoint Checkpoint) (*TranslogReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open translog: %w", err)
	}

	header, _, err := ReadHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	if header.TranslogUUID != expectedUUID {
		f.Close()
		return nil, fmt.Errorf("UUID mismatch: expected %q, got %q", expectedUUID, header.TranslogUUID)
	}

	return &TranslogReader{
		BaseTranslogReader: BaseTranslogReader{
			file:       f,
			generation: generation,
			header:     *header,
			checkpoint: checkpoint,
		},
	}, nil
}

// NewTranslogReaderFromWriter creates an immutable reader by reusing the
// writer's file handle (reopened for reading). Called during CloseIntoReader.
func NewTranslogReaderFromWriter(writerFile *os.File, generation int64, header TranslogHeader, checkpoint Checkpoint) (*TranslogReader, error) {
	// Close write handle and reopen for reading.
	path := writerFile.Name()
	if err := writerFile.Close(); err != nil {
		return nil, fmt.Errorf("close writer file: %w", err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reopen for reading: %w", err)
	}

	return &TranslogReader{
		BaseTranslogReader: BaseTranslogReader{
			file:       f,
			generation: generation,
			header:     header,
			checkpoint: checkpoint,
		},
	}, nil
}

// Snapshot creates a point-in-time iterator over this reader's operations.
func (r *TranslogReader) Snapshot() *TranslogSnapshot {
	return &TranslogSnapshot{
		reader:            &r.BaseTranslogReader,
		numOps:            int(r.checkpoint.NumOps),
		currentOp:         0,
		offset:            r.HeaderSize(),
		trimmedAboveSeqNo: r.checkpoint.TrimmedAboveSeqNo,
	}
}

// Close closes the underlying file.
func (r *TranslogReader) Close() error {
	return r.file.Close()
}
