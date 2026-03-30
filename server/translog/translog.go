package translog

import (
	"fmt"
	"os"
	"path/filepath"
)

const translogFile = "translog.tlog"

// Translog manages the translog writer and provides Add/Sync/Recover/TrimToEmpty/Close lifecycle.
type Translog struct {
	dir    string
	writer *TranslogWriter
}

// NewTranslog creates a new Translog in the given directory.
// It creates the directory if it does not exist and opens a TranslogWriter.
func NewTranslog(dir string) (*Translog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create translog dir: %w", err)
	}

	w, err := NewTranslogWriter(filepath.Join(dir, translogFile))
	if err != nil {
		return nil, fmt.Errorf("open translog writer: %w", err)
	}

	return &Translog{
		dir:    dir,
		writer: w,
	}, nil
}

// Add appends an operation to the translog.
func (tl *Translog) Add(op Operation) error {
	return tl.writer.Add(op)
}

// Sync flushes and fsyncs the translog to disk.
func (tl *Translog) Sync() error {
	return tl.writer.Sync()
}

// Recover reads all operations from the translog file and returns them.
// The caller is responsible for replaying them into the engine.
func (tl *Translog) Recover() ([]Operation, error) {
	path := filepath.Join(tl.dir, translogFile)

	reader, err := NewTranslogReader(path)
	if err != nil {
		return nil, fmt.Errorf("open translog reader: %w", err)
	}
	defer reader.Close()

	ops, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read translog: %w", err)
	}
	return ops, nil
}

// TrimToEmpty clears the translog by closing the current writer,
// truncating the file, and opening a new writer.
func (tl *Translog) TrimToEmpty() error {
	if err := tl.writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	path := filepath.Join(tl.dir, translogFile)
	if err := os.Truncate(path, 0); err != nil {
		return fmt.Errorf("truncate translog: %w", err)
	}

	w, err := NewTranslogWriter(path)
	if err != nil {
		return fmt.Errorf("reopen translog writer: %w", err)
	}
	tl.writer = w
	return nil
}

// Close closes the translog writer.
func (tl *Translog) Close() error {
	return tl.writer.Close()
}
