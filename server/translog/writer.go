package translog

import (
	"bufio"
	"os"
	"sync"
)

const defaultBufSize = 64 * 1024 // 64KB

// TranslogWriter appends operations to a translog file with buffering.
type TranslogWriter struct {
	mu         sync.Mutex
	file       *os.File
	buf        *bufio.Writer
	operations int
}

// NewTranslogWriter opens (or creates) the file at path for appending
// and wraps it in a buffered writer.
func NewTranslogWriter(path string) (*TranslogWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &TranslogWriter{
		file: f,
		buf:  bufio.NewWriterSize(f, defaultBufSize),
	}, nil
}

// Add serializes op and appends it to the buffer.
func (w *TranslogWriter) Add(op Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := op.WriteTo(w.buf); err != nil {
		return err
	}
	w.operations++
	return nil
}

// Sync flushes the buffer and fsyncs the underlying file.
func (w *TranslogWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Operations returns the number of operations written so far.
func (w *TranslogWriter) Operations() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.operations
}

// Close flushes the buffer and closes the underlying file.
func (w *TranslogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
