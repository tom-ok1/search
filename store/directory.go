package store

import "io"

// Directory abstracts file I/O for index storage.
type Directory interface {
	// CreateOutput creates a write stream for the named file.
	CreateOutput(name string) (IndexOutput, error)
	// OpenInput opens a read stream for the named file.
	OpenInput(name string) (IndexInput, error)
	// ListAll returns all file names in the directory.
	ListAll() ([]string, error)
	// DeleteFile removes a file from the directory.
	DeleteFile(name string) error
	// FileExists reports whether a file exists in the directory.
	FileExists(name string) bool
}

// IndexOutput is a write stream for index data.
type IndexOutput interface {
	io.Writer
	// WriteVInt writes a variable-length encoded integer.
	WriteVInt(v int) error
	Close() error
}

// IndexInput is a read stream for index data.
type IndexInput interface {
	io.Reader
	// ReadVInt reads a variable-length encoded integer.
	ReadVInt() (int, error)
	Close() error
}
