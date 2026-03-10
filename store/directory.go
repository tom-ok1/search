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
	// FilePath returns the absolute filesystem path for the named file.
	FilePath(name string) string
	// Sync fsyncs the given files individually so that their data is durable.
	Sync(names []string) error
	// SyncMetaData fsyncs the directory itself so that new file entries are durable.
	SyncMetaData() error
	// Rename atomically renames a file within the directory.
	Rename(source, dest string) error
}

// IndexOutput is a write stream for index data.
type IndexOutput interface {
	io.Writer
	// WriteVInt writes a variable-length encoded integer.
	WriteVInt(v int) error
	// WriteUint16 writes a little-endian uint16.
	WriteUint16(v uint16) error
	// WriteUint32 writes a little-endian uint32.
	WriteUint32(v uint32) error
	// WriteUint64 writes a little-endian uint64.
	WriteUint64(v uint64) error
	Close() error
}

// IndexInput is a read stream for index data.
type IndexInput interface {
	io.Reader
	// ReadVInt reads a variable-length encoded integer.
	ReadVInt() (int, error)
	Close() error
}

// DataInput provides random-access and sequential reads over index data.
// It is implemented by MMapIndexInput and used by FST for byte-level access.
type DataInput interface {
	ReadByte() (byte, error)
	ReadBytes(n int) ([]byte, error)
	ReadUvarint() (uint64, error)
	Seek(pos int)
	Position() int
	Length() int
	Clone() DataInput
}
