package translog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// magicBytes identifies a translog file.
var magicBytes = []byte("tlog")

// headerVersion is the current on-disk header format version.
const headerVersion int32 = 1

// TranslogHeader contains metadata written at the beginning of every
// translog generation file.
type TranslogHeader struct {
	TranslogUUID string // Unique ID tying all generations together
	PrimaryTerm  int64  // Primary term when this file was created
}

// WriteHeader serialises h into w using little-endian encoding and appends
// a CRC32 checksum. It returns the total number of bytes written (including
// the checksum).
func WriteHeader(w io.Writer, h *TranslogHeader) (int64, error) {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	var written int64

	// Magic bytes.
	n, err := mw.Write(magicBytes)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("translog header: write magic: %w", err)
	}

	// Version.
	if err := binary.Write(mw, binary.LittleEndian, headerVersion); err != nil {
		return written, fmt.Errorf("translog header: write version: %w", err)
	}
	written += 4

	// UUID length + UUID bytes.
	uuidBytes := []byte(h.TranslogUUID)
	uuidLen := int32(len(uuidBytes))
	if err := binary.Write(mw, binary.LittleEndian, uuidLen); err != nil {
		return written, fmt.Errorf("translog header: write uuid length: %w", err)
	}
	written += 4

	n, err = mw.Write(uuidBytes)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("translog header: write uuid: %w", err)
	}

	// Primary term.
	if err := binary.Write(mw, binary.LittleEndian, h.PrimaryTerm); err != nil {
		return written, fmt.Errorf("translog header: write primary term: %w", err)
	}
	written += 8

	// CRC32 checksum (written only to w, not fed back into the hasher).
	checksum := hasher.Sum32()
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return written, fmt.Errorf("translog header: write crc32: %w", err)
	}
	written += 4

	return written, nil
}

// ReadHeader deserialises a TranslogHeader from r, validates the magic bytes,
// version, and CRC32 checksum. It returns the header and the total number of
// bytes consumed (including the checksum).
func ReadHeader(r io.Reader) (*TranslogHeader, int64, error) {
	hasher := crc32.NewIEEE()
	tr := io.TeeReader(r, hasher)

	var read int64

	// Magic bytes.
	magic := make([]byte, 4)
	n, err := io.ReadFull(tr, magic)
	read += int64(n)
	if err != nil {
		return nil, read, fmt.Errorf("translog header: read magic: %w", err)
	}
	if string(magic) != string(magicBytes) {
		return nil, read, errors.New("translog header: invalid magic bytes")
	}

	// Version.
	var version int32
	if err := binary.Read(tr, binary.LittleEndian, &version); err != nil {
		return nil, read, fmt.Errorf("translog header: read version: %w", err)
	}
	read += 4
	if version != headerVersion {
		return nil, read, fmt.Errorf("translog header: unsupported version %d", version)
	}

	// UUID length.
	var uuidLen int32
	if err := binary.Read(tr, binary.LittleEndian, &uuidLen); err != nil {
		return nil, read, fmt.Errorf("translog header: read uuid length: %w", err)
	}
	read += 4
	if uuidLen < 0 {
		return nil, read, fmt.Errorf("translog header: negative uuid length %d", uuidLen)
	}

	// UUID bytes.
	uuidBytes := make([]byte, uuidLen)
	n, err = io.ReadFull(tr, uuidBytes)
	read += int64(n)
	if err != nil {
		return nil, read, fmt.Errorf("translog header: read uuid: %w", err)
	}

	// Primary term.
	var primaryTerm int64
	if err := binary.Read(tr, binary.LittleEndian, &primaryTerm); err != nil {
		return nil, read, fmt.Errorf("translog header: read primary term: %w", err)
	}
	read += 8

	// CRC32 checksum — read directly from r so it is not fed into the hasher.
	var storedCRC uint32
	if err := binary.Read(r, binary.LittleEndian, &storedCRC); err != nil {
		return nil, read, fmt.Errorf("translog header: read crc32: %w", err)
	}
	read += 4

	if computed := hasher.Sum32(); computed != storedCRC {
		return nil, read, fmt.Errorf("translog header: crc32 mismatch: stored=%d computed=%d", storedCRC, computed)
	}

	return &TranslogHeader{
		TranslogUUID: string(uuidBytes),
		PrimaryTerm:  primaryTerm,
	}, read, nil
}

// HeaderSizeInBytes returns the total serialised size of the header in bytes:
// 4 (magic) + 4 (version) + 4 (uuid length) + len(uuid) + 8 (primary term) + 4 (crc32).
func HeaderSizeInBytes(h *TranslogHeader) int64 {
	return 4 + 4 + 4 + int64(len(h.TranslogUUID)) + 8 + 4
}
