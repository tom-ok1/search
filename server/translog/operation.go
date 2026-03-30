package translog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	OpTypeIndex  byte = 1
	OpTypeDelete byte = 2
)

// Operation represents a translog operation that can be serialized.
type Operation interface {
	OpType() byte
	WriteTo(w io.Writer) error
}

// IndexOperation records an index (create/update) operation.
type IndexOperation struct {
	ID      string
	Source  []byte
	Version int64
}

// DeleteOperation records a delete operation.
type DeleteOperation struct {
	ID      string
	Version int64
}

func (op *IndexOperation) OpType() byte  { return OpTypeIndex }
func (op *DeleteOperation) OpType() byte { return OpTypeDelete }

// WriteTo serializes an IndexOperation.
// Format: [opType:1][version:8][idLen:4][id][sourceLen:4][source][crc32:4]
func (op *IndexOperation) WriteTo(w io.Writer) error {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	if err := binary.Write(mw, binary.LittleEndian, op.OpType()); err != nil {
		return fmt.Errorf("write opType: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.Version); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	if err := writeBytes(mw, []byte(op.ID)); err != nil {
		return fmt.Errorf("write id: %w", err)
	}
	if err := writeBytes(mw, op.Source); err != nil {
		return fmt.Errorf("write source: %w", err)
	}

	checksum := hasher.Sum32()
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("write crc32: %w", err)
	}
	return nil
}

// WriteTo serializes a DeleteOperation.
// Format: [opType:1][version:8][idLen:4][id][crc32:4]
func (op *DeleteOperation) WriteTo(w io.Writer) error {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	if err := binary.Write(mw, binary.LittleEndian, op.OpType()); err != nil {
		return fmt.Errorf("write opType: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.Version); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	if err := writeBytes(mw, []byte(op.ID)); err != nil {
		return fmt.Errorf("write id: %w", err)
	}

	checksum := hasher.Sum32()
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("write crc32: %w", err)
	}
	return nil
}

// ReadOperation deserializes an Operation from the reader, verifying the CRC32 checksum.
func ReadOperation(r io.Reader) (Operation, error) {
	hasher := crc32.NewIEEE()
	tr := io.TeeReader(r, hasher)

	var opType byte
	if err := binary.Read(tr, binary.LittleEndian, &opType); err != nil {
		return nil, fmt.Errorf("read opType: %w", err)
	}

	var version int64
	if err := binary.Read(tr, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}

	id, err := readBytes(tr)
	if err != nil {
		return nil, fmt.Errorf("read id: %w", err)
	}

	switch opType {
	case OpTypeIndex:
		source, err := readBytes(tr)
		if err != nil {
			return nil, fmt.Errorf("read source: %w", err)
		}

		computed := hasher.Sum32()
		var stored uint32
		if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
			return nil, fmt.Errorf("read crc32: %w", err)
		}
		if computed != stored {
			return nil, fmt.Errorf("crc32 mismatch: computed %d, stored %d", computed, stored)
		}

		return &IndexOperation{
			ID:      string(id),
			Source:  source,
			Version: version,
		}, nil

	case OpTypeDelete:
		computed := hasher.Sum32()
		var stored uint32
		if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
			return nil, fmt.Errorf("read crc32: %w", err)
		}
		if computed != stored {
			return nil, fmt.Errorf("crc32 mismatch: computed %d, stored %d", computed, stored)
		}

		return &DeleteOperation{
			ID:      string(id),
			Version: version,
		}, nil

	default:
		return nil, fmt.Errorf("unknown operation type: %d", opType)
	}
}

// writeBytes writes a length-prefixed byte slice.
func writeBytes(w io.Writer, b []byte) error {
	length := uint32(len(b))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// readBytes reads a length-prefixed byte slice.
func readBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
