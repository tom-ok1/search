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
	OpTypeNoOp   byte = 3
)

// Operation represents a translog operation that can be serialized.
type Operation interface {
	OpType() byte
	SeqNo() int64
	PrimaryTerm() int64
	Serialize(w io.Writer) error
}

// IndexOperation records an index (create/update) operation.
type IndexOperation struct {
	ID         string
	Source     []byte
	SequenceNo int64
	PrimTerm   int64
}

// DeleteOperation records a delete operation.
type DeleteOperation struct {
	ID         string
	SequenceNo int64
	PrimTerm   int64
}

// NoOpOperation records a no-op operation used for primary-replica alignment.
type NoOpOperation struct {
	SequenceNo int64
	PrimTerm   int64
	Reason     string
}

func (op *IndexOperation) OpType() byte       { return OpTypeIndex }
func (op *IndexOperation) SeqNo() int64        { return op.SequenceNo }
func (op *IndexOperation) PrimaryTerm() int64  { return op.PrimTerm }

func (op *DeleteOperation) OpType() byte       { return OpTypeDelete }
func (op *DeleteOperation) SeqNo() int64        { return op.SequenceNo }
func (op *DeleteOperation) PrimaryTerm() int64  { return op.PrimTerm }

func (op *NoOpOperation) OpType() byte       { return OpTypeNoOp }
func (op *NoOpOperation) SeqNo() int64        { return op.SequenceNo }
func (op *NoOpOperation) PrimaryTerm() int64  { return op.PrimTerm }

// WriteTo serializes an IndexOperation.
// Format: [opType:1][seqNo:8][primaryTerm:8][idLen:4][id][sourceLen:4][source][crc32:4]
func (op *IndexOperation) Serialize(w io.Writer) error {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	if err := binary.Write(mw, binary.LittleEndian, op.OpType()); err != nil {
		return fmt.Errorf("write opType: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.SequenceNo); err != nil {
		return fmt.Errorf("write seqNo: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.PrimTerm); err != nil {
		return fmt.Errorf("write primaryTerm: %w", err)
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
// Format: [opType:1][seqNo:8][primaryTerm:8][idLen:4][id][crc32:4]
func (op *DeleteOperation) Serialize(w io.Writer) error {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	if err := binary.Write(mw, binary.LittleEndian, op.OpType()); err != nil {
		return fmt.Errorf("write opType: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.SequenceNo); err != nil {
		return fmt.Errorf("write seqNo: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.PrimTerm); err != nil {
		return fmt.Errorf("write primaryTerm: %w", err)
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

// WriteTo serializes a NoOpOperation.
// Format: [opType:1][seqNo:8][primaryTerm:8][reasonLen:4][reason][crc32:4]
func (op *NoOpOperation) Serialize(w io.Writer) error {
	hasher := crc32.NewIEEE()
	mw := io.MultiWriter(w, hasher)

	if err := binary.Write(mw, binary.LittleEndian, op.OpType()); err != nil {
		return fmt.Errorf("write opType: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.SequenceNo); err != nil {
		return fmt.Errorf("write seqNo: %w", err)
	}
	if err := binary.Write(mw, binary.LittleEndian, op.PrimTerm); err != nil {
		return fmt.Errorf("write primaryTerm: %w", err)
	}
	if err := writeBytes(mw, []byte(op.Reason)); err != nil {
		return fmt.Errorf("write reason: %w", err)
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

	var seqNo int64
	if err := binary.Read(tr, binary.LittleEndian, &seqNo); err != nil {
		return nil, fmt.Errorf("read seqNo: %w", err)
	}

	var primaryTerm int64
	if err := binary.Read(tr, binary.LittleEndian, &primaryTerm); err != nil {
		return nil, fmt.Errorf("read primaryTerm: %w", err)
	}

	switch opType {
	case OpTypeIndex:
		id, err := readBytes(tr)
		if err != nil {
			return nil, fmt.Errorf("read id: %w", err)
		}

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
			ID:         string(id),
			Source:     source,
			SequenceNo: seqNo,
			PrimTerm:   primaryTerm,
		}, nil

	case OpTypeDelete:
		id, err := readBytes(tr)
		if err != nil {
			return nil, fmt.Errorf("read id: %w", err)
		}

		computed := hasher.Sum32()
		var stored uint32
		if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
			return nil, fmt.Errorf("read crc32: %w", err)
		}
		if computed != stored {
			return nil, fmt.Errorf("crc32 mismatch: computed %d, stored %d", computed, stored)
		}

		return &DeleteOperation{
			ID:         string(id),
			SequenceNo: seqNo,
			PrimTerm:   primaryTerm,
		}, nil

	case OpTypeNoOp:
		reason, err := readBytes(tr)
		if err != nil {
			return nil, fmt.Errorf("read reason: %w", err)
		}

		computed := hasher.Sum32()
		var stored uint32
		if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
			return nil, fmt.Errorf("read crc32: %w", err)
		}
		if computed != stored {
			return nil, fmt.Errorf("crc32 mismatch: computed %d, stored %d", computed, stored)
		}

		return &NoOpOperation{
			SequenceNo: seqNo,
			PrimTerm:   primaryTerm,
			Reason:     string(reason),
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
