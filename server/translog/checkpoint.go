package translog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	// SeqNoUnassigned indicates a sequence number has not been assigned.
	SeqNoUnassigned int64 = -2

	// NoOpsPerformed indicates no operations have been performed.
	NoOpsPerformed int64 = -1

	// checkpointFieldsSize is the total size of the serialized Checkpoint fields in bytes.
	// 8 (Offset) + 4 (NumOps) + 8 (Generation) + 8 (MinSeqNo) + 8 (MaxSeqNo)
	// + 8 (GlobalCheckpoint) + 8 (MinTranslogGeneration) + 8 (TrimmedAboveSeqNo) = 60
	checkpointFieldsSize = 60

	// checkpointCRC32Size is the size of the CRC32 footer.
	checkpointCRC32Size = 4

	// CheckpointSize is the total serialized size of a checkpoint (fields + CRC32).
	CheckpointSize = checkpointFieldsSize + checkpointCRC32Size
)

// Checkpoint holds the metadata for a translog generation file,
// tracking write position, operation counts, and sequence number ranges.
type Checkpoint struct {
	Offset                int64
	NumOps                int32
	Generation            int64
	MinSeqNo              int64
	MaxSeqNo              int64
	GlobalCheckpoint      int64
	MinTranslogGeneration int64
	TrimmedAboveSeqNo     int64
}

// EmptyCheckpoint creates a checkpoint with sentinel values for an empty generation.
func EmptyCheckpoint(generation, minTranslogGeneration int64) *Checkpoint {
	return &Checkpoint{
		Offset:                0,
		NumOps:                0,
		Generation:            generation,
		MinSeqNo:              SeqNoUnassigned,
		MaxSeqNo:              NoOpsPerformed,
		GlobalCheckpoint:      NoOpsPerformed,
		MinTranslogGeneration: minTranslogGeneration,
		TrimmedAboveSeqNo:     SeqNoUnassigned,
	}
}

// MarshalBinary serializes the checkpoint to a fixed-size byte slice
// with a CRC32 (IEEE) footer.
func (cp *Checkpoint) MarshalBinary() ([]byte, error) {
	buf := make([]byte, CheckpointSize)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(cp.Offset))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(cp.NumOps))
	binary.LittleEndian.PutUint64(buf[12:20], uint64(cp.Generation))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(cp.MinSeqNo))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(cp.MaxSeqNo))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(cp.GlobalCheckpoint))
	binary.LittleEndian.PutUint64(buf[44:52], uint64(cp.MinTranslogGeneration))
	binary.LittleEndian.PutUint64(buf[52:60], uint64(cp.TrimmedAboveSeqNo))

	checksum := crc32.ChecksumIEEE(buf[:checkpointFieldsSize])
	binary.LittleEndian.PutUint32(buf[60:64], checksum)

	return buf, nil
}

// UnmarshalCheckpoint deserializes a checkpoint from a fixed-size byte slice,
// verifying the CRC32 footer.
func UnmarshalCheckpoint(data []byte) (*Checkpoint, error) {
	if len(data) != CheckpointSize {
		return nil, fmt.Errorf("checkpoint data: expected %d bytes, got %d", CheckpointSize, len(data))
	}

	stored := binary.LittleEndian.Uint32(data[60:64])
	computed := crc32.ChecksumIEEE(data[:checkpointFieldsSize])
	if stored != computed {
		return nil, fmt.Errorf("checkpoint CRC32 mismatch: stored %08x, computed %08x", stored, computed)
	}

	cp := &Checkpoint{
		Offset:                int64(binary.LittleEndian.Uint64(data[0:8])),
		NumOps:                int32(binary.LittleEndian.Uint32(data[8:12])),
		Generation:            int64(binary.LittleEndian.Uint64(data[12:20])),
		MinSeqNo:              int64(binary.LittleEndian.Uint64(data[20:28])),
		MaxSeqNo:              int64(binary.LittleEndian.Uint64(data[28:36])),
		GlobalCheckpoint:      int64(binary.LittleEndian.Uint64(data[36:44])),
		MinTranslogGeneration: int64(binary.LittleEndian.Uint64(data[44:52])),
		TrimmedAboveSeqNo:     int64(binary.LittleEndian.Uint64(data[52:60])),
	}

	return cp, nil
}

// WriteCheckpointSync atomically writes the checkpoint to path.
// It writes to a temporary file in the same directory, fsyncs, renames
// over the target, and fsyncs the parent directory for durability.
func WriteCheckpointSync(path string, cp *Checkpoint) error {
	data, err := cp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	dir := filepath.Dir(path)

	tmp, err := os.CreateTemp(dir, "checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("fsync temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	parentDir, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open parent dir: %w", err)
	}
	if err := parentDir.Sync(); err != nil {
		parentDir.Close()
		return fmt.Errorf("fsync parent dir: %w", err)
	}
	return parentDir.Close()
}
