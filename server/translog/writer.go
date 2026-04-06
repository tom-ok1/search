package translog

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
)

const defaultBufSize = 64 * 1024 // 64KB

// Location identifies where an operation was written in the translog.
type Location struct {
	Generation int64
	Offset     int64
	Size       int32
}

// TranslogWriter is the single mutable file in the translog.
// It appends operations, tracks sequence number ranges, and manages
// checkpoint persistence on sync.
type TranslogWriter struct {
	mu sync.Mutex

	file       *os.File
	buf        *bufio.Writer
	generation int64
	header     TranslogHeader

	totalOffset      int64 // current byte position
	operationCounter int32 // ops written to this generation
	minSeqNo         int64
	maxSeqNo         int64

	lastSyncedCheckpoint Checkpoint
	checkpointPath       string // path to translog.ckp
}

// NewTranslogWriter creates a new translog generation file at path,
// writes the header, and initializes the checkpoint state.
func NewTranslogWriter(path, checkpointPath string, generation int64, header TranslogHeader, initialCheckpoint Checkpoint) (*TranslogWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("create translog file: %w", err)
	}

	buf := bufio.NewWriterSize(f, defaultBufSize)

	// Write header.
	headerSize, err := WriteHeader(buf, &header)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	w := &TranslogWriter{
		file:             f,
		buf:              buf,
		generation:       generation,
		header:           header,
		totalOffset:      headerSize,
		operationCounter: 0,
		minSeqNo:         SeqNoUnassigned,
		maxSeqNo:         NoOpsPerformed,
		checkpointPath:   checkpointPath,
		lastSyncedCheckpoint: initialCheckpoint,
	}

	return w, nil
}

// Add serializes op, appends it to the buffer, updates sequence number
// tracking and offset, and returns the Location where it was written.
func (w *TranslogWriter) Add(op Operation) (Location, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize to a temporary buffer to measure size.
	var tmp bytes.Buffer
	if err := op.Serialize(&tmp); err != nil {
		return Location{}, fmt.Errorf("serialize op: %w", err)
	}

	loc := Location{
		Generation: w.generation,
		Offset:     w.totalOffset,
		Size:       int32(tmp.Len()),
	}

	if _, err := w.buf.Write(tmp.Bytes()); err != nil {
		return Location{}, fmt.Errorf("write to buffer: %w", err)
	}

	w.totalOffset += int64(tmp.Len())
	w.operationCounter++

	// Update seqNo tracking.
	seqNo := op.SeqNo()
	if w.minSeqNo == SeqNoUnassigned || seqNo < w.minSeqNo {
		w.minSeqNo = seqNo
	}
	if seqNo > w.maxSeqNo {
		w.maxSeqNo = seqNo
	}

	return loc, nil
}

// Sync flushes the buffer, fsyncs the data file, and atomically writes
// the checkpoint to reflect the current state.
func (w *TranslogWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.syncLocked()
}

func (w *TranslogWriter) syncLocked() error {
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync data: %w", err)
	}

	cp := w.currentCheckpoint()
	if err := WriteCheckpointSync(w.checkpointPath, &cp); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	w.lastSyncedCheckpoint = cp

	return nil
}

// currentCheckpoint builds a Checkpoint reflecting the writer's current state.
func (w *TranslogWriter) currentCheckpoint() Checkpoint {
	return Checkpoint{
		Offset:                w.totalOffset,
		NumOps:                w.operationCounter,
		Generation:            w.generation,
		MinSeqNo:              w.minSeqNo,
		MaxSeqNo:              w.maxSeqNo,
		GlobalCheckpoint:      w.lastSyncedCheckpoint.GlobalCheckpoint,
		MinTranslogGeneration: w.lastSyncedCheckpoint.MinTranslogGeneration,
		TrimmedAboveSeqNo:     w.lastSyncedCheckpoint.TrimmedAboveSeqNo,
	}
}

// CloseIntoReader syncs the writer, writes its final per-generation checkpoint,
// and returns an immutable TranslogReader for this generation.
func (w *TranslogWriter) CloseIntoReader(genCheckpointPath string) (*TranslogReader, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Final sync.
	if err := w.syncLocked(); err != nil {
		return nil, fmt.Errorf("final sync: %w", err)
	}

	// Write per-generation checkpoint.
	cp := w.currentCheckpoint()
	if err := WriteCheckpointSync(genCheckpointPath, &cp); err != nil {
		return nil, fmt.Errorf("write generation checkpoint: %w", err)
	}

	// Create reader from writer's file handle.
	reader, err := NewTranslogReaderFromWriter(w.file, w.generation, w.header, cp)
	if err != nil {
		return nil, fmt.Errorf("create reader from writer: %w", err)
	}

	// Nil out file so Close doesn't double-close.
	w.file = nil

	return reader, nil
}

// Generation returns this writer's generation number.
func (w *TranslogWriter) Generation() int64 {
	return w.generation
}

// Close syncs the checkpoint and closes the underlying file, ensuring
// all buffered operations are durable and recoverable.
func (w *TranslogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	if err := w.syncLocked(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
