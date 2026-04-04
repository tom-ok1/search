package translog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// TranslogConfig holds configuration for a Translog instance.
type TranslogConfig struct {
	Dir                     string
	BufferSize              int   // Default 64KB
	GenerationThresholdSize int64 // Roll when file exceeds this (0 = no auto-roll)
}

// Translog manages multiple generations of translog files, providing
// durable operation logging with multi-generation rollover, snapshot-based
// recovery, and retention management.
type Translog struct {
	mu sync.RWMutex

	dir         string
	uuid        string
	currentTerm int64

	current        *TranslogWriter
	readers        []*TranslogReader
	deletionPolicy *TranslogDeletionPolicy
	config         *TranslogConfig
	tragedy        error
}

// NewTranslog recovers an existing translog or creates a new one.
// If no checkpoint exists, a fresh generation 1 is created.
func NewTranslog(config *TranslogConfig, translogUUID string, primaryTerm int64, globalCheckpoint int64, minGen int64) (*Translog, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("create translog dir: %w", err)
	}

	tl := &Translog{
		dir:            config.Dir,
		currentTerm:    primaryTerm,
		config:         config,
		deletionPolicy: NewTranslogDeletionPolicy(minGen),
	}

	// Try to recover from existing checkpoint.
	ckpPath := filepath.Join(config.Dir, "translog.ckp")
	ckpData, err := os.ReadFile(ckpPath)
	if err == nil && len(ckpData) == CheckpointSize {
		cp, err := UnmarshalCheckpoint(ckpData)
		if err != nil {
			return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
		}

		if translogUUID == "" {
			// Recover UUID from existing translog file.
			tlogPath := filepath.Join(config.Dir, translogFileName(cp.Generation))
			f, err := os.Open(tlogPath)
			if err != nil {
				return nil, fmt.Errorf("open translog for UUID recovery: %w", err)
			}
			header, _, err := ReadHeader(f)
			f.Close()
			if err != nil {
				return nil, fmt.Errorf("read header for UUID recovery: %w", err)
			}
			translogUUID = header.TranslogUUID
		}
		tl.uuid = translogUUID

		return tl.recoverFromCheckpoint(cp, globalCheckpoint)
	}

	// Fresh translog.
	if translogUUID == "" {
		translogUUID = uuid.New().String()
	}
	tl.uuid = translogUUID

	return tl.createFresh(globalCheckpoint, minGen)
}

// createFresh creates a new translog starting at generation 1.
func (tl *Translog) createFresh(globalCheckpoint int64, minGen int64) (*Translog, error) {
	w, err := tl.createNextWriter(1, minGen, globalCheckpoint)
	if err != nil {
		return nil, err
	}

	tl.current = w
	tl.readers = nil

	return tl, nil
}

// recoverFromCheckpoint recovers readers and creates a new writer.
func (tl *Translog) recoverFromCheckpoint(cp *Checkpoint, globalCheckpoint int64) (*Translog, error) {
	minGen := cp.MinTranslogGeneration
	currentGen := cp.Generation

	// Clean up incomplete rollover: if gen+1 file exists without a committed checkpoint, remove it.
	nextGenPath := filepath.Join(tl.dir, translogFileName(currentGen+1))
	nextGenCkpPath := filepath.Join(tl.dir, checkpointFileName(currentGen+1))
	if _, err := os.Stat(nextGenPath); err == nil {
		if _, err := os.Stat(nextGenCkpPath); err != nil {
			// No committed checkpoint for the next generation — cleanup.
			os.Remove(nextGenPath)
		}
	}

	// Open readers for [minGen, currentGen] in order.
	var readers []*TranslogReader
	for gen := minGen; gen <= currentGen; gen++ {
		genCkpPath := filepath.Join(tl.dir, checkpointFileName(gen))
		var genCp Checkpoint

		if gen == currentGen {
			genCp = *cp // use the live checkpoint for the current generation
		} else {
			ckpData, err := os.ReadFile(genCkpPath)
			if err != nil {
				closeReaders(readers)
				return nil, fmt.Errorf("read checkpoint for gen %d: %w", gen, err)
			}
			parsedCp, err := UnmarshalCheckpoint(ckpData)
			if err != nil {
				closeReaders(readers)
				return nil, fmt.Errorf("unmarshal checkpoint for gen %d: %w", gen, err)
			}
			genCp = *parsedCp
		}

		tlogPath := filepath.Join(tl.dir, translogFileName(gen))
		reader, err := NewTranslogReader(tlogPath, gen, tl.uuid, genCp)
		if err != nil {
			closeReaders(readers)
			return nil, fmt.Errorf("open reader gen %d: %w", gen, err)
		}

		// Validate primary term.
		if reader.header.PrimaryTerm > tl.currentTerm {
			reader.Close()
			closeReaders(readers)
			return nil, fmt.Errorf("gen %d primary term %d > current %d", gen, reader.header.PrimaryTerm, tl.currentTerm)
		}

		readers = append(readers, reader)
	}

	tl.readers = readers

	// Create new writer at generation + 1.
	w, err := tl.createNextWriter(currentGen+1, minGen, globalCheckpoint)
	if err != nil {
		closeReaders(readers)
		return nil, err
	}

	tl.current = w

	return tl, nil
}

// Add writes an operation to the current writer.
func (tl *Translog) Add(op Operation) (Location, error) {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	if tl.tragedy != nil {
		return Location{}, tl.tragedy
	}

	return tl.current.Add(op)
}

// Sync delegates to the writer's Sync.
func (tl *Translog) Sync() error {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	return tl.current.Sync()
}

// RollGeneration closes the current writer into an immutable reader
// and opens a new writer at generation+1.
func (tl *Translog) RollGeneration() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	oldGen := tl.current.Generation()
	genCkpPath := filepath.Join(tl.dir, checkpointFileName(oldGen))

	reader, err := tl.current.CloseIntoReader(genCkpPath)
	if err != nil {
		return fmt.Errorf("close writer into reader: %w", err)
	}
	tl.readers = append(tl.readers, reader)

	// Open new writer at next generation.
	minGen := tl.deletionPolicy.MinTranslogGenRequired()
	w, err := tl.createNextWriter(oldGen+1, minGen, reader.checkpoint.GlobalCheckpoint)
	if err != nil {
		return err
	}

	tl.current = w
	return nil
}

// CurrentGeneration returns the current writer's generation.
func (tl *Translog) CurrentGeneration() int64 {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	return tl.current.Generation()
}

// NewSnapshot creates a filtered, deduplicated snapshot over all generations.
func (tl *Translog) NewSnapshot(fromSeqNo, toSeqNo int64) (Snapshot, error) {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	var snapshots []*TranslogSnapshot

	// Add reader snapshots (older generations).
	for _, r := range tl.readers {
		snapshots = append(snapshots, r.Snapshot())
	}

	// Add current writer snapshot: sync first, then create a temporary reader.
	if err := tl.current.Sync(); err != nil {
		return nil, fmt.Errorf("sync before snapshot: %w", err)
	}

	// For the current writer, we need to read its operations.
	// Open a temporary read handle on the current tlog file.
	writerPath := filepath.Join(tl.dir, translogFileName(tl.current.Generation()))
	writerCp := tl.current.currentCheckpoint()
	header := tl.current.header

	tmpFile, err := os.Open(writerPath)
	if err != nil {
		return nil, fmt.Errorf("open current gen for snapshot: %w", err)
	}

	writerBaseReader := &BaseTranslogReader{
		file:       tmpFile,
		generation: tl.current.Generation(),
		header:     header,
		checkpoint: writerCp,
	}

	writerSnapshot := &TranslogSnapshot{
		reader:            writerBaseReader,
		numOps:            int(writerCp.NumOps),
		currentOp:         0,
		offset:            HeaderSizeInBytes(&header),
		trimmedAboveSeqNo: writerCp.TrimmedAboveSeqNo,
	}
	snapshots = append(snapshots, writerSnapshot)

	multi := NewMultiSnapshot(snapshots)
	filtered := NewSeqNoFilterSnapshot(multi, fromSeqNo, toSeqNo)

	return filtered, nil
}

// SetMinRequiredGeneration advances the safe-to-delete boundary.
func (tl *Translog) SetMinRequiredGeneration(gen int64) {
	tl.deletionPolicy.SetMinRequiredGeneration(gen)
}

// TrimUnreferencedReaders deletes old generations below the minimum required.
func (tl *Translog) TrimUnreferencedReaders() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	minRequired := tl.deletionPolicy.MinTranslogGenRequired()

	var kept []*TranslogReader
	for _, r := range tl.readers {
		if r.generation < minRequired {
			r.Close()
			// Delete .tlog and .ckp files.
			os.Remove(filepath.Join(tl.dir, translogFileName(r.generation)))
			os.Remove(filepath.Join(tl.dir, checkpointFileName(r.generation)))
		} else {
			kept = append(kept, r)
		}
	}
	tl.readers = kept
	return nil
}

// Close closes the writer and all readers.
func (tl *Translog) Close() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	var firstErr error
	if tl.current != nil {
		if err := tl.current.Close(); err != nil {
			firstErr = err
		}
		tl.current = nil
	}
	for _, r := range tl.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	tl.readers = nil
	return firstErr
}

// translogFileName returns the filename for a generation's translog data.
func translogFileName(gen int64) string {
	return fmt.Sprintf("translog-%d.tlog", gen)
}

// checkpointFileName returns the filename for a generation's checkpoint.
func checkpointFileName(gen int64) string {
	return fmt.Sprintf("translog-%d.ckp", gen)
}

// createNextWriter creates a new TranslogWriter at the given generation with an initial checkpoint.
func (tl *Translog) createNextWriter(gen, minGen, globalCheckpoint int64) (*TranslogWriter, error) {
	header := TranslogHeader{
		TranslogUUID: tl.uuid,
		PrimaryTerm:  tl.currentTerm,
	}

	cp := EmptyCheckpoint(gen, minGen)
	cp.GlobalCheckpoint = globalCheckpoint
	cp.Offset = HeaderSizeInBytes(&header)

	ckpPath := filepath.Join(tl.dir, "translog.ckp")
	tlogPath := filepath.Join(tl.dir, translogFileName(gen))

	w, err := NewTranslogWriter(tlogPath, ckpPath, gen, header, *cp)
	if err != nil {
		return nil, fmt.Errorf("create writer gen %d: %w", gen, err)
	}

	if err := WriteCheckpointSync(ckpPath, cp); err != nil {
		w.Close()
		return nil, fmt.Errorf("write checkpoint gen %d: %w", gen, err)
	}

	return w, nil
}

func closeReaders(readers []*TranslogReader) {
	for _, r := range readers {
		r.Close()
	}
}
