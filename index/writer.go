package index

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// IndexWriter adds documents to the index and manages segments.
// It is safe for concurrent use by multiple goroutines.
type IndexWriter struct {
	mu             sync.Mutex
	dir            store.Directory
	fieldAnalyzers *analysis.FieldAnalyzers
	segmentInfos   *SegmentInfos
	segmentCounter int32
	readerMap      map[string]*ReadersAndUpdates
	mergePolicy    MergePolicy
	docWriter      *DocumentsWriter
	fileDeleter    *FileDeleter
	infoStream     InfoStream
	metrics        *IndexWriterMetrics
}

// NewIndexWriter creates a new IndexWriter. bufferSize controls the approximate
// number of documents buffered in RAM before auto-flushing (converted internally
// to a RAM byte threshold).
func NewIndexWriter(dir store.Directory, fieldAnalyzers *analysis.FieldAnalyzers, bufferSize int) *IndexWriter {
	w := &IndexWriter{
		dir:            dir,
		fieldAnalyzers: fieldAnalyzers,
		readerMap:      make(map[string]*ReadersAndUpdates),
		fileDeleter:    NewFileDeleter(dir),
		infoStream:     &NoOpInfoStream{},
		metrics:        &IndexWriterMetrics{},
	}

	// Try to load existing committed state from the directory.
	si, err := ReadLatestSegmentInfos(dir)
	if err == nil {
		w.segmentInfos = si
		for _, info := range si.Segments {
			var n int
			if _, parseErr := fmt.Sscanf(info.Name, "_seg%d", &n); parseErr == nil {
				if int32(n+1) > w.segmentCounter {
					w.segmentCounter = int32(n + 1)
				}
			}
		}
	} else {
		w.segmentInfos = NewSegmentInfos()
	}

	// Clean up any stale pending_segments_* files from prior crashes.
	if files, err := dir.ListAll(); err == nil {
		for _, f := range files {
			if strings.HasPrefix(f, "pending_segments_") {
				_ = dir.DeleteFile(f)
			}
		}
	}

	// Use a large default RAM buffer; the bufferSize parameter controls
	// max docs per DWPT for deterministic flush behavior.
	const defaultRAMBufferSize = 256 * 1024 * 1024 // 256 MB

	w.docWriter = newDocumentsWriter(dir, fieldAnalyzers, defaultRAMBufferSize, bufferSize, func() string {
		return w.nextSegmentName()
	})
	w.docWriter.metrics = w.metrics
	w.docWriter.flushControl.metrics = w.metrics
	w.docWriter.onSegmentFlushed = func(info *SegmentCommitInfo) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.segmentInfos.Segments = append(w.segmentInfos.Segments, info)
		w.segmentInfos.Version++
	}
	w.docWriter.onGlobalUpdates = func(updates *FrozenBufferedUpdates) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.applyFrozenUpdates(updates)
	}

	return w
}

// SetMergePolicy sets the merge policy for automatic merging.
func (w *IndexWriter) SetMergePolicy(policy MergePolicy) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mergePolicy = policy
}

// SetInfoStream sets the InfoStream for diagnostic logging.
func (w *IndexWriter) SetInfoStream(infoStream InfoStream) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.infoStream = infoStream
	w.docWriter.setInfoStream(infoStream)
}

// Metrics returns the IndexWriterMetrics for monitoring.
func (w *IndexWriter) Metrics() *IndexWriterMetrics {
	return w.metrics
}

// autoMerge runs the merge policy if one is configured.
func (w *IndexWriter) autoMerge() error {
	if w.mergePolicy == nil {
		return nil
	}
	return w.MaybeMerge(w.mergePolicy)
}

func (w *IndexWriter) nextSegmentName() string {
	n := atomic.AddInt32(&w.segmentCounter, 1)
	return fmt.Sprintf("_seg%d", n-1)
}

// AddDocument adds a document to the index. Safe for concurrent use.
func (w *IndexWriter) AddDocument(doc *document.Document) error {
	return w.docWriter.addDocument(doc)
}

// Flush writes all in-memory buffers to disk as new segments.
func (w *IndexWriter) Flush() error {
	if err := w.docWriter.flushAllThreads(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	frozen := w.docWriter.freezeGlobalBuffer()
	w.applyFrozenUpdates(frozen)

	return w.autoMerge()
}

// Commit flushes any buffered data, resolves pending deletes, and writes
// the segment metadata (segments_N) to disk.
func (w *IndexWriter) Commit() error {
	// 1. Flush all threads
	if err := w.docWriter.flushAllThreads(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 2. Freeze and apply any remaining global deletes
	frozen := w.docWriter.freezeGlobalBuffer()
	if err := w.applyFrozenUpdates(frozen); err != nil {
		return err
	}

	// 3. Write deletion bitmaps and collect .del file names
	for _, info := range w.segmentInfos.Segments {
		rau, ok := w.readerMap[info.Name]
		if ok && rau.HasPendingDeletes() {
			delFile, err := rau.WriteLiveDocs(w.dir)
			if err != nil {
				return fmt.Errorf("write deletions for %s: %w", info.Name, err)
			}
			if delFile != "" && !slices.Contains(info.Files, delFile) {
				info.Files = append(info.Files, delFile)
			}
		}
	}

	// 4. Collect all segment files and fsync them
	var allFiles []string
	for _, info := range w.segmentInfos.Segments {
		allFiles = append(allFiles, info.Files...)
	}
	if err := w.dir.Sync(allFiles); err != nil {
		return fmt.Errorf("sync segment files: %w", err)
	}

	// 5. Write pending_segments_N
	pendingName, finalName, err := w.segmentInfos.WritePending(w.dir)
	if err != nil {
		return err
	}

	// 6. Fsync the pending file
	if err := w.dir.Sync([]string{pendingName}); err != nil {
		return fmt.Errorf("sync %s: %w", pendingName, err)
	}

	// 7. Atomic rename: pending_segments_N → segments_N
	if err := w.dir.Rename(pendingName, finalName); err != nil {
		return fmt.Errorf("rename %s → %s: %w", pendingName, finalName, err)
	}

	// 8. Fsync directory to make the rename durable
	if err := w.dir.SyncMetaData(); err != nil {
		return err
	}

	// 9. Best-effort cleanup of stale files
	w.deleteStaleFiles()

	// 10. Trigger auto-merge
	return w.autoMerge()
}

// applyFrozenUpdates resolves frozen delete terms against all existing disk segments.
func (w *IndexWriter) applyFrozenUpdates(frozen *FrozenBufferedUpdates) error {
	if frozen == nil || !frozen.any() {
		return nil
	}
	for _, info := range w.segmentInfos.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for delete: %w", info.Name, err)
		}
		for _, dt := range frozen.deleteTerms {
			iter := reader.PostingsIterator(dt.Field, dt.Term)
			for iter.Next() {
				rau.Delete(iter.DocID())
			}
		}
	}
	return nil
}

// getOrCreateRAU returns the ReadersAndUpdates for the given segment.
func (w *IndexWriter) getOrCreateRAU(info *SegmentCommitInfo) *ReadersAndUpdates {
	rau, ok := w.readerMap[info.Name]
	if !ok {
		rau = NewReadersAndUpdates(info, w.dir.FilePath(""))
		w.readerMap[info.Name] = rau
	}
	return rau
}

// nrtSegments returns all segments for near-real-time reading.
// It opens independent DiskSegments (not shared with the writer) and
// increments file reference counts so that deleteStaleFiles will not
// remove files while the returned readers are in use.
// The caller must call DecRefDeleter(files) when the reader is closed.
func (w *IndexWriter) nrtSegments() ([]SegmentReader, []string, error) {
	// Flush all threads
	if err := w.docWriter.flushAllThreads(); err != nil {
		return nil, nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	frozen := w.docWriter.freezeGlobalBuffer()
	if err := w.applyFrozenUpdates(frozen); err != nil {
		return nil, nil, err
	}

	// Trigger auto-merge
	if err := w.autoMerge(); err != nil {
		return nil, nil, err
	}

	// Snapshot referenced files and protect them
	files := w.segmentInfos.AllFiles()
	w.fileDeleter.IncRef(files)

	segs := make([]SegmentReader, 0, len(w.segmentInfos.Segments))
	for _, info := range w.segmentInfos.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			// Close already opened segments and release refs
			for _, s := range segs {
				s.Close()
			}
			w.fileDeleter.DecRef(files)
			return nil, nil, err
		}
		reader.IncRef()

		// Apply deletion overlay if the segment has deletions
		liveDocs := rau.GetLiveDocs()
		if liveDocs != nil {
			segs = append(segs, &LiveDocsSegmentReader{inner: reader, liveDocs: liveDocs})
		} else {
			segs = append(segs, reader)
		}
	}
	return segs, files, nil
}

// DeleteDocuments buffers a delete-by-term operation.
func (w *IndexWriter) DeleteDocuments(field, term string) error {
	w.docWriter.deleteDocuments(field, term)
	return nil
}

// MaybeMerge runs the given merge policy and executes any suggested merges.
// Caller must hold w.mu.
func (w *IndexWriter) MaybeMerge(policy MergePolicy) error {
	candidates := policy.FindMerges(w.segmentInfos.Segments)
	for _, candidate := range candidates {
		if err := w.executeMerge(candidate); err != nil {
			return err
		}
	}
	return nil
}

// ForceMerge merges all segments into at most maxSegments segments.
func (w *IndexWriter) ForceMerge(maxSegments int) error {
	if maxSegments < 1 {
		maxSegments = 1
	}
	if err := w.Flush(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for len(w.segmentInfos.Segments) > maxSegments {
		candidate := MergeCandidate{
			Segments: make([]*SegmentCommitInfo, len(w.segmentInfos.Segments)),
		}
		copy(candidate.Segments, w.segmentInfos.Segments)
		if err := w.executeMerge(candidate); err != nil {
			return err
		}
	}

	// Clean up orphaned files from merged-away segments immediately,
	// rather than waiting for the next Commit.
	w.deleteStaleFiles()

	return nil
}

// executeMerge performs a single merge operation. Caller must hold w.mu.
func (w *IndexWriter) executeMerge(candidate MergeCandidate) error {
	if len(candidate.Segments) < 2 {
		return nil
	}

	inputs := make([]MergeInput, len(candidate.Segments))
	for i, info := range candidate.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for merge: %w", info.Name, err)
		}
		inputs[i] = MergeInput{
			Segment:   reader,
			IsDeleted: rau.IsDeleted,
		}
	}

	newName := w.nextSegmentName()
	result, err := MergeSegmentsToDisk(w.dir, inputs, newName)
	if err != nil {
		return fmt.Errorf("merge segments: %w", err)
	}

	newInfo := &SegmentCommitInfo{
		Name:   newName,
		MaxDoc: result.DocCount,
		Fields: result.Fields,
		Files:  result.Files,
	}

	mergedNames := make(map[string]bool)
	for _, info := range candidate.Segments {
		mergedNames[info.Name] = true
	}

	var remaining []*SegmentCommitInfo
	for _, info := range w.segmentInfos.Segments {
		if mergedNames[info.Name] {
			if rau, ok := w.readerMap[info.Name]; ok {
				rau.Close()
				delete(w.readerMap, info.Name)
			}
			continue
		}
		remaining = append(remaining, info)
	}
	remaining = append(remaining, newInfo)
	w.segmentInfos.Segments = remaining
	w.segmentInfos.Version++

	return nil
}

// Close releases all resources held by the IndexWriter.
func (w *IndexWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, rau := range w.readerMap {
		rau.Close()
		delete(w.readerMap, name)
	}
	return nil
}

// IncRefDeleter increments reference counts for the given files,
// preventing them from being deleted by deleteStaleFiles.
func (w *IndexWriter) IncRefDeleter(files []string) {
	w.fileDeleter.IncRef(files)
}

// DecRefDeleter decrements reference counts for the given files.
// Files whose count reaches 0 and are pending deletion are deleted immediately.
func (w *IndexWriter) DecRefDeleter(files []string) {
	w.fileDeleter.DecRef(files)
}

// deleteStaleFiles removes old segments_N files, stale pending_segments_* files,
// and orphaned segment data files not referenced by the current commit.
// Files with active reader references are deferred until those readers close.
func (w *IndexWriter) deleteStaleFiles() {
	files, err := w.dir.ListAll()
	if err != nil {
		return
	}

	refs := w.segmentInfos.ReferencedFiles()

	var toDelete []string
	for _, f := range files {
		switch {
		case isOldSegmentsFile(f, w.segmentInfos.Generation):
			toDelete = append(toDelete, f)
		case strings.HasPrefix(f, "pending_segments_"):
			toDelete = append(toDelete, f)
		case strings.HasPrefix(f, "_seg") && !refs[f]:
			toDelete = append(toDelete, f)
		}
	}
	if len(toDelete) > 0 {
		w.fileDeleter.DeleteIfUnreferenced(toDelete)
	}
}

// isOldSegmentsFile returns true if f is a segments_N file with generation < current.
func isOldSegmentsFile(f string, currentGen int64) bool {
	if !strings.HasPrefix(f, "segments_") {
		return false
	}
	var gen int64
	if _, err := fmt.Sscanf(f, "segments_%d", &gen); err == nil {
		return gen < currentGen
	}
	return false
}
