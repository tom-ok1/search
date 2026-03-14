package index

import (
	"fmt"
	"sort"
	"strings"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// DeleteTerm represents a buffered delete-by-term operation.
// Actual resolution (finding matching docIDs) is deferred until Commit.
type DeleteTerm struct {
	Field string
	Term  string
}

// IndexWriter adds documents to the index and manages segments.
type IndexWriter struct {
	dir                store.Directory
	analyzer           *analysis.Analyzer
	segmentInfos       *SegmentInfos
	buffer             *InMemorySegment // in-memory buffer (not yet a segment)
	bufferSize         int              // flush threshold (number of documents)
	segmentCounter     int
	pendingDeleteTerms []DeleteTerm                  // buffered deletes, resolved at commit time
	readerMap          map[string]*ReadersAndUpdates // per-segment reader + pending deletes
}

func NewIndexWriter(dir store.Directory, analyzer *analysis.Analyzer, bufferSize int) *IndexWriter {
	w := &IndexWriter{
		dir:        dir,
		analyzer:   analyzer,
		bufferSize: bufferSize,
		readerMap:  make(map[string]*ReadersAndUpdates),
	}

	// Try to load existing committed state from the directory.
	si, err := ReadLatestSegmentInfos(dir)
	if err == nil {
		w.segmentInfos = si
		// Set segmentCounter past the highest existing segment number
		// to avoid name collisions.
		for _, info := range si.Segments {
			var n int
			if _, parseErr := fmt.Sscanf(info.Name, "_seg%d", &n); parseErr == nil {
				if n+1 > w.segmentCounter {
					w.segmentCounter = n + 1
				}
			}
		}
	} else {
		// No existing state — start fresh.
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

	w.buffer = newInMemorySegment(w.nextSegmentName())
	return w
}

func (w *IndexWriter) nextSegmentName() string {
	name := fmt.Sprintf("_seg%d", w.segmentCounter)
	w.segmentCounter++
	return name
}

// AddDocument adds a document to the in-memory buffer.
// When the buffer reaches the threshold, it is automatically flushed.
func (w *IndexWriter) AddDocument(doc *document.Document) error {
	docID := w.buffer.docCount
	w.buffer.docCount++

	for _, field := range doc.Fields {
		switch field.Type {
		case document.FieldTypeText:
			tokens, err := w.analyzer.Analyze(field.Value)
			if err != nil {
				return err
			}
			fi := w.getOrCreateFieldIndex(w.buffer, field.Name)

			// Extend the fieldLengths slice with zeros so it can be indexed by docID.
			if w.buffer.fieldLengths[field.Name] == nil {
				w.buffer.fieldLengths[field.Name] = make([]int, 0)
			}
			for len(w.buffer.fieldLengths[field.Name]) <= docID {
				w.buffer.fieldLengths[field.Name] = append(w.buffer.fieldLengths[field.Name], 0)
			}
			w.buffer.fieldLengths[field.Name][docID] = len(tokens)

			// Build postings
			termInfo := make(map[string]*Posting)
			for _, token := range tokens {
				posting, exists := termInfo[token.Term]
				if !exists {
					posting = &Posting{DocID: docID}
					termInfo[token.Term] = posting
				}
				posting.Freq++
				posting.Positions = append(posting.Positions, token.Position)
			}

			for term, posting := range termInfo {
				pl, exists := fi.postings[term]
				if !exists {
					pl = &PostingsList{Term: term}
					fi.postings[term] = pl
				}
				pl.Postings = append(pl.Postings, *posting)
			}

		case document.FieldTypeKeyword:
			fi := w.getOrCreateFieldIndex(w.buffer, field.Name)
			pl, exists := fi.postings[field.Value]
			if !exists {
				pl = &PostingsList{Term: field.Value}
				fi.postings[field.Value] = pl
			}
			pl.Postings = append(pl.Postings, Posting{
				DocID: docID, Freq: 1, Positions: []int{0},
			})

		case document.FieldTypeNumericDocValues:
			// Fill the numericDocValues slice with zeros up to docID, then set the value.
			vals := w.buffer.numericDocValues[field.Name]
			if len(vals) <= docID {
				vals = append(vals, make([]int64, docID+1-len(vals))...)
			}
			vals[docID] = field.NumericValue
			w.buffer.numericDocValues[field.Name] = vals

		case document.FieldTypeSortedDocValues:
			// Fill the sortedDocValues slice with empty strings up to docID, then set the value.
			svals := w.buffer.sortedDocValues[field.Name]
			if len(svals) <= docID {
				svals = append(svals, make([]string, docID+1-len(svals))...)
			}
			svals[docID] = field.Value
			w.buffer.sortedDocValues[field.Name] = svals
		}

		// Stored fields
		if field.Type == document.FieldTypeStored || field.Type == document.FieldTypeText {
			if w.buffer.storedFields[docID] == nil {
				w.buffer.storedFields[docID] = make(map[string]string)
			}
			w.buffer.storedFields[docID][field.Name] = field.Value
		}
	}

	// Auto flush
	if w.buffer.docCount >= w.bufferSize {
		if err := w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Flush writes the in-memory buffer to disk as a new segment.
func (w *IndexWriter) Flush() error {
	if w.buffer.docCount == 0 {
		return nil
	}

	// Write segment files to disk (no fsync yet)
	files, err := WriteSegmentV2(w.dir, w.buffer)
	if err != nil {
		return fmt.Errorf("flush segment %s: %w", w.buffer.name, err)
	}

	// Build SegmentCommitInfo
	fields := make([]string, 0, len(w.buffer.fields))
	for f := range w.buffer.fields {
		fields = append(fields, f)
	}
	sort.Strings(fields)

	info := &SegmentCommitInfo{
		Name:   w.buffer.name,
		MaxDoc: w.buffer.docCount,
		Fields: fields,
		Files:  files,
	}

	// Transfer any buffer deletions to a ReadersAndUpdates
	if len(w.buffer.deletedDocs) > 0 {
		rau := NewReadersAndUpdates(info, w.dir.FilePath(""))
		for docID := range w.buffer.deletedDocs {
			rau.Delete(docID)
		}
		w.readerMap[info.Name] = rau
	}

	w.segmentInfos.Segments = append(w.segmentInfos.Segments, info)
	w.segmentInfos.Version++

	// Apply pending deletes to all existing disk segments
	if err := w.applyPendingDeletes(); err != nil {
		return err
	}

	// Reset buffer — old buffer is now GC'd
	w.buffer = newInMemorySegment(w.nextSegmentName())
	return nil
}

// Commit flushes any buffered data, resolves pending deletes, and writes
// the segment metadata (segments_N) to disk using Lucene's multi-stage
func (w *IndexWriter) Commit() error {
	// 1. Flush buffered data to disk (no fsync)
	if err := w.Flush(); err != nil {
		return err
	}

	// 2. Resolve buffered delete terms against disk segments
	if err := w.applyPendingDeletes(); err != nil {
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
			if delFile != "" && !containsString(info.Files, delFile) {
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

	return nil
}

// applyPendingDeletes resolves all buffered delete terms against disk segments.
// Each segment is opened once via ReadersAndUpdates and all pending terms are applied.
func (w *IndexWriter) applyPendingDeletes() error {
	if len(w.pendingDeleteTerms) == 0 {
		return nil
	}

	for _, info := range w.segmentInfos.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for delete: %w", info.Name, err)
		}

		for _, dt := range w.pendingDeleteTerms {
			iter := reader.PostingsIterator(dt.Field, dt.Term)
			for iter.Next() {
				rau.Delete(iter.DocID())
			}
		}
	}

	w.pendingDeleteTerms = nil
	return nil
}

// getOrCreateRAU returns the ReadersAndUpdates for the given segment,
// creating one if it doesn't exist yet.
func (w *IndexWriter) getOrCreateRAU(info *SegmentCommitInfo) *ReadersAndUpdates {
	rau, ok := w.readerMap[info.Name]
	if !ok {
		rau = NewReadersAndUpdates(info, w.dir.FilePath(""))
		w.readerMap[info.Name] = rau
	}
	return rau
}

// nrtSegments returns all segments for near-real-time reading.
// Like Lucene's IndexWriter.getReader(), the in-memory buffer is flushed
// to an immutable disk segment first, then pending deletes are resolved.
// The returned readers form a point-in-time snapshot; subsequent writes
// to the IndexWriter are not visible through them.
func (w *IndexWriter) nrtSegments() ([]SegmentReader, error) {
	// Flush the in-memory buffer so all data is in immutable disk segments.
	if err := w.Flush(); err != nil {
		return nil, err
	}

	// Resolve buffered delete terms against all segments (including newly flushed).
	if err := w.applyPendingDeletes(); err != nil {
		return nil, err
	}

	segs := make([]SegmentReader, 0, len(w.segmentInfos.Segments))
	for _, info := range w.segmentInfos.Segments {
		rau := w.getOrCreateRAU(info)
		sr, err := rau.GetSegmentReader()
		if err != nil {
			return nil, err
		}
		segs = append(segs, sr)
	}
	return segs, nil
}

// DeleteDocuments buffers a delete-by-term operation.
// Disk segments are NOT opened here — resolution is deferred to Commit().
// Only the in-memory buffer is resolved immediately (since it's already in RAM).
func (w *IndexWriter) DeleteDocuments(field, term string) error {
	// Buffer the delete term for deferred resolution against disk segments
	w.pendingDeleteTerms = append(w.pendingDeleteTerms, DeleteTerm{Field: field, Term: term})

	// In-memory buffer: resolve immediately (no disk I/O, already in RAM)
	if w.buffer.docCount > 0 {
		fi, exists := w.buffer.fields[field]
		if exists {
			pl, exists := fi.postings[term]
			if exists {
				for _, posting := range pl.Postings {
					w.buffer.MarkDeleted(posting.DocID)
				}
			}
		}
	}
	return nil
}

// MaybeMerge runs the given merge policy and executes any suggested merges.
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
	if err := w.applyPendingDeletes(); err != nil {
		return err
	}

	for len(w.segmentInfos.Segments) > maxSegments {
		candidate := MergeCandidate{
			Segments: make([]*SegmentCommitInfo, len(w.segmentInfos.Segments)),
		}
		copy(candidate.Segments, w.segmentInfos.Segments)
		if err := w.executeMerge(candidate); err != nil {
			return err
		}
	}
	return nil
}

// executeMerge performs a single merge operation.
func (w *IndexWriter) executeMerge(candidate MergeCandidate) error {
	if len(candidate.Segments) < 2 {
		return nil
	}

	// Build MergeInputs from RAUs
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

	// Merge and write directly to disk (streaming).
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

	// Remove merged segments from segmentInfos and close their RAUs
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
// All RAUs (and their underlying DiskSegments) are closed here.
func (w *IndexWriter) Close() error {
	for name, rau := range w.readerMap {
		rau.Close()
		delete(w.readerMap, name)
	}
	return nil
}

// deleteStaleFiles removes old segments_N files, stale pending_segments_* files,
// and orphaned segment data files not referenced by the current commit.
// All deletions are best-effort — errors are silently ignored.
func (w *IndexWriter) deleteStaleFiles() {
	files, err := w.dir.ListAll()
	if err != nil {
		return
	}

	refs := w.segmentInfos.ReferencedFiles()

	for _, f := range files {
		switch {
		case isOldSegmentsFile(f, w.segmentInfos.Generation):
			_ = w.dir.DeleteFile(f)
		case strings.HasPrefix(f, "pending_segments_"):
			_ = w.dir.DeleteFile(f)
		case strings.HasPrefix(f, "_seg") && !refs[f]:
			_ = w.dir.DeleteFile(f)
		}
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

// containsString reports whether slice contains s.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func (w *IndexWriter) getOrCreateFieldIndex(seg *InMemorySegment, fieldName string) *FieldIndex {
	fi, exists := seg.fields[fieldName]
	if !exists {
		fi = newFieldIndex()
		seg.fields[fieldName] = fi
	}
	return fi
}
