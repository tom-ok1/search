package index

import "gosearch/store"

// IndexReader reads across multiple segments.
type IndexReader struct {
	segments []SegmentReader
	onClose  func() // called once on Close; nil for standalone readers
	closed   bool
}

// LeafReaderContext holds a single segment and its global DocID base offset.
type LeafReaderContext struct {
	Segment SegmentReader
	DocBase int // global DocID of the first document in this segment
}

func NewIndexReader(segments []SegmentReader) *IndexReader {
	return &IndexReader{segments: segments}
}

// Leaves returns a LeafReaderContext for each segment.
func (r *IndexReader) Leaves() []LeafReaderContext {
	var leaves []LeafReaderContext
	docBase := 0
	for _, seg := range r.segments {
		leaves = append(leaves, LeafReaderContext{
			Segment: seg,
			DocBase: docBase,
		})
		docBase += seg.DocCount()
	}
	return leaves
}

// TotalDocCount returns the total number of documents across all segments.
func (r *IndexReader) TotalDocCount() int {
	total := 0
	for _, seg := range r.segments {
		total += seg.DocCount()
	}
	return total
}

// LiveDocCount returns the total number of non-deleted documents.
func (r *IndexReader) LiveDocCount() int {
	total := 0
	for _, seg := range r.segments {
		liveDocs := seg.LiveDocs()
		if liveDocs != nil {
			total += seg.DocCount() - liveDocs.Count()
		} else {
			total += seg.DocCount()
		}
	}
	return total
}

// Close closes any closeable segments (e.g., DiskSegments with mmap'd files)
// and releases file references held by this reader.
func (r *IndexReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	for _, seg := range r.segments {
		seg.Close()
	}
	if r.onClose != nil {
		r.onClose()
	}
	return nil
}

// findLeaf returns the leaf and local DocID for a global DocID.
func (r *IndexReader) findLeaf(globalDocID int) (LeafReaderContext, int, bool) {
	for _, leaf := range r.Leaves() {
		localDocID := globalDocID - leaf.DocBase
		if localDocID >= 0 && localDocID < leaf.Segment.DocCount() {
			return leaf, localDocID, true
		}
	}
	return LeafReaderContext{}, 0, false
}

// GetStoredFields returns stored fields for a global DocID.
func (r *IndexReader) GetStoredFields(globalDocID int) map[string][]byte {
	leaf, localDocID, ok := r.findLeaf(globalDocID)
	if !ok {
		return nil
	}
	fields, _ := leaf.Segment.StoredFields(localDocID)
	return fields
}

// GetPositions returns term positions for a global DocID in the given field/term.
func (r *IndexReader) GetPositions(globalDocID int, field, term string) []int {
	leaf, localDocID, ok := r.findLeaf(globalDocID)
	if !ok {
		return nil
	}
	iter := leaf.Segment.PostingsIterator(field, term)
	if iter.Advance(localDocID) && iter.DocID() == localDocID {
		return iter.Positions()
	}
	return nil
}

// GetNumericDocValue returns the numeric doc value for a global DocID and field.
// The second return value indicates whether the value was found.
func (r *IndexReader) GetNumericDocValue(globalDocID int, field string) (int64, bool) {
	leaf, localDocID, ok := r.findLeaf(globalDocID)
	if !ok {
		return 0, false
	}
	ndv := leaf.Segment.NumericDocValues(field)
	if ndv == nil {
		return 0, false
	}
	if !ndv.HasValue(localDocID) {
		return 0, false
	}
	val, err := ndv.Get(localDocID)
	if err != nil {
		return 0, false
	}
	return val, true
}

// OpenDirectoryReader opens an IndexReader from a committed index on disk.
// It reads the latest segments_N file and opens each segment as a DiskSegment.
func OpenDirectoryReader(dir store.Directory) (*IndexReader, error) {
	si, err := ReadLatestSegmentInfos(dir)
	if err != nil {
		return nil, err
	}

	segments := make([]SegmentReader, 0, len(si.Segments))
	dirPath := dir.FilePath("")
	closeAll := func() {
		for _, s := range segments {
			s.Close()
		}
	}
	for _, info := range si.Segments {
		seg, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			closeAll()
			return nil, err
		}

		liveDocs, err := loadLiveDocs(dirPath, &SegmentCommitInfo{Name: info.Name, MaxDoc: seg.DocCount()})
		if err != nil {
			seg.Close()
			closeAll()
			return nil, err
		}
		if liveDocs != nil {
			segments = append(segments, &LiveDocsSegmentReader{inner: seg, liveDocs: liveDocs})
		} else {
			segments = append(segments, seg)
		}
	}

	return NewIndexReader(segments), nil
}

// OpenNRTReader opens a near-real-time IndexReader from a writer.
// The in-memory buffer is flushed and pending deletes are resolved,
// producing a point-in-time snapshot. Subsequent writes to the writer
// are not visible through the returned reader.
func OpenNRTReader(w *IndexWriter) (*IndexReader, error) {
	segs, files, err := w.nrtSegments()
	if err != nil {
		return nil, err
	}
	reader := NewIndexReader(segs)
	reader.onClose = func() {
		w.DecRefDeleter(files)
	}
	return reader, nil
}
