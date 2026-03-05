package index

// IndexReader reads across multiple segments.
type IndexReader struct {
	segments []SegmentReader
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
		total += seg.LiveDocCount()
	}
	return total
}

// Close closes any closeable segments (e.g., DiskSegments with mmap'd files).
func (r *IndexReader) Close() error {
	for _, seg := range r.segments {
		if c, ok := seg.(interface{ Close() error }); ok {
			c.Close()
		}
	}
	return nil
}

// GetStoredFields returns stored fields for a global DocID.
func (r *IndexReader) GetStoredFields(globalDocID int) map[string]string {
	for _, leaf := range r.Leaves() {
		localDocID := globalDocID - leaf.DocBase
		if localDocID >= 0 && localDocID < leaf.Segment.DocCount() {
			fields, _ := leaf.Segment.StoredFields(localDocID)
			return fields
		}
	}
	return nil
}
