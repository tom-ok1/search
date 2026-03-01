package index

// IndexReader reads across multiple segments.
type IndexReader struct {
	segments []*Segment
}

// LeafReaderContext holds a single segment and its global DocID base offset.
type LeafReaderContext struct {
	Segment *Segment
	DocBase int // global DocID of the first document in this segment
}

func NewIndexReader(segments []*Segment) *IndexReader {
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
		docBase += seg.docCount
	}
	return leaves
}

// TotalDocCount returns the total number of documents across all segments.
func (r *IndexReader) TotalDocCount() int {
	total := 0
	for _, seg := range r.segments {
		total += seg.docCount
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

// GetStoredFields returns stored fields for a global DocID.
func (r *IndexReader) GetStoredFields(globalDocID int) map[string]string {
	for _, leaf := range r.Leaves() {
		localDocID := globalDocID - leaf.DocBase
		if localDocID >= 0 && localDocID < leaf.Segment.docCount {
			return leaf.Segment.storedFields[localDocID]
		}
	}
	return nil
}
