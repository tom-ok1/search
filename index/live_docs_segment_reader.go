package index

// LiveDocsSegmentReader wraps a SegmentReader with an immutable liveDocs
// bitset snapshot from PendingDeletes. Unlike PendingDeletesSegmentReader,
// the bitset is the complete deletion view (committed + pending).
//
// Close() is a no-op because the underlying DiskSegment is owned by
// ReadersAndUpdates, not by this reader.
type LiveDocsSegmentReader struct {
	inner    SegmentReader
	liveDocs *Bitset // immutable snapshot; set bit = deleted
}

func (r *LiveDocsSegmentReader) Name() string  { return r.inner.Name() }
func (r *LiveDocsSegmentReader) DocCount() int  { return r.inner.DocCount() }

func (r *LiveDocsSegmentReader) IsDeleted(docID int) bool {
	return r.liveDocs.Get(docID)
}

func (r *LiveDocsSegmentReader) LiveDocCount() int {
	return r.inner.DocCount() - r.liveDocs.Count()
}

func (r *LiveDocsSegmentReader) DocFreq(field, term string) int {
	return r.inner.DocFreq(field, term)
}

func (r *LiveDocsSegmentReader) FieldLength(field string, docID int) int {
	return r.inner.FieldLength(field, docID)
}

func (r *LiveDocsSegmentReader) TotalFieldLength(field string) int {
	return r.inner.TotalFieldLength(field)
}

func (r *LiveDocsSegmentReader) StoredFields(docID int) (map[string]string, error) {
	return r.inner.StoredFields(docID)
}

func (r *LiveDocsSegmentReader) PostingsIterator(field, term string) PostingsIterator {
	return r.inner.PostingsIterator(field, term)
}

func (r *LiveDocsSegmentReader) Close() error {
	return nil
}
