package index

// LiveDocsSegmentReader wraps a SegmentReader with an immutable liveDocs
// bitset snapshot from PendingDeletes. Unlike PendingDeletesSegmentReader,
// the bitset is the complete deletion view (committed + pending).
type LiveDocsSegmentReader struct {
	inner    SegmentReader
	liveDocs *Bitset // immutable snapshot; set bit = deleted
}

func (r *LiveDocsSegmentReader) Name() string  { return r.inner.Name() }
func (r *LiveDocsSegmentReader) DocCount() int { return r.inner.DocCount() }

func (r *LiveDocsSegmentReader) LiveDocs() *Bitset {
	return r.liveDocs
}

func (r *LiveDocsSegmentReader) DocFreq(field, term string) int {
	count := 0
	iter := r.inner.PostingsIterator(field, term)
	for iter.Next() {
		if !r.liveDocs.Get(iter.DocID()) {
			count++
		}
	}
	return count
}

func (r *LiveDocsSegmentReader) FieldLength(field string, docID int) int {
	return r.inner.FieldLength(field, docID)
}

func (r *LiveDocsSegmentReader) TotalFieldLength(field string) int {
	total := 0
	for i := 0; i < r.inner.DocCount(); i++ {
		if !r.liveDocs.Get(i) {
			total += r.inner.FieldLength(field, i)
		}
	}
	return total
}

func (r *LiveDocsSegmentReader) StoredFields(docID int) (map[string][]byte, error) {
	return r.inner.StoredFields(docID)
}

func (r *LiveDocsSegmentReader) PostingsIterator(field, term string) PostingsIterator {
	return r.inner.PostingsIterator(field, term)
}

func (r *LiveDocsSegmentReader) NumericDocValues(field string) NumericDocValues {
	return r.inner.NumericDocValues(field)
}

func (r *LiveDocsSegmentReader) DocValuesSkipper(field string) *DocValuesSkipper {
	return r.inner.DocValuesSkipper(field)
}

func (r *LiveDocsSegmentReader) SortedDocValues(field string) SortedDocValues {
	return r.inner.SortedDocValues(field)
}

func (r *LiveDocsSegmentReader) PointValues(field string) PointValues {
	return r.inner.PointValues(field)
}

func (r *LiveDocsSegmentReader) PointFields() map[string]struct{} {
	return r.inner.PointFields()
}

func (r *LiveDocsSegmentReader) Close() error {
	return r.inner.Close()
}
