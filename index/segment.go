package index

// SegmentReader abstracts read access to a segment.
// Implemented by DiskSegment (mmap-based) and LiveDocsSegmentReader (deletion overlay).
type SegmentReader interface {
	Name() string
	DocCount() int
	LiveDocCount() int
	IsDeleted(docID int) bool
	DocFreq(field, term string) int
	FieldLength(field string, docID int) int
	TotalFieldLength(field string) int
	StoredFields(docID int) (map[string]string, error)
	PostingsIterator(field, term string) PostingsIterator
}

// FieldIndex is the inverted index for a single field.
// It maps term to PostingsList.
type FieldIndex struct {
	postings map[string]*PostingsList
}

func newFieldIndex() *FieldIndex {
	return &FieldIndex{
		postings: make(map[string]*PostingsList),
	}
}

// ---------------------------------------------------------------------------
// InMemorySegment (in-memory buffer)
// ---------------------------------------------------------------------------

// InMemorySegment is the in-memory write buffer used by IndexWriter.
// It accumulates documents until flushed to disk as a DiskSegment.
// It does NOT implement SegmentReader — only DiskSegment is used for reading.
type InMemorySegment struct {
	name         string
	fields       map[string]*FieldIndex
	docCount     int
	storedFields map[int]map[string]string
	fieldLengths map[string][]int
	// Deletion marks: localDocID -> deleted
	deletedDocs map[int]bool
}

func newInMemorySegment(name string) *InMemorySegment {
	return &InMemorySegment{
		name:         name,
		fields:       make(map[string]*FieldIndex),
		storedFields: make(map[int]map[string]string),
		fieldLengths: make(map[string][]int),
		deletedDocs:  make(map[int]bool),
	}
}

// MarkDeleted marks a document as deleted.
func (s *InMemorySegment) MarkDeleted(localDocID int) {
	s.deletedDocs[localDocID] = true
}

// ---------------------------------------------------------------------------
// LiveDocsSegmentReader
// ---------------------------------------------------------------------------

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
