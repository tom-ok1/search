package index

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
