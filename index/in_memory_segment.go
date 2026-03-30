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
	storedFields map[int]map[string][]byte
	fieldLengths map[string][]int
	// Doc values buffers
	numericDocValues map[string][]int64  // field -> values indexed by docID
	sortedDocValues  map[string][]string // field -> values indexed by docID
	// Point fields: tracks which numeric doc values fields are point-indexed
	pointFields map[string]struct{}
}

func newInMemorySegment(name string) *InMemorySegment {
	return &InMemorySegment{
		name:             name,
		fields:           make(map[string]*FieldIndex),
		storedFields:     make(map[int]map[string][]byte, 0),
		fieldLengths:     make(map[string][]int),
		numericDocValues: make(map[string][]int64),
		sortedDocValues:  make(map[string][]string),
		pointFields:      make(map[string]struct{}),
	}
}
