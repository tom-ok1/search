package index

// SegmentReader abstracts read access to a segment.
// Implemented by DiskSegment (mmap-based) and LiveDocsSegmentReader (deletion overlay).
type SegmentReader interface {
	Name() string
	DocCount() int
	// LiveDocs returns a bitset where set bits represent deleted documents.
	// Returns nil if all documents are alive (no deletions).
	// Lucene equivalent: SegmentReader.getLiveDocs()
	LiveDocs() *Bitset
	DocFreq(field, term string) int
	FieldLength(field string, docID int) int
	TotalFieldLength(field string) int
	StoredFields(docID int) (map[string][]byte, error)
	PostingsIterator(field, term string) PostingsIterator
	NumericDocValues(field string) NumericDocValues
	DocValuesSkipper(field string) *DocValuesSkipper
	SortedDocValues(field string) SortedDocValues
	PointFields() map[string]struct{}
	Close() error
}
