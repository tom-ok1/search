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
	NumericDocValues(field string) NumericDocValues
	SortedDocValues(field string) SortedDocValues
	Close() error
}
