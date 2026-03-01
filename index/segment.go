package index

// Segment is an immutable unit of the index.
// Once created, it is never modified (except for deletion marks).
type Segment struct {
	name         string
	fields       map[string]*FieldIndex
	docCount     int
	storedFields map[int]map[string]string
	fieldLengths map[string][]int
	// Deletion marks: localDocID -> deleted
	deletedDocs map[int]bool
}

func newSegment(name string) *Segment {
	return &Segment{
		name:         name,
		fields:       make(map[string]*FieldIndex),
		storedFields: make(map[int]map[string]string),
		fieldLengths: make(map[string][]int),
		deletedDocs:  make(map[int]bool),
	}
}

// IsDeleted reports whether the document is marked as deleted.
func (s *Segment) IsDeleted(localDocID int) bool {
	return s.deletedDocs[localDocID]
}

// LiveDocCount returns the number of non-deleted documents.
func (s *Segment) LiveDocCount() int {
	return s.docCount - len(s.deletedDocs)
}

// MarkDeleted marks a document as deleted.
// The segment itself is immutable, but deletion info is managed separately.
func (s *Segment) MarkDeleted(localDocID int) {
	s.deletedDocs[localDocID] = true
}

// DocCount returns the total number of documents (including deleted).
func (s *Segment) DocCount() int {
	return s.docCount
}

// GetPostings returns the postings list for a term in the given field.
func (s *Segment) GetPostings(fieldName, term string) *PostingsList {
	fi, exists := s.fields[fieldName]
	if !exists {
		return nil
	}
	return fi.postings[term]
}

// GetStoredFields returns the stored fields for a local document ID.
func (s *Segment) GetStoredFields(localDocID int) map[string]string {
	return s.storedFields[localDocID]
}

// GetFieldLengths returns the token count slice for a field.
func (s *Segment) GetFieldLengths(fieldName string) []int {
	return s.fieldLengths[fieldName]
}
