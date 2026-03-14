package search

// SortFieldType defines how to interpret a sort field.
type SortFieldType int

const (
	SortFieldScore   SortFieldType = iota // sort by relevance score
	SortFieldDoc                          // sort by document ID
	SortFieldNumeric                      // sort by numeric doc values
	SortFieldString                       // sort by sorted doc values ordinal
)

// SortField specifies a single sort criterion.
type SortField struct {
	Field   string
	Type    SortFieldType
	Reverse bool // true = descending
}

// Sort defines the sort order for search results.
type Sort struct {
	Fields []SortField
}

func NewSort(fields ...SortField) *Sort {
	return &Sort{Fields: fields}
}
