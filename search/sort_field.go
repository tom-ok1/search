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
