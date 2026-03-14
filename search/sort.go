package search

// Sort defines the sort order for search results.
type Sort struct {
	Fields []SortField
}

func NewSort(fields ...SortField) *Sort {
	return &Sort{Fields: fields}
}
