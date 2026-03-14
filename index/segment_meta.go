package index

// SegmentMeta holds segment metadata persisted as JSON.
type SegmentMeta struct {
	Name            string   `json:"name"`
	DocCount        int      `json:"doc_count"`
	Fields          []string `json:"fields"`
	NumericDVFields []string `json:"numeric_dv_fields,omitempty"`
	SortedDVFields  []string `json:"sorted_dv_fields,omitempty"`
}
