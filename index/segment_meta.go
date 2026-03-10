package index

// SegmentMeta holds segment metadata persisted as JSON.
type SegmentMeta struct {
	Name     string   `json:"name"`
	DocCount int      `json:"doc_count"`
	Fields   []string `json:"fields"`
}
