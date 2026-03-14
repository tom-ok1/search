package search

import "gosearch/index"

// FieldComparator compares documents by field values for sorting.
// It manages K "slots" (one per position in the top-K results) and
// operates at the global (cross-segment) level.
type FieldComparator interface {
	// CompareSlots compares two already-collected slots.
	CompareSlots(slot1, slot2 int) int
	// Value returns the sort value stored in the given slot.
	Value(slot int) interface{}
	// GetLeafComparator returns a segment-local comparator for the given segment.
	GetLeafComparator(seg index.SegmentReader) LeafFieldComparator
}
