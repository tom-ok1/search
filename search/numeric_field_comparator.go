package search

import (
	"cmp"

	"gosearch/index"
)

// NumericFieldComparator sorts by numeric doc values.
type NumericFieldComparator struct {
	field  string
	values []int64
}

func NewNumericFieldComparator(field string, numSlots int) *NumericFieldComparator {
	return &NumericFieldComparator{
		field:  field,
		values: make([]int64, numSlots),
	}
}

func (c *NumericFieldComparator) CompareSlots(slot1, slot2 int) int {
	return cmp.Compare(c.values[slot1], c.values[slot2])
}

func (c *NumericFieldComparator) Value(slot int) any {
	return c.values[slot]
}

func (c *NumericFieldComparator) GetLeafComparator(seg index.SegmentReader) LeafFieldComparator {
	return &numericLeafComparator{
		parent: c,
		dvs:    seg.NumericDocValues(c.field),
	}
}

type numericLeafComparator struct {
	parent *NumericFieldComparator
	dvs    index.NumericDocValues
	bottom int64
}

func (lc *numericLeafComparator) SetBottom(slot int) {
	lc.bottom = lc.parent.values[slot]
}

func (lc *numericLeafComparator) CompareBottom(docID int) int {
	v := lc.getValueForDoc(docID)
	return cmp.Compare(lc.bottom, v)
}

func (lc *numericLeafComparator) Copy(slot int, docID int) {
	lc.parent.values[slot] = lc.getValueForDoc(docID)
}

func (lc *numericLeafComparator) SetScorer(score float64) {}

func (lc *numericLeafComparator) getValueForDoc(docID int) int64 {
	if lc.dvs == nil {
		return 0
	}
	v, err := lc.dvs.Get(docID)
	if err != nil {
		return 0
	}
	return v
}
