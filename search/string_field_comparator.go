package search

import (
	"cmp"

	"gosearch/index"
)

// StringFieldComparator sorts by sorted doc values, resolving ordinals to
// strings at Copy time so that comparisons work correctly across segments.
type StringFieldComparator struct {
	field    string
	values   []string
	hasValue []bool
}

func NewStringFieldComparator(field string, numSlots int) *StringFieldComparator {
	return &StringFieldComparator{
		field:    field,
		values:   make([]string, numSlots),
		hasValue: make([]bool, numSlots),
	}
}

func (c *StringFieldComparator) CompareSlots(slot1, slot2 int) int {
	h1, h2 := c.hasValue[slot1], c.hasValue[slot2]
	if !h1 && !h2 {
		return 0
	}
	if !h1 {
		return -1
	}
	if !h2 {
		return 1
	}
	return cmp.Compare(c.values[slot1], c.values[slot2])
}

func (c *StringFieldComparator) Value(slot int) any {
	if !c.hasValue[slot] {
		return nil
	}
	return c.values[slot]
}

func (c *StringFieldComparator) GetLeafComparator(seg index.SegmentReader) LeafFieldComparator {
	return &stringLeafComparator{
		parent: c,
		dvs:    seg.SortedDocValues(c.field),
	}
}

type stringLeafComparator struct {
	parent    *StringFieldComparator
	dvs       index.SortedDocValues
	bottom    string
	bottomHas bool
}

func (lc *stringLeafComparator) SetBottom(slot int) {
	lc.bottom = lc.parent.values[slot]
	lc.bottomHas = lc.parent.hasValue[slot]
}

func (lc *stringLeafComparator) CompareBottom(docID int) int {
	val, has := lc.resolveDoc(docID)
	if !lc.bottomHas && !has {
		return 0
	}
	if !lc.bottomHas {
		return -1
	}
	if !has {
		return 1
	}
	return cmp.Compare(lc.bottom, val)
}

func (lc *stringLeafComparator) Copy(slot int, docID int) {
	lc.parent.values[slot], lc.parent.hasValue[slot] = lc.resolveDoc(docID)
}

func (lc *stringLeafComparator) SetScorer(score float64) {}

func (lc *stringLeafComparator) CompetitiveIterator() DocIdSetIterator { return nil }

func (lc *stringLeafComparator) resolveDoc(docID int) (string, bool) {
	if lc.dvs == nil {
		return "", false
	}
	ord, err := lc.dvs.OrdValue(docID)
	if err != nil || ord < 0 {
		return "", false
	}
	val, err := lc.dvs.LookupOrd(ord)
	if err != nil {
		return "", false
	}
	return string(val), true
}
