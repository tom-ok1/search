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
		parent:  c,
		dvs:     seg.SortedDocValues(c.field),
		skipper: seg.DocValuesSkipper(c.field),
	}
}

type stringLeafComparator struct {
	parent          *StringFieldComparator
	dvs             index.SortedDocValues
	skipper         *index.DocValuesSkipper
	bottom          string
	bottomHas       bool
	competitiveIter DocIdSetIterator
	iterDirty       bool
}

func (lc *stringLeafComparator) SetBottom(slot int) {
	newBottom := lc.parent.values[slot]
	newHas := lc.parent.hasValue[slot]
	if lc.bottomHas == newHas && lc.bottom == newBottom {
		return
	}
	lc.bottom = newBottom
	lc.bottomHas = newHas
	lc.iterDirty = true
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

func (lc *stringLeafComparator) CompetitiveIterator() DocIdSetIterator {
	if lc.iterDirty {
		lc.rebuildCompetitiveIterator()
		lc.iterDirty = false
	}
	return lc.competitiveIter
}

func (lc *stringLeafComparator) rebuildCompetitiveIterator() {
	if lc.skipper == nil || lc.dvs == nil || !lc.bottomHas {
		return
	}

	// Convert bottom string to ordinal. If exact match, use that ord;
	// if not found, LookupTerm returns -(insertionPoint+1), so the
	// max competitive ordinal is insertionPoint-1.
	bottomOrd := lc.dvs.LookupTerm([]byte(lc.bottom))
	var maxOrd int
	if bottomOrd >= 0 {
		maxOrd = bottomOrd
	} else {
		maxOrd = -(bottomOrd + 1) - 1
	}
	if maxOrd < 0 {
		// All values are greater than bottom, nothing is competitive
		lc.competitiveIter = emptyIterator{}
		return
	}

	lc.competitiveIter = NewSkipBlockRangeIterator(lc.skipper, 0, int64(maxOrd))
}

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

// emptyIterator is a DocIdSetIterator that matches no documents.
type emptyIterator struct{}

func (emptyIterator) DocID() int      { return NoMoreDocs }
func (emptyIterator) NextDoc() int    { return NoMoreDocs }
func (emptyIterator) Advance(int) int { return NoMoreDocs }
func (emptyIterator) Cost() int64     { return 0 }
