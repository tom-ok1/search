package search

import (
	"cmp"
	"math"

	"gosearch/index"
)

// NumericFieldComparator sorts by numeric doc values.
type NumericFieldComparator struct {
	field   string
	values  []int64
	reverse bool
}

func NewNumericFieldComparator(field string, numSlots int, reverse bool) *NumericFieldComparator {
	return &NumericFieldComparator{
		field:   field,
		values:  make([]int64, numSlots),
		reverse: reverse,
	}
}

func (c *NumericFieldComparator) CompareSlots(slot1, slot2 int) int {
	return cmp.Compare(c.values[slot1], c.values[slot2])
}

func (c *NumericFieldComparator) Value(slot int) any {
	return c.values[slot]
}

func (c *NumericFieldComparator) GetLeafComparator(seg index.SegmentReader) LeafFieldComparator {
	lc := &numericLeafComparator{
		parent:  c,
		dvs:     seg.NumericDocValues(c.field),
		skipper: seg.NumericDocValuesSkipper(c.field),
	}
	return lc
}

type numericLeafComparator struct {
	parent          *NumericFieldComparator
	dvs             index.NumericDocValues
	skipper         *index.DocValuesSkipper
	bottom          int64
	competitiveIter DocIdSetIterator
	hasBottom       bool
	iterDirty       bool
}

func (lc *numericLeafComparator) SetBottom(slot int) {
	newBottom := lc.parent.values[slot]
	if lc.hasBottom && newBottom == lc.bottom {
		return
	}
	lc.bottom = newBottom
	lc.hasBottom = true
	lc.iterDirty = true
}

func (lc *numericLeafComparator) CompareBottom(docID int) int {
	v := lc.getValueForDoc(docID)
	return cmp.Compare(lc.bottom, v)
}

func (lc *numericLeafComparator) Copy(slot int, docID int) {
	lc.parent.values[slot] = lc.getValueForDoc(docID)
}

func (lc *numericLeafComparator) SetScorer(score float64) {}

func (lc *numericLeafComparator) CompetitiveIterator() DocIdSetIterator {
	if lc.iterDirty {
		lc.rebuildCompetitiveIterator()
		lc.iterDirty = false
	}
	return lc.competitiveIter
}

func (lc *numericLeafComparator) rebuildCompetitiveIterator() {
	if lc.skipper == nil || !lc.hasBottom {
		return
	}

	var minValue, maxValue int64
	if lc.parent.reverse {
		minValue = lc.bottom
		maxValue = math.MaxInt64
	} else {
		minValue = math.MinInt64
		maxValue = lc.bottom
	}

	lc.competitiveIter = NewSkipBlockRangeIterator(lc.skipper, minValue, maxValue)
}

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
