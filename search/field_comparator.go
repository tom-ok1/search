package search

import (
	"cmp"

	"gosearch/index"
)

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

// LeafFieldComparator performs segment-local comparisons and copies.
type LeafFieldComparator interface {
	// SetBottom tells the comparator which slot is the current bottom.
	SetBottom(slot int)
	// CompareBottom compares the bottom (worst) slot with a new candidate doc.
	CompareBottom(docID int) int
	// Copy copies a doc's value into a slot.
	Copy(slot int, docID int)
	// SetScorer sets the current document's score.
	// Must be called before CompareBottom or Copy for score-based comparators.
	SetScorer(score float64)
}

// --- Numeric ---

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

func (c *NumericFieldComparator) Value(slot int) interface{} {
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

// --- String ---

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

func (c *StringFieldComparator) Value(slot int) interface{} {
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

// --- Score ---

// ScoreFieldComparator sorts by BM25 score.
// It implements both FieldComparator and LeafFieldComparator because
// score has no segment-specific state (analogous to Lucene's RelevanceComparator).
type ScoreFieldComparator struct {
	scores       []float64
	bottom       float64
	currentScore float64
}

func NewScoreFieldComparator(numSlots int) *ScoreFieldComparator {
	return &ScoreFieldComparator{
		scores: make([]float64, numSlots),
	}
}

func (c *ScoreFieldComparator) CompareSlots(slot1, slot2 int) int {
	return cmp.Compare(c.scores[slot1], c.scores[slot2])
}

func (c *ScoreFieldComparator) Value(slot int) interface{} {
	return c.scores[slot]
}

func (c *ScoreFieldComparator) GetLeafComparator(seg index.SegmentReader) LeafFieldComparator {
	return c
}

// LeafFieldComparator implementation

func (c *ScoreFieldComparator) SetBottom(slot int) {
	c.bottom = c.scores[slot]
}

func (c *ScoreFieldComparator) CompareBottom(docID int) int {
	return cmp.Compare(c.currentScore, c.bottom)
}

func (c *ScoreFieldComparator) Copy(slot int, docID int) {
	c.scores[slot] = c.currentScore
}

func (c *ScoreFieldComparator) SetScorer(score float64) {
	c.currentScore = score
}
