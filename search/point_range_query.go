package search

import (
	"gosearch/document"
	"gosearch/index"
	"gosearch/index/bkd"
	"sort"
)

// PointRangeQuery matches documents where a numeric point field's value falls
// within an inclusive [min, max] range.
type PointRangeQuery struct {
	field string
	Min   int64
	Max   int64
}

func NewPointRangeQuery(field string, min, max int64) *PointRangeQuery {
	return &PointRangeQuery{field: field, Min: min, Max: max}
}

// NewDoublePointRangeQuery creates a range query for double point fields.
func NewDoublePointRangeQuery(field string, min, max float64) *PointRangeQuery {
	return &PointRangeQuery{
		field: field,
		Min:   document.DoubleToSortableLong(min),
		Max:   document.DoubleToSortableLong(max),
	}
}

func (q *PointRangeQuery) ExtractTerms() []FieldTerm {
	return nil
}

func (q *PointRangeQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	return &pointRangeWeight{query: q}
}

type pointRangeWeight struct {
	query *PointRangeQuery
}

func (w *pointRangeWeight) Query() Query {
	return w.query
}

func (w *pointRangeWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	seg := ctx.Segment

	// Check that this field has point values
	if _, ok := seg.PointFields()[w.query.field]; !ok {
		return nil
	}

	pv := seg.PointValues(w.query.field)
	if pv == nil {
		return nil
	}

	visitor := &pointRangeVisitor{min: w.query.Min, max: w.query.Max}
	bkd.Intersect(pv.PointTree(), visitor)

	if len(visitor.docs) == 0 {
		return nil
	}
	sort.Ints(visitor.docs)

	return &pointRangeScorer{
		docs:     visitor.docs,
		liveDocs: seg.LiveDocs(),
		pos:      -1,
		doc:      -1,
	}
}

// pointRangeVisitor collects matching document IDs during BKD tree intersection.
type pointRangeVisitor struct {
	min, max int64
	docs     []int
}

func (v *pointRangeVisitor) Visit(docID int) {
	v.docs = append(v.docs, docID)
}

func (v *pointRangeVisitor) VisitValue(docID int, value int64) {
	if value >= v.min && value <= v.max {
		v.docs = append(v.docs, docID)
	}
}

func (v *pointRangeVisitor) Compare(minValue, maxValue int64) bkd.Relation {
	if maxValue < v.min || minValue > v.max {
		return bkd.CellOutsideQuery
	}
	if minValue >= v.min && maxValue <= v.max {
		return bkd.CellInsideQuery
	}
	return bkd.CellCrossesQuery
}

// pointRangeScorer iterates documents whose point values fall within [min, max].
type pointRangeScorer struct {
	docs     []int
	liveDocs *index.Bitset
	pos      int
	doc      int
}

func (s *pointRangeScorer) Score() float64 {
	return 1.0 // constant score
}

func (s *pointRangeScorer) DocID() int {
	return s.doc
}

func (s *pointRangeScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *pointRangeScorer) NextDoc() int {
	return s.Advance(s.doc + 1)
}

func (s *pointRangeScorer) Advance(target int) int {
	for s.pos+1 < len(s.docs) {
		s.pos++
		docID := s.docs[s.pos]
		if docID < target {
			continue
		}
		if s.liveDocs != nil && s.liveDocs.Get(docID) {
			continue
		}
		s.doc = docID
		return docID
	}
	s.doc = NoMoreDocs
	return NoMoreDocs
}

func (s *pointRangeScorer) Cost() int64 {
	return int64(len(s.docs))
}
