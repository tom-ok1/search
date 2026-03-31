package search

import (
	"gosearch/index"
	"gosearch/index/bkd"
	"sort"
)

// FieldExistsQuery matches documents that have a value for the given field.
// It auto-detects whether to use doc values or norms, mirroring Lucene's
// FieldExistsQuery which inspects FieldInfo at scorer creation time.
type FieldExistsQuery struct {
	Field string
}

func NewFieldExistsQuery(field string) *FieldExistsQuery {
	return &FieldExistsQuery{Field: field}
}

func (q *FieldExistsQuery) ExtractTerms() []FieldTerm {
	return nil
}

func (q *FieldExistsQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	return &fieldExistsWeight{query: q}
}

type fieldExistsWeight struct {
	query *FieldExistsQuery
}

func (w *fieldExistsWeight) Query() Query { return w.query }

func (w *fieldExistsWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	seg := ctx.Segment
	liveDocs := seg.LiveDocs()

	// Auto-detect: try sorted doc values, then point values, then norms.
	// This mirrors Lucene's FieldExistsQuery which checks FieldInfo to
	// determine whether to use doc values, point values, or norms.
	if sdv := seg.SortedDocValues(w.query.Field); sdv != nil {
		return &docValuesExistsScorer{
			sdv:      sdv,
			liveDocs: liveDocs,
			doc:      -1,
			max:      seg.DocCount(),
		}
	}

	if pv := seg.PointValues(w.query.Field); pv != nil {
		return &pointValuesExistsScorer{
			pv:       pv,
			liveDocs: liveDocs,
			doc:      -1,
			max:      seg.DocCount(),
		}
	}

	return &normsExistsScorer{
		seg:      seg,
		field:    w.query.Field,
		liveDocs: liveDocs,
		doc:      -1,
		max:      seg.DocCount(),
	}
}

// normsExistsScorer yields docs where FieldLength > 0.
type normsExistsScorer struct {
	seg      index.SegmentReader
	field    string
	liveDocs *index.Bitset
	doc      int
	max      int
}

func (s *normsExistsScorer) Score() float64             { return 1.0 }
func (s *normsExistsScorer) DocID() int                 { return s.doc }
func (s *normsExistsScorer) Iterator() DocIdSetIterator { return s }
func (s *normsExistsScorer) Cost() int64                { return int64(s.max) }

func (s *normsExistsScorer) NextDoc() int {
	for {
		s.doc++
		if s.doc >= s.max {
			s.doc = NoMoreDocs
			return NoMoreDocs
		}
		if s.liveDocs != nil && s.liveDocs.Get(s.doc) {
			continue // deleted
		}
		if s.seg.FieldLength(s.field, s.doc) > 0 {
			return s.doc
		}
	}
}

func (s *normsExistsScorer) Advance(target int) int {
	s.doc = target - 1
	return s.NextDoc()
}

// docValuesExistsScorer yields docs where sorted doc values ordinal >= 0.
type docValuesExistsScorer struct {
	sdv      index.SortedDocValues
	liveDocs *index.Bitset
	doc      int
	max      int
}

func (s *docValuesExistsScorer) Score() float64             { return 1.0 }
func (s *docValuesExistsScorer) DocID() int                 { return s.doc }
func (s *docValuesExistsScorer) Iterator() DocIdSetIterator { return s }
func (s *docValuesExistsScorer) Cost() int64                { return int64(s.max) }

func (s *docValuesExistsScorer) NextDoc() int {
	for {
		s.doc++
		if s.doc >= s.max {
			s.doc = NoMoreDocs
			return NoMoreDocs
		}
		if s.liveDocs != nil && s.liveDocs.Get(s.doc) {
			continue // deleted
		}
		ord, err := s.sdv.OrdValue(s.doc)
		if err == nil && ord >= 0 {
			return s.doc
		}
	}
}

func (s *docValuesExistsScorer) Advance(target int) int {
	s.doc = target - 1
	return s.NextDoc()
}

// pointValuesExistsScorer yields docs that have a point value, using PointValues.
type pointValuesExistsScorer struct {
	pv       index.PointValues
	liveDocs *index.Bitset
	doc      int
	max      int
	docIDs   []int // sorted docIDs from BKD tree
	pos      int   // current position in docIDs
	inited   bool
}

func (s *pointValuesExistsScorer) init() {
	tree := s.pv.PointTree()
	collector := &docIDCollector{}
	// Use math.MinInt64/MaxInt64 range to match all points.
	allVisitor := &allPointsVisitor{collector: collector}
	bkd.Intersect(tree, allVisitor)
	sort.Ints(collector.docIDs)
	s.docIDs = collector.docIDs
	s.inited = true
}

func (s *pointValuesExistsScorer) Score() float64             { return 1.0 }
func (s *pointValuesExistsScorer) DocID() int                 { return s.doc }
func (s *pointValuesExistsScorer) Iterator() DocIdSetIterator { return s }
func (s *pointValuesExistsScorer) Cost() int64                { return int64(s.pv.DocCount()) }

func (s *pointValuesExistsScorer) NextDoc() int {
	if !s.inited {
		s.init()
	}
	for s.pos < len(s.docIDs) {
		d := s.docIDs[s.pos]
		s.pos++
		if s.liveDocs != nil && s.liveDocs.Get(d) {
			continue // deleted
		}
		s.doc = d
		return d
	}
	s.doc = NoMoreDocs
	return NoMoreDocs
}

func (s *pointValuesExistsScorer) Advance(target int) int {
	if !s.inited {
		s.init()
	}
	// Binary search for target in sorted docIDs.
	s.pos = sort.SearchInts(s.docIDs, target)
	return s.NextDoc()
}

// allPointsVisitor is an IntersectVisitor that matches all points.
type allPointsVisitor struct {
	collector *docIDCollector
}

func (v *allPointsVisitor) Compare(minValue, maxValue int64) bkd.Relation {
	return bkd.CellInsideQuery
}

func (v *allPointsVisitor) Visit(docID int) {
	v.collector.docIDs = append(v.collector.docIDs, docID)
}

func (v *allPointsVisitor) VisitValue(docID int, value int64) {
	v.collector.docIDs = append(v.collector.docIDs, docID)
}

type docIDCollector struct {
	docIDs []int
}
