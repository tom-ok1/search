package search

import (
	"gosearch/index"
)

// FieldExistsMode determines how field presence is checked.
type FieldExistsMode int

const (
	// FieldExistsNorms checks field length > 0 (for text fields).
	FieldExistsNorms FieldExistsMode = iota
	// FieldExistsDocValues checks sorted doc values ordinal >= 0 (for keyword/boolean fields).
	FieldExistsDocValues
)

// FieldExistsQuery matches documents that have a value for the given field.
// Equivalent to Lucene's NormsFieldExistsQuery / DocValuesFieldExistsQuery.
type FieldExistsQuery struct {
	Field string
	Mode  FieldExistsMode
}

func NewFieldExistsQuery(field string, mode FieldExistsMode) *FieldExistsQuery {
	return &FieldExistsQuery{Field: field, Mode: mode}
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
	switch w.query.Mode {
	case FieldExistsNorms:
		return &normsExistsScorer{
			seg:      seg,
			field:    w.query.Field,
			liveDocs: liveDocs,
			doc:      -1,
			max:      seg.DocCount(),
		}
	case FieldExistsDocValues:
		sdv := seg.SortedDocValues(w.query.Field)
		if sdv == nil {
			return nil
		}
		return &docValuesExistsScorer{
			sdv:      sdv,
			liveDocs: liveDocs,
			doc:      -1,
			max:      seg.DocCount(),
		}
	}
	return nil
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
