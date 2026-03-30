package search

import (
	"gosearch/document"
	"gosearch/index"
)

// PointRangeQuery matches documents where a numeric point field's value falls
// within an inclusive [min, max] range.
type PointRangeQuery struct {
	field string
	min   int64
	max   int64
}

func NewPointRangeQuery(field string, min, max int64) *PointRangeQuery {
	return &PointRangeQuery{field: field, min: min, max: max}
}

// NewDoublePointRangeQuery creates a range query for double point fields.
func NewDoublePointRangeQuery(field string, min, max float64) *PointRangeQuery {
	return &PointRangeQuery{
		field: field,
		min:   document.DoubleToSortableLong(min),
		max:   document.DoubleToSortableLong(max),
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

	dv := seg.NumericDocValues(w.query.field)
	if dv == nil {
		return nil
	}

	skipper := seg.DocValuesSkipper(w.query.field)

	return &pointRangeScorer{
		min:      w.query.min,
		max:      w.query.max,
		dv:       dv,
		skipper:  skipper,
		liveDocs: seg.LiveDocs(),
		maxDoc:   seg.DocCount(),
		doc:      -1,
	}
}

// pointRangeScorer iterates documents whose point values fall within [min, max].
type pointRangeScorer struct {
	min, max int64
	dv       index.NumericDocValues
	skipper  *index.DocValuesSkipper
	liveDocs *index.Bitset
	maxDoc   int
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
	docID := target

	for docID < s.maxDoc {
		// Use skip index to jump over non-competitive blocks
		if s.skipper != nil {
			s.skipper.Advance(docID)
			if s.skipper.DocCount() == 0 {
				s.doc = NoMoreDocs
				return NoMoreDocs
			}

			blockMin := s.skipper.MinValue()
			blockMax := s.skipper.MaxValue()

			// If the entire block is outside our range, skip to next block
			if blockMin > s.max || blockMax < s.min {
				docID = s.skipper.MaxDocID() + 1
				continue
			}
		}

		// Check individual documents within this block
		blockEnd := s.maxDoc
		if s.skipper != nil {
			blockEnd = min(s.skipper.MaxDocID()+1, s.maxDoc)
		}

		for ; docID < blockEnd; docID++ {
			if s.liveDocs != nil && s.liveDocs.Get(docID) {
				continue
			}
			val, err := s.dv.Get(docID)
			if err != nil {
				continue
			}
			if val >= s.min && val <= s.max {
				s.doc = docID
				return docID
			}
		}

		// Move to next block
		if s.skipper == nil {
			break
		}
	}

	s.doc = NoMoreDocs
	return NoMoreDocs
}

func (s *pointRangeScorer) Cost() int64 {
	return int64(s.maxDoc)
}
