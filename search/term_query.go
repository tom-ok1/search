package search

import "gosearch/index"

// TermQuery searches for a single term in a field.
type TermQuery struct {
	Field string
	Term  string
}

func NewTermQuery(field, term string) *TermQuery {
	return &TermQuery{Field: field, Term: term}
}

// CreateScorer creates a Scorer for this term query in the given segment.
func (q *TermQuery) CreateScorer(ctx index.LeafReaderContext, scoreMode ScoreMode) Scorer {
	seg := ctx.Segment
	docFreq := seg.DocFreq(q.Field, q.Term)
	if docFreq == 0 {
		return nil
	}

	postings := seg.PostingsIterator(q.Field, q.Term)
	iter := NewPostingsDocIdSetIterator(postings, int64(docFreq))

	// Skip BM25 computation if scores not needed
	if scoreMode == ScoreModeNone {
		return &termScorer{
			iter:      iter,
			needScore: false,
		}
	}

	// Prepare BM25 scoring parameters
	bm25 := NewBM25Scorer()
	docCount := seg.LiveDocCount()
	idf := bm25.IDF(docCount, docFreq)

	totalFieldLen := seg.TotalFieldLength(q.Field)
	avgDocLen := 0.0
	if docCount > 0 {
		avgDocLen = float64(totalFieldLen) / float64(docCount)
	}

	return &termScorer{
		iter:      iter,
		needScore: true,
		bm25:      bm25,
		seg:       seg,
		field:     q.Field,
		idf:       idf,
		avgDocLen: avgDocLen,
	}
}

// termScorer is the Scorer implementation for TermQuery.
type termScorer struct {
	iter      *PostingsDocIdSetIterator
	needScore bool

	// Scoring state (only used when needScore=true)
	bm25      *BM25Scorer
	seg       index.SegmentReader
	field     string
	idf       float64
	avgDocLen float64
}

func (s *termScorer) Iterator() DocIdSetIterator {
	return s.iter
}

func (s *termScorer) DocID() int {
	return s.iter.DocID()
}

func (s *termScorer) Score() float64 {
	if !s.needScore {
		return 0.0
	}
	docLen := float64(s.seg.FieldLength(s.field, s.iter.DocID()))
	return s.bm25.Score(float64(s.iter.Freq()), docLen, s.avgDocLen, s.idf)
}
