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

func (q *TermQuery) ExtractTerms() []FieldTerm {
	return []FieldTerm{{Field: q.Field, Term: q.Term}}
}

// CreateWeight creates a Weight that precomputes collection-level BM25 statistics.
func (q *TermQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	w := &termWeight{query: q}

	if scoreMode == ScoreModeNone {
		return w
	}

	collStats := searcher.CollectionStatistics(q.Field)
	termStats := searcher.TermStatistics(q.Field, q.Term)
	if collStats != nil && termStats != nil {
		w.bm25, w.avgDocLen = ComputeBM25Stats(collStats)
		w.idf = w.bm25.IDF(int(collStats.DocCount), int(termStats.DocFreq))
	}
	return w
}

// termWeight holds precomputed collection-level statistics for TermQuery.
type termWeight struct {
	query     *TermQuery
	bm25      *BM25Scorer
	idf       float64
	avgDocLen float64
}

func (w *termWeight) Query() Query { return w.query }

func (w *termWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	seg := ctx.Segment
	docFreq := seg.DocFreq(w.query.Field, w.query.Term)
	if docFreq == 0 {
		return nil
	}

	postings := seg.PostingsIterator(w.query.Field, w.query.Term)
	iter := NewPostingsDocIdSetIterator(postings, int64(docFreq))

	return &termScorer{
		iter:      iter,
		needScore: w.bm25 != nil,
		bm25:      w.bm25,
		seg:       seg,
		field:     w.query.Field,
		idf:       w.idf,
		avgDocLen: w.avgDocLen,
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
