package search

import (
	"slices"

	"gosearch/index"
)

// PhraseQuery searches for documents where terms appear consecutively in the specified order.
type PhraseQuery struct {
	Field string
	Terms []string
}

func NewPhraseQuery(field string, terms ...string) *PhraseQuery {
	return &PhraseQuery{Field: field, Terms: terms}
}

// CreateWeight creates a Weight that precomputes collection-level BM25 statistics.
func (q *PhraseQuery) CreateWeight(searcher *IndexSearcher, scoreMode ScoreMode) Weight {
	w := &phraseWeight{query: q}
	if scoreMode != ScoreModeNone && len(q.Terms) > 0 {
		collStats := searcher.CollectionStatistics(q.Field)
		if collStats != nil {
			w.bm25, w.avgDocLen = ComputeBM25Stats(collStats)
			w.idfs = make([]float64, len(q.Terms))
			for i, term := range q.Terms {
				termStats := searcher.TermStatistics(q.Field, term)
				if termStats != nil {
					w.idfs[i] = w.bm25.IDF(int(collStats.DocCount), int(termStats.DocFreq))
				}
			}
		}
	}
	return w
}

// phraseWeight holds precomputed collection-level statistics for PhraseQuery.
type phraseWeight struct {
	query     *PhraseQuery
	bm25      *BM25Scorer
	avgDocLen float64
	idfs      []float64
}

func (w *phraseWeight) Query() Query { return w.query }

func (w *phraseWeight) Scorer(ctx index.LeafReaderContext) Scorer {
	if len(w.query.Terms) == 0 {
		return nil
	}

	seg := ctx.Segment

	iterators := make([]*PostingsDocIdSetIterator, len(w.query.Terms))
	for i, term := range w.query.Terms {
		docFreq := seg.DocFreq(w.query.Field, term)
		if docFreq == 0 {
			return nil
		}
		postings := seg.PostingsIterator(w.query.Field, term)
		iterators[i] = NewPostingsDocIdSetIterator(postings, int64(docFreq))
	}

	return &phraseScorer{
		iterators:    iterators,
		field:        w.query.Field,
		seg:          seg,
		needScore:    w.bm25 != nil,
		bm25:         w.bm25,
		avgDocLen:    w.avgDocLen,
		idfs:         w.idfs,
		positionSets: make([][]int, len(w.query.Terms)),
		docID:        -1,
	}
}

// phraseScorer is the Scorer implementation for PhraseQuery.
// It also implements DocIdSetIterator directly to avoid per-call allocations.
type phraseScorer struct {
	iterators []*PostingsDocIdSetIterator
	field     string
	seg       index.SegmentReader
	docID     int

	// Scoring state
	needScore    bool
	bm25         *BM25Scorer
	avgDocLen    float64
	idfs         []float64
	positionSets [][]int // reusable buffer
}

func (s *phraseScorer) Iterator() DocIdSetIterator {
	return s
}

func (s *phraseScorer) DocID() int {
	return s.docID
}

func (s *phraseScorer) Score() float64 {
	if !s.needScore {
		return 0.0
	}

	totalScore := 0.0
	docLen := float64(s.seg.FieldLength(s.field, s.docID))
	for i, iter := range s.iterators {
		totalScore += s.bm25.Score(float64(iter.Freq()), docLen, s.avgDocLen, s.idfs[i])
	}
	return totalScore
}

func (s *phraseScorer) NextDoc() int {
	doc := s.iterators[0].NextDoc()
	return s.findNextMatch(doc)
}

func (s *phraseScorer) Advance(target int) int {
	doc := s.iterators[0].Advance(target)
	return s.findNextMatch(doc)
}

func (s *phraseScorer) Cost() int64 {
	minCost := s.iterators[0].Cost()
	for _, iter := range s.iterators[1:] {
		if iter.Cost() < minCost {
			minCost = iter.Cost()
		}
	}
	return minCost
}

// findNextMatch advances from doc until all iterators align with matching positions.
func (s *phraseScorer) findNextMatch(doc int) int {
	for doc != NoMoreDocs {
		allMatch := true
		for i := 1; i < len(s.iterators); i++ {
			other := s.iterators[i]
			if other.DocID() < doc {
				other.Advance(doc)
			}
			if other.DocID() != doc {
				allMatch = false
				break
			}
		}

		if allMatch && s.matchPositions() {
			s.docID = doc
			return doc
		}

		doc = s.iterators[0].NextDoc()
	}
	s.docID = NoMoreDocs
	return NoMoreDocs
}

// matchPositions checks if term positions are consecutive at the current doc.
func (s *phraseScorer) matchPositions() bool {
	for i, iter := range s.iterators {
		s.positionSets[i] = iter.Positions()
	}

	for _, startPos := range s.positionSets[0] {
		matched := true
		for i := 1; i < len(s.positionSets); i++ {
			expectedPos := startPos + i
			if !slices.Contains(s.positionSets[i], expectedPos) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
