package search

import "math"

// Default BM25 parameters
const (
	DefaultK1 = 1.2
	DefaultB  = 0.75
)

// BM25Scorer implements BM25 scoring.
type BM25Scorer struct {
	K1 float64
	B  float64
}

func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{
		K1: DefaultK1,
		B:  DefaultB,
	}
}

// IDF computes inverse document frequency.
// docCount: total number of documents
// docFreq: number of documents containing the term
func (s *BM25Scorer) IDF(docCount, docFreq int) float64 {
	return math.Log(1 + (float64(docCount-docFreq)+0.5)/(float64(docFreq)+0.5))
}

// Score computes the BM25 score for a single term in a document.
// tf: term frequency in the document
// docLen: token count of the document field
// avgDocLen: average token count across all documents
// idf: precomputed IDF value
func (s *BM25Scorer) Score(tf float64, docLen float64, avgDocLen float64, idf float64) float64 {
	if avgDocLen == 0 {
		avgDocLen = 1
	}
	tfNorm := (tf * (s.K1 + 1)) / (tf + s.K1*(1-s.B+s.B*docLen/avgDocLen))
	return idf * tfNorm
}
