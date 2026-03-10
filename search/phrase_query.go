package search

import (
	"slices"
	"sort"

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

func (q *PhraseQuery) Execute(seg index.SegmentReader) []DocScore {
	if len(q.Terms) == 0 {
		return nil
	}

	// Materialize PostingsLists from iterators
	var postingsLists []*index.PostingsList
	for _, term := range q.Terms {
		pl := materializePostings(term, seg.PostingsIterator(q.Field, term))
		if len(pl.Postings) == 0 {
			return nil // any missing term means no match
		}
		postingsLists = append(postingsLists, pl)
	}

	// Find DocIDs common to all terms
	commonDocs := findCommonDocs(postingsLists)

	scorer := NewBM25Scorer()
	docCount := seg.LiveDocCount()

	totalFieldLen := seg.TotalFieldLength(q.Field)
	avgDocLen := 0.0
	if docCount > 0 {
		avgDocLen = float64(totalFieldLen) / float64(docCount)
	}

	var results []DocScore
	for _, docID := range commonDocs {
		// Check if positions are consecutive
		if q.matchPositions(postingsLists, docID) {
			// Score: sum of BM25 scores for each term
			totalScore := 0.0
			docLen := float64(seg.FieldLength(q.Field, docID))
			for i, pl := range postingsLists {
				posting := findPosting(pl, docID)
				if posting != nil {
					idf := scorer.IDF(docCount, len(postingsLists[i].Postings))
					totalScore += scorer.Score(float64(posting.Freq), docLen, avgDocLen, idf)
				}
			}
			results = append(results, DocScore{DocID: docID, Score: totalScore})
		}
	}

	return results
}

// materializePostings collects all postings from an iterator into a PostingsList.
func materializePostings(term string, iter index.PostingsIterator) *index.PostingsList {
	pl := &index.PostingsList{Term: term}
	for iter.Next() {
		pl.Postings = append(pl.Postings, index.Posting{
			DocID:     iter.DocID(),
			Freq:      iter.Freq(),
			Positions: iter.Positions(),
		})
	}
	return pl
}

// matchPositions checks whether term positions are consecutive for the given DocID.
func (q *PhraseQuery) matchPositions(postingsLists []*index.PostingsList, docID int) bool {
	var positionSets [][]int
	for _, pl := range postingsLists {
		posting := findPosting(pl, docID)
		if posting == nil {
			return false
		}
		positionSets = append(positionSets, posting.Positions)
	}

	// For each starting position of the first term, check consecutive positions
	for _, startPos := range positionSets[0] {
		matched := true
		for i := 1; i < len(positionSets); i++ {
			expectedPos := startPos + i
			if !containsInt(positionSets[i], expectedPos) {
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

// findCommonDocs returns DocIDs that appear in all PostingsLists.
func findCommonDocs(lists []*index.PostingsList) []int {
	if len(lists) == 0 {
		return nil
	}

	docSet := make(map[int]bool)
	for _, p := range lists[0].Postings {
		docSet[p.DocID] = true
	}

	for _, pl := range lists[1:] {
		newSet := make(map[int]bool)
		for _, p := range pl.Postings {
			if docSet[p.DocID] {
				newSet[p.DocID] = true
			}
		}
		docSet = newSet
	}

	var result []int
	for docID := range docSet {
		result = append(result, docID)
	}
	sort.Ints(result)
	return result
}

func findPosting(pl *index.PostingsList, docID int) *index.Posting {
	for i := range pl.Postings {
		if pl.Postings[i].DocID == docID {
			return &pl.Postings[i]
		}
	}
	return nil
}

func containsInt(slice []int, val int) bool {
	return slices.Contains(slice, val)
}
