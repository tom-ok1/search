package search

import (
	"gosearch/index"
	"sort"
)

// Occur represents the type of a boolean clause.
type Occur int

const (
	OccurMust    Occur = iota // AND
	OccurShould               // OR
	OccurMustNot              // NOT
)

// BooleanClause is a single clause in a BooleanQuery.
type BooleanClause struct {
	Query Query
	Occur Occur
}

// BooleanQuery combines multiple query clauses with boolean logic.
type BooleanQuery struct {
	Clauses []BooleanClause
}

func NewBooleanQuery() *BooleanQuery {
	return &BooleanQuery{}
}

func (q *BooleanQuery) Add(query Query, occur Occur) *BooleanQuery {
	q.Clauses = append(q.Clauses, BooleanClause{Query: query, Occur: occur})
	return q
}

func (q *BooleanQuery) Execute(idx *index.InMemoryIndex) []DocScore {
	var mustResults [][]DocScore
	var shouldResults [][]DocScore
	var mustNotResults [][]DocScore

	for _, clause := range q.Clauses {
		results := clause.Query.Execute(idx)
		switch clause.Occur {
		case OccurMust:
			mustResults = append(mustResults, results)
		case OccurShould:
			shouldResults = append(shouldResults, results)
		case OccurMustNot:
			mustNotResults = append(mustNotResults, results)
		}
	}

	// Compute MUST intersection
	candidates := intersectAll(mustResults)

	// If no MUST clauses, use SHOULD union as candidates
	if len(mustResults) == 0 && len(shouldResults) > 0 {
		candidates = unionAll(shouldResults)
	} else if len(shouldResults) > 0 {
		// When MUST exists, SHOULD only adds scores
		candidates = addShouldScores(candidates, shouldResults)
	}

	// Exclude MUST_NOT matches
	if len(mustNotResults) > 0 {
		excludeSet := make(map[int]bool)
		for _, results := range mustNotResults {
			for _, ds := range results {
				excludeSet[ds.DocID] = true
			}
		}
		var filtered []DocScore
		for _, ds := range candidates {
			if !excludeSet[ds.DocID] {
				filtered = append(filtered, ds)
			}
		}
		candidates = filtered
	}

	return candidates
}

// intersectAll finds DocIDs common to all lists, summing their scores.
func intersectAll(lists [][]DocScore) []DocScore {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}

	result := lists[0]
	for i := 1; i < len(lists); i++ {
		result = intersectTwo(result, lists[i])
	}
	return result
}

// intersectTwo finds common DocIDs between two sorted DocScore lists.
func intersectTwo(a, b []DocScore) []DocScore {
	var result []DocScore
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].DocID == b[j].DocID {
			result = append(result, DocScore{
				DocID: a[i].DocID,
				Score: a[i].Score + b[j].Score,
			})
			i++
			j++
		} else if a[i].DocID < b[j].DocID {
			i++
		} else {
			j++
		}
	}
	return result
}

// unionAll merges multiple DocScore lists, summing scores for duplicate DocIDs.
func unionAll(lists [][]DocScore) []DocScore {
	scoreMap := make(map[int]float64)
	for _, list := range lists {
		for _, ds := range list {
			scoreMap[ds.DocID] += ds.Score
		}
	}

	var result []DocScore
	for docID, score := range scoreMap {
		result = append(result, DocScore{DocID: docID, Score: score})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DocID < result[j].DocID
	})
	return result
}

// addShouldScores adds SHOULD scores to MUST results as a boost.
func addShouldScores(must []DocScore, shouldLists [][]DocScore) []DocScore {
	shouldScores := make(map[int]float64)
	for _, list := range shouldLists {
		for _, ds := range list {
			shouldScores[ds.DocID] += ds.Score
		}
	}

	for i := range must {
		if bonus, exists := shouldScores[must[i].DocID]; exists {
			must[i].Score += bonus
		}
	}
	return must
}
