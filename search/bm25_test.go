package search

import (
	"math"
	"sort"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestBM25IDF(t *testing.T) {
	scorer := NewBM25Scorer()

	// Rare terms should have higher IDF
	idfRare := scorer.IDF(1000, 5)
	idfCommon := scorer.IDF(1000, 500)
	if idfRare <= idfCommon {
		t.Errorf("rare term should have higher IDF: rare=%f, common=%f", idfRare, idfCommon)
	}

	// A term appearing in all documents should have near-zero IDF
	idfAll := scorer.IDF(1000, 1000)
	if idfAll > 0.1 {
		t.Errorf("term in all docs should have near-zero IDF: %f", idfAll)
	}
}

func TestBM25Scoring(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := index.NewIndexWriter(dir, fa, 100)

	// doc0: "fox" appears twice in a short document
	doc0 := document.NewDocument()
	doc0.AddField("body", "fox fox", document.FieldTypeText)
	writer.AddDocument(doc0)

	// doc1: "fox" appears once in a longer document
	doc1 := document.NewDocument()
	doc1.AddField("body", "the quick brown fox jumps over the lazy dog", document.FieldTypeText)
	writer.AddDocument(doc1)

	// doc2: does not contain "fox"
	doc2 := document.NewDocument()
	doc2.AddField("body", "the lazy dog sleeps all day", document.FieldTypeText)
	writer.AddDocument(doc2)

	writer.Flush()
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seg := reader.Leaves()[0].Segment

	q := NewTermQuery("body", "fox")
	results := collectDocs(t, q, seg)

	// Only doc0 and doc1 should match
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Sort by score descending to verify ranking
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// doc0 should rank higher (short doc with higher TF)
	if results[0].DocID != 0 {
		t.Errorf("expected doc0 first, got doc%d", results[0].DocID)
	}

	// All scores should be positive
	for _, r := range results {
		if r.Score <= 0 {
			t.Errorf("expected positive score, got %f", r.Score)
		}
	}
}

func TestTopKCollector(t *testing.T) {
	collector := NewTopKCollector(2)
	lc := collector.GetLeafCollector(leafCtx(0))
	ms := &mockScorable{}
	lc.SetScorer(ms)

	ms.score = 1.0
	lc.Collect(0)
	ms.score = 3.0
	lc.Collect(1)
	ms.score = 2.0
	lc.Collect(2)

	results := collector.Results()
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// Results should be in descending score order
	if results[0].DocID != 1 || results[1].DocID != 2 {
		t.Errorf("expected [doc1, doc2], got [doc%d, doc%d]",
			results[0].DocID, results[1].DocID)
	}
}

func TestBM25ScoreZeroAvgDocLen(t *testing.T) {
	scorer := NewBM25Scorer()
	idf := scorer.IDF(10, 1)

	// avgDocLen=0 should not produce NaN or Inf
	score := scorer.Score(1.0, 5.0, 0.0, idf)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		t.Errorf("BM25 score with avgDocLen=0 should be finite, got %f", score)
	}
	if score <= 0 {
		t.Errorf("BM25 score should be positive, got %f", score)
	}
}
