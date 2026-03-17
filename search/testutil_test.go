package search

import (
	"slices"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func setupTestSegment(t *testing.T) index.SegmentReader {
	t.Helper()
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := index.NewIndexWriter(dir, analyzer, 100)

	docs := []string{
		"the quick brown fox",     // doc0
		"the lazy brown dog",      // doc1
		"the quick red fox jumps", // doc2
		"brown fox brown fox",     // doc3
	}

	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}
	writer.Flush()
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return reader.Leaves()[0].Segment
}

// collectDocs runs a query against a single segment and returns matched docIDs and their scores.
func collectDocs(t *testing.T, q Query, seg index.SegmentReader) []SearchResult {
	t.Helper()
	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)
	ctx := index.LeafReaderContext{Segment: seg, DocBase: 0}
	weight := q.CreateWeight(searcher, ScoreModeComplete)
	scorer := weight.Scorer(ctx)
	if scorer == nil {
		return nil
	}
	iter := scorer.Iterator()
	var results []SearchResult
	for iter.NextDoc() != NoMoreDocs {
		results = append(results, SearchResult{
			DocID: scorer.DocID(),
			Score: scorer.Score(),
		})
	}
	return results
}

func extractDocIDs(results []SearchResult) []int {
	var ids []int
	for _, r := range results {
		ids = append(ids, r.DocID)
	}
	return ids
}

func containsDocID(ids []int, target int) bool {
	return slices.Contains(ids, target)
}
