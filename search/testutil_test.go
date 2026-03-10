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

func extractDocIDs(results []DocScore) []int {
	var ids []int
	for _, r := range results {
		ids = append(ids, r.DocID)
	}
	return ids
}

func containsDocID(ids []int, target int) bool {
	return slices.Contains(ids, target)
}
