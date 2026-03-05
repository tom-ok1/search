package index_test

import (
	"math"
	"sort"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/search"
	"gosearch/store"
)

func TestE2EDiskSegmentSearch(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)

	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)

	// Create two segments via auto-flush (bufferSize=2)
	writer := index.NewIndexWriter(dir, analyzer, 2)
	doc0 := document.NewDocument()
	doc0.AddField("body", "the quick brown fox", document.FieldTypeText)
	writer.AddDocument(doc0)
	doc1 := document.NewDocument()
	doc1.AddField("body", "the lazy brown dog", document.FieldTypeText)
	writer.AddDocument(doc1) // auto-flush after 2 docs → _seg0

	doc2 := document.NewDocument()
	doc2.AddField("body", "brown fox jumps over lazy dog", document.FieldTypeText)
	writer.AddDocument(doc2)

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Search via DirectoryReader (disk)
	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search("body", "fox", 10)

	// "fox" appears in doc0 (seg0) and doc2 (seg1)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Verify stored fields are accessible
	for _, r := range results {
		if r.Fields == nil || r.Fields["body"] == "" {
			t.Errorf("missing stored fields for docID %d", r.DocID)
		}
	}

	// Verify all scores are positive
	for _, r := range results {
		if r.Score <= 0 {
			t.Errorf("expected positive score for docID %d, got %f", r.DocID, r.Score)
		}
	}

	// Compare with NRT reader (in-memory)
	nrtReader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader.Close()
	nrtSearcher := search.NewIndexSearcher(nrtReader)
	nrtResults := nrtSearcher.Search("body", "fox", 10)

	if len(results) != len(nrtResults) {
		t.Fatalf("result count mismatch: disk=%d, nrt=%d", len(results), len(nrtResults))
	}

	for i, r := range results {
		nr := nrtResults[i]
		if r.DocID != nr.DocID {
			t.Errorf("result[%d] DocID: disk=%d, nrt=%d", i, r.DocID, nr.DocID)
		}
		if math.Abs(r.Score-nr.Score) > 1e-9 {
			t.Errorf("result[%d] Score: disk=%f, nrt=%f", i, r.Score, nr.Score)
		}
	}
}

func TestE2EDiskSegmentQueryExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)

	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)

	writer := index.NewIndexWriter(dir, analyzer, 100)
	docs := []string{
		"the quick brown fox",
		"the lazy brown dog",
		"the quick red fox jumps",
		"brown fox brown fox",
	}
	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Open from disk
	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	diskSeg := reader.Leaves()[0].Segment

	// TermQuery
	tq := search.NewTermQuery("body", "fox")
	tqResults := tq.Execute(diskSeg)
	if len(tqResults) != 3 {
		t.Errorf("TermQuery 'fox': expected 3 results, got %d", len(tqResults))
	}

	// PhraseQuery "brown fox"
	pq := search.NewPhraseQuery("body", "brown", "fox")
	pqResults := pq.Execute(diskSeg)
	if len(pqResults) != 2 {
		t.Errorf("PhraseQuery 'brown fox': expected 2 results, got %d", len(pqResults))
	}

	// BooleanQuery: "brown" AND NOT "dog"
	bq := search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "brown"), search.OccurMust).
		Add(search.NewTermQuery("body", "dog"), search.OccurMustNot)
	bqResults := bq.Execute(diskSeg)
	if len(bqResults) != 2 {
		t.Errorf("BooleanQuery 'brown AND NOT dog': expected 2 results, got %d", len(bqResults))
	}

	// Compare all query results between NRT and disk
	nrtReader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader.Close()
	nrtSeg := nrtReader.Leaves()[0].Segment

	for _, tc := range []struct {
		name  string
		query search.Query
	}{
		{"TermQuery fox", search.NewTermQuery("body", "fox")},
		{"PhraseQuery brown fox", search.NewPhraseQuery("body", "brown", "fox")},
		{"BooleanQuery brown AND NOT dog", search.NewBooleanQuery().
			Add(search.NewTermQuery("body", "brown"), search.OccurMust).
			Add(search.NewTermQuery("body", "dog"), search.OccurMustNot)},
	} {
		nrtResults := tc.query.Execute(nrtSeg)
		diskResults := tc.query.Execute(diskSeg)

		if len(nrtResults) != len(diskResults) {
			t.Errorf("%s: result count mismatch: nrt=%d, disk=%d",
				tc.name, len(nrtResults), len(diskResults))
			continue
		}

		// Sort by DocID for deterministic comparison (map iteration order may differ)
		sort.Slice(nrtResults, func(i, j int) bool { return nrtResults[i].DocID < nrtResults[j].DocID })
		sort.Slice(diskResults, func(i, j int) bool { return diskResults[i].DocID < diskResults[j].DocID })

		for i, nr := range nrtResults {
			dr := diskResults[i]
			if nr.DocID != dr.DocID {
				t.Errorf("%s result[%d]: DocID nrt=%d, disk=%d", tc.name, i, nr.DocID, dr.DocID)
			}
			if math.Abs(nr.Score-dr.Score) > 1e-9 {
				t.Errorf("%s result[%d]: Score nrt=%f, disk=%f", tc.name, i, nr.Score, dr.Score)
			}
		}
	}
}
