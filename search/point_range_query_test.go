package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestPointRangeQueryLong(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024)

	// Add documents with long point values: 10, 20, 30, 40, 50
	values := []int64{10, 20, 30, 40, 50}
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddLongPoint("price", val)
		writer.AddDocument(doc)
	}
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)

	tests := []struct {
		name     string
		min      int64
		max      int64
		expected []int
	}{
		{"exact match", 30, 30, []int{2}},
		{"full range", 10, 50, []int{0, 1, 2, 3, 4}},
		{"partial range", 20, 40, []int{1, 2, 3}},
		{"no match below", 1, 5, []int{}},
		{"no match above", 60, 100, []int{}},
		{"left boundary", 10, 20, []int{0, 1}},
		{"right boundary", 40, 50, []int{3, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewPointRangeQuery("price", tt.min, tt.max)
			collector := NewTopKCollector(10)
			results := searcher.Search(q, collector)
			if len(results) != len(tt.expected) {
				t.Fatalf("expected %d results, got %d", len(tt.expected), len(results))
			}

			for i, expectedDocID := range tt.expected {
				if results[i].DocID != expectedDocID {
					t.Errorf("result %d: expected docID %d, got %d", i, expectedDocID, results[i].DocID)
				}
				if results[i].Score != 1.0 {
					t.Errorf("result %d: expected score 1.0, got %f", i, results[i].Score)
				}
			}
		})
	}
}

func TestPointRangeQueryDouble(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024)

	// Add documents with double point values: 1.5, 2.5, 3.5, 4.5, 5.5
	values := []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddDoublePoint("temperature", val)
		writer.AddDocument(doc)
	}
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)

	tests := []struct {
		name     string
		min      float64
		max      float64
		expected []int
	}{
		{"range [2.0, 4.0] matches 2.5 and 3.5", 2.0, 4.0, []int{1, 2}},
		{"exact match 3.5", 3.5, 3.5, []int{2}},
		{"full range", 1.0, 6.0, []int{0, 1, 2, 3, 4}},
		{"no match below", 0.0, 1.0, []int{}},
		{"no match above", 6.0, 10.0, []int{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewDoublePointRangeQuery("temperature", tt.min, tt.max)
			collector := NewTopKCollector(10)
			results := searcher.Search(q, collector)
			if len(results) != len(tt.expected) {
				t.Fatalf("expected %d results, got %d", len(tt.expected), len(results))
			}

			for i, expectedDocID := range tt.expected {
				if results[i].DocID != expectedDocID {
					t.Errorf("result %d: expected docID %d, got %d", i, expectedDocID, results[i].DocID)
				}
			}
		})
	}
}

func TestPointRangeQueryDeletedDocs(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024)

	// Add 3 documents with long point values and IDs
	values := []int64{10, 20, 30}
	for i, val := range values {
		doc := document.NewDocument()
		doc.AddField("id", string(rune('a'+i)), document.FieldTypeKeyword)
		doc.AddLongPoint("value", val)
		writer.AddDocument(doc)
	}
	writer.Commit()

	// Delete doc with id="b" (docID 1, value=20)
	writer.DeleteDocuments("id", "b")
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)

	// Query range [10, 30] should match all 3 docs, but doc 1 is deleted
	q := NewPointRangeQuery("value", 10, 30)
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)
	if len(results) != 2 {
		t.Fatalf("expected 2 results (doc 1 deleted), got %d", len(results))
	}

	// Should match doc 0 and doc 2, skipping deleted doc 1
	expectedDocs := []int{0, 2}
	for i, expectedDocID := range expectedDocs {
		if results[i].DocID != expectedDocID {
			t.Errorf("result %d: expected docID %d, got %d", i, expectedDocID, results[i].DocID)
		}
	}
}

func TestPointRangeQueryFieldDoesNotExist(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024)

	// Add a document with a different field
	doc := document.NewDocument()
	doc.AddLongPoint("price", 100)
	writer.AddDocument(doc)
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)

	// Query for a field that doesn't exist
	q := NewPointRangeQuery("nonexistent", 0, 1000)
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for nonexistent field, got %d", len(results))
	}
}

func TestPointRangeQuerySkipBlocks(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024*1024*10)

	// Create 1000 docs to span multiple skip blocks (blockSize=128)
	for i := range 1000 {
		doc := document.NewDocument()
		doc.AddLongPoint("val", int64(i))
		writer.AddDocument(doc)
	}
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)

	// Narrow range in the middle — should skip many blocks
	q := NewPointRangeQuery("val", 500, 510)
	collector := NewTopKCollector(100)
	results := searcher.Search(q, collector)

	if len(results) != 11 {
		t.Errorf("got %d hits, want 11 (values 500-510)", len(results))
	}
}

func TestFieldExistsQuery_PointFields(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024*1024)

	// Doc 0: has price field
	doc0 := document.NewDocument()
	doc0.AddLongPoint("price", 100)
	writer.AddDocument(doc0)

	// Doc 1: no price field
	doc1 := document.NewDocument()
	doc1.AddField("name", "test", document.FieldTypeKeyword)
	writer.AddDocument(doc1)

	// Doc 2: has price field
	doc2 := document.NewDocument()
	doc2.AddLongPoint("price", 200)
	writer.AddDocument(doc2)

	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := NewIndexSearcher(reader)
	q := NewFieldExistsQuery("price")
	collector := NewTopKCollector(10)
	results := searcher.Search(q, collector)

	if len(results) != 2 {
		t.Errorf("exists query: got %d hits, want 2 (docs 0 and 2 have price)", len(results))
	}
}
