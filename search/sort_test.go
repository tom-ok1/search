package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func setupIndexWithDV(t *testing.T, docs []testDoc) *index.IndexReader {
	t.Helper()
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	writer := index.NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)
	for _, td := range docs {
		doc := document.NewDocument()
		doc.AddField("body", td.body, document.FieldTypeText)
		if td.price != 0 {
			doc.AddNumericDocValuesField("price", td.price)
		}
		if td.category != "" {
			doc.AddSortedDocValuesField("category", td.category)
		}
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	return reader
}

type testDoc struct {
	body     string
	price    int64
	category string
}

func TestSearchWithSortNumericAsc(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "apple fruit", price: 300},
		{body: "apple pie", price: 100},
		{body: "apple sauce", price: 200},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "apple")
	sort := NewSort(SortField{Field: "price", Type: SortFieldNumeric})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be sorted by price ascending: 100, 200, 300
	expectedPrices := []int64{100, 200, 300}
	for i, r := range results {
		price := r.SortValues[0].(int64)
		if price != expectedPrices[i] {
			t.Errorf("result[%d] price = %d, want %d", i, price, expectedPrices[i])
		}
	}
}

func TestSearchWithSortNumericDesc(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "apple fruit", price: 300},
		{body: "apple pie", price: 100},
		{body: "apple sauce", price: 200},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "apple")
	sort := NewSort(SortField{Field: "price", Type: SortFieldNumeric, Reverse: true})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Should be sorted by price descending: 300, 200, 100
	expectedPrices := []int64{300, 200, 100}
	for i, r := range results {
		price := r.SortValues[0].(int64)
		if price != expectedPrices[i] {
			t.Errorf("result[%d] price = %d, want %d", i, price, expectedPrices[i])
		}
	}
}

func TestSearchWithSortString(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "item alpha", category: "zebra"},
		{body: "item beta", category: "apple"},
		{body: "item gamma", category: "mango"},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "item")
	sort := NewSort(SortField{Field: "category", Type: SortFieldString})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Sorted by category ordinal (alphabetically): apple, mango, zebra
	expectedCats := []string{"apple", "mango", "zebra"}
	for i, r := range results {
		cat := r.SortValues[0].(string)
		if cat != expectedCats[i] {
			t.Errorf("result[%d] category = %q, want %q", i, cat, expectedCats[i])
		}
	}
}

func TestSearchWithSortTopK(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "apple one", price: 500},
		{body: "apple two", price: 100},
		{body: "apple three", price: 300},
		{body: "apple four", price: 200},
		{body: "apple five", price: 400},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "apple")
	sort := NewSort(SortField{Field: "price", Type: SortFieldNumeric})

	results := searcher.Search(query, NewTopFieldCollector(3, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Top 3 cheapest: 100, 200, 300
	expectedPrices := []int64{100, 200, 300}
	for i, r := range results {
		price := r.SortValues[0].(int64)
		if price != expectedPrices[i] {
			t.Errorf("result[%d] price = %d, want %d", i, price, expectedPrices[i])
		}
	}
}

func TestSearchWithSortSkipsDeletedDocs(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	writer := index.NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	docs := []struct {
		id    string
		body  string
		price int64
	}{
		{"1", "apple fruit", 300},
		{"2", "apple pie", 100}, // will be deleted
		{"3", "apple sauce", 200},
	}
	for _, d := range docs {
		doc := document.NewDocument()
		doc.AddField("id", d.id, document.FieldTypeKeyword)
		doc.AddField("body", d.body, document.FieldTypeText)
		doc.AddNumericDocValuesField("price", d.price)
		writer.AddDocument(doc)
	}
	writer.Commit()

	// Delete doc with price=100
	writer.DeleteDocuments("id", "2")
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "apple")
	sort := NewSort(SortField{Field: "price", Type: SortFieldNumeric})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 2 {
		t.Fatalf("expected 2 results (deleted doc skipped), got %d", len(results))
	}

	// Should be 200, 300 (100 was deleted)
	expectedPrices := []int64{200, 300}
	for i, r := range results {
		price := r.SortValues[0].(int64)
		if price != expectedPrices[i] {
			t.Errorf("result[%d] price = %d, want %d", i, price, expectedPrices[i])
		}
	}

	writer.Close()
}

func TestSearchWithSortMultipleSegments(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	// Buffer size 2 to force multiple segments
	writer := index.NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	prices := []int64{400, 100, 300, 200}
	for _, p := range prices {
		doc := document.NewDocument()
		doc.AddField("body", "item", document.FieldTypeText)
		doc.AddNumericDocValuesField("price", p)
		writer.AddDocument(doc)
	}
	writer.Commit()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	if len(reader.Leaves()) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(reader.Leaves()))
	}

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "item")
	sort := NewSort(SortField{Field: "price", Type: SortFieldNumeric})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	// Should be sorted ascending: 100, 200, 300, 400
	expectedPrices := []int64{100, 200, 300, 400}
	for i, r := range results {
		price := r.SortValues[0].(int64)
		if price != expectedPrices[i] {
			t.Errorf("result[%d] price = %d, want %d", i, price, expectedPrices[i])
		}
	}

	writer.Close()
}

func TestSearchWithSortMultiFieldTiebreaker(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "item alpha", category: "b", price: 200},
		{body: "item beta", category: "a", price: 300},
		{body: "item gamma", category: "b", price: 100},
		{body: "item delta", category: "a", price: 100},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "item")

	// Sort by category (string asc), then by price (numeric asc) as tiebreaker
	sort := NewSort(
		SortField{Field: "category", Type: SortFieldString},
		SortField{Field: "price", Type: SortFieldNumeric},
	)

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	type expected struct {
		cat   string
		price int64
	}
	// category "a" first (ord 0), then "b" (ord 1); within same category, sorted by price
	wants := []expected{
		{"a", 100}, // delta
		{"a", 300}, // beta
		{"b", 100}, // gamma
		{"b", 200}, // alpha
	}
	for i, r := range results {
		cat := r.SortValues[0].(string)
		price := r.SortValues[1].(int64)
		if cat != wants[i].cat || price != wants[i].price {
			t.Errorf("result[%d] = (%q, %d), want (%q, %d)", i, cat, price, wants[i].cat, wants[i].price)
		}
	}
}

func TestSearchWithSortByScore(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "apple banana cherry", price: 100},
		{body: "apple apple apple", price: 200},
		{body: "apple banana", price: 300},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "apple")

	// Sort by score descending (higher score = better)
	sort := NewSort(SortField{Type: SortFieldScore, Reverse: true})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Results should be in descending score order
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score descending: result[%d].Score=%f > result[%d].Score=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}
}

func TestSearchWithSortMissingDocValues(t *testing.T) {
	reader := setupIndexWithDV(t, []testDoc{
		{body: "item alpha", category: "zebra", price: 300},
		{body: "item beta", price: 100}, // no category
		{body: "item gamma", category: "apple", price: 200},
	})

	searcher := NewIndexSearcher(reader)
	query := NewTermQuery("body", "item")
	sort := NewSort(SortField{Field: "category", Type: SortFieldString})

	results := searcher.Search(query, NewTopFieldCollector(10, sort))

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Doc with missing category should have ord -1, which sorts first (lowest)
	// So order should be: missing (nil), "apple", "zebra"
	if results[0].SortValues[0] != nil {
		t.Errorf("result[0] sort value = %v, want nil (missing)", results[0].SortValues[0])
	}
	if results[1].SortValues[0].(string) != "apple" {
		t.Errorf("result[1] category = %v, want %q", results[1].SortValues[0], "apple")
	}
	if results[2].SortValues[0].(string) != "zebra" {
		t.Errorf("result[2] category = %v, want %q", results[2].SortValues[0], "zebra")
	}
}
