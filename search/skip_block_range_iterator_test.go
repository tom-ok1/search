package search

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func createSkipperForTest(t *testing.T, prices []int64) *index.DocValuesSkipper {
	t.Helper()
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	writer := index.NewIndexWriter(dir, analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}), len(prices)+1)
	for _, p := range prices {
		doc := document.NewDocument()
		doc.AddField("body", "test", document.FieldTypeText)
		doc.AddNumericDocValuesField("price", p)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })

	leaves := reader.Leaves()
	if len(leaves) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(leaves))
	}

	return leaves[0].Segment.DocValuesSkipper("price")
}

func TestSkipBlockRangeIteratorStartsAtMinusOne(t *testing.T) {
	skipper := createSkipperForTest(t, []int64{1, 2, 3})
	iter := NewSkipBlockRangeIterator(skipper, 0, 10)
	if iter.DocID() != -1 {
		t.Errorf("initial DocID() = %d, want -1", iter.DocID())
	}
}

func TestSkipBlockRangeIteratorAllCompetitive(t *testing.T) {
	skipper := createSkipperForTest(t, []int64{10, 20, 30, 40, 50})
	iter := NewSkipBlockRangeIterator(skipper, 0, 100)

	for expected := range 5 {
		doc := iter.NextDoc()
		if doc != expected {
			t.Fatalf("NextDoc() = %d, want %d", doc, expected)
		}
	}
	if iter.NextDoc() != NoMoreDocs {
		t.Error("expected NoMoreDocs after exhausting all docs")
	}
}

func TestSkipBlockRangeIteratorAdvance(t *testing.T) {
	skipper := createSkipperForTest(t, []int64{10, 20, 30, 40, 50})
	iter := NewSkipBlockRangeIterator(skipper, 0, 100)

	doc := iter.Advance(3)
	if doc != 3 {
		t.Errorf("Advance(3) = %d, want 3", doc)
	}
}

func TestSkipBlockRangeIteratorNoCompetitiveBlocks(t *testing.T) {
	skipper := createSkipperForTest(t, []int64{10, 20, 30})
	iter := NewSkipBlockRangeIterator(skipper, 100, 200)

	doc := iter.NextDoc()
	if doc != NoMoreDocs {
		t.Errorf("NextDoc() = %d, want NoMoreDocs", doc)
	}
}

func TestSkipBlockRangeIteratorCost(t *testing.T) {
	skipper := createSkipperForTest(t, []int64{10, 20, 30})
	iter := NewSkipBlockRangeIterator(skipper, 0, 100)

	if iter.Cost() != int64(NoMoreDocs) {
		t.Errorf("Cost() = %d, want %d", iter.Cost(), NoMoreDocs)
	}
}
