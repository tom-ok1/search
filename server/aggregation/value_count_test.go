package aggregation

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestValueCountAggregator(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter())
	fa := analysis.NewFieldAnalyzers(analyzer)
	writer := index.NewIndexWriter(dir, fa, 1024)

	statuses := []string{"active", "inactive", "active", "pending", "active"}
	for i, status := range statuses {
		doc := document.NewDocument()
		doc.AddField("_id", fmt.Sprintf("%d", i+1), document.FieldTypeKeyword)
		doc.AddField("status", status, document.FieldTypeKeyword)
		writer.AddDocument(doc)
	}

	writer.Flush()

	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatalf("OpenNRTReader: %v", err)
	}
	defer reader.Close()

	agg := NewValueCountAggregator("status_count", "status")

	for _, leaf := range reader.Leaves() {
		leafAgg := agg.GetLeafAggregator(leaf)
		for docID := 0; docID < leaf.Segment.DocCount(); docID++ {
			leafAgg.Collect(docID)
		}
	}

	result := agg.BuildResult()

	if result.Name != "status_count" {
		t.Errorf("expected name 'status_count', got %q", result.Name)
	}
	if result.Type != "value_count" {
		t.Errorf("expected type 'value_count', got %q", result.Type)
	}

	count, ok := result.Value.(int64)
	if !ok {
		t.Fatalf("expected Value to be int64, got %T", result.Value)
	}
	if count != 5 {
		t.Errorf("expected count 5, got %d", count)
	}
}
