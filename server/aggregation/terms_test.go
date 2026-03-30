package aggregation

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

func TestTermsAggregator(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	fa := analysis.NewFieldAnalyzers(analysis.DefaultRegistry().Get("standard"))
	writer := index.NewIndexWriter(dir, fa, 100)

	statuses := []string{"active", "inactive", "active", "inactive", "active"}
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

	agg := NewTermsAggregator("status_terms", "status", 10)

	for _, leaf := range reader.Leaves() {
		leafAgg := agg.GetLeafAggregator(leaf)
		for docID := 0; docID < leaf.Segment.DocCount(); docID++ {
			leafAgg.Collect(docID)
		}
	}

	result := agg.BuildResult()

	if result.Name != "status_terms" {
		t.Errorf("expected name 'status_terms', got %q", result.Name)
	}
	if result.Type != "terms" {
		t.Errorf("expected type 'terms', got %q", result.Type)
	}
	if len(result.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(result.Buckets))
	}

	// Sorted by count desc: active=3, inactive=2
	if result.Buckets[0].Key != "active" || result.Buckets[0].DocCount != 3 {
		t.Errorf("expected first bucket {active, 3}, got {%s, %d}",
			result.Buckets[0].Key, result.Buckets[0].DocCount)
	}
	if result.Buckets[1].Key != "inactive" || result.Buckets[1].DocCount != 2 {
		t.Errorf("expected second bucket {inactive, 2}, got {%s, %d}",
			result.Buckets[1].Key, result.Buckets[1].DocCount)
	}
}

func TestTermsAggregator_SizeLimit(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	fa := analysis.NewFieldAnalyzers(analysis.DefaultRegistry().Get("standard"))
	writer := index.NewIndexWriter(dir, fa, 100)

	// 3 distinct values: a=3, b=2, c=1
	values := []string{"a", "b", "a", "c", "b", "a"}
	for i, v := range values {
		doc := document.NewDocument()
		doc.AddField("_id", fmt.Sprintf("%d", i+1), document.FieldTypeKeyword)
		doc.AddField("tag", v, document.FieldTypeKeyword)
		writer.AddDocument(doc)
	}

	writer.Flush()

	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatalf("OpenNRTReader: %v", err)
	}
	defer reader.Close()

	agg := NewTermsAggregator("tag_terms", "tag", 2)

	for _, leaf := range reader.Leaves() {
		leafAgg := agg.GetLeafAggregator(leaf)
		for docID := 0; docID < leaf.Segment.DocCount(); docID++ {
			leafAgg.Collect(docID)
		}
	}

	result := agg.BuildResult()

	if len(result.Buckets) != 2 {
		t.Fatalf("expected 2 buckets (size limit), got %d", len(result.Buckets))
	}
	if result.Buckets[0].Key != "a" || result.Buckets[0].DocCount != 3 {
		t.Errorf("expected first bucket {a, 3}, got {%s, %d}",
			result.Buckets[0].Key, result.Buckets[0].DocCount)
	}
	if result.Buckets[1].Key != "b" || result.Buckets[1].DocCount != 2 {
		t.Errorf("expected second bucket {b, 2}, got {%s, %d}",
			result.Buckets[1].Key, result.Buckets[1].DocCount)
	}
}
