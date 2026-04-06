package index

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestSegmentCountGaugeOnFlush(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	// Small buffer to force flushes
	w := NewIndexWriter(dir, fa, 5)
	defer w.Close()

	m := w.Metrics()

	// Initially should be 0
	if m.SegmentCount.Load() != 0 {
		t.Errorf("initial SegmentCount = %d, want 0", m.SegmentCount.Load())
	}

	// Add enough docs to trigger a flush
	for i := range 10 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("test document %d", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	// After flush, should have at least 1 segment
	if m.SegmentCount.Load() == 0 {
		t.Error("SegmentCount should be > 0 after flush")
	}

	// Verify it matches the actual segment count
	actualCount := int64(len(w.segmentInfos.Segments))
	if m.SegmentCount.Load() != actualCount {
		t.Errorf("SegmentCount = %d, want %d", m.SegmentCount.Load(), actualCount)
	}
}
