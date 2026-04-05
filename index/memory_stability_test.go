package index

import (
	"fmt"
	"runtime"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// TestMemoryStability indexes a large number of documents and verifies that
// heap memory plateaus rather than growing linearly. This is a regression test
// for memory leaks like the flush queue leak (issue #31).
//
// The test indexes 500K docs in batches, committing every 5K docs (triggering
// merges). After an initial ramp-up phase (first 200K), heap should not grow
// more than 2x from the ramp-up measurement.
func TestMemoryStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory stability test in short mode")
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	w := NewIndexWriter(dir, fa, 5_000)
	w.SetMergePolicy(NewTieredMergePolicy())
	defer w.Close()

	const totalDocs = 500_000
	const commitInterval = 5_000
	const rampUpDocs = 200_000

	var rampUpHeap uint64

	for i := range totalDocs {
		doc := document.NewDocument()
		doc.AddField("title", fmt.Sprintf("document number %d about search engines", i), document.FieldTypeText)
		doc.AddField("body", fmt.Sprintf("this is the body of document %d it contains several words about indexing and searching", i), document.FieldTypeText)
		doc.AddField("tag", "stability-test", document.FieldTypeKeyword)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
		if (i+1)%commitInterval == 0 {
			if err := w.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		if i+1 == rampUpDocs {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			rampUpHeap = m.HeapInuse
			t.Logf("ramp-up complete: heap=%d MB, segments=%d",
				rampUpHeap/(1024*1024), len(w.segmentInfos.Segments))
		}
	}

	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	finalHeap := m.HeapInuse

	t.Logf("final: heap=%d MB, segments=%d", finalHeap/(1024*1024), len(w.segmentInfos.Segments))

	// After ramp-up, heap should not grow more than 2x.
	// With the leak, it would grow ~2.55 MB per 1K docs (300K extra docs = ~765 MB).
	// Without the leak, it should stay roughly flat.
	maxAllowed := rampUpHeap * 2
	if finalHeap > maxAllowed {
		t.Errorf("heap grew beyond 2x ramp-up: ramp-up=%d MB, final=%d MB (max allowed=%d MB)",
			rampUpHeap/(1024*1024), finalHeap/(1024*1024), maxAllowed/(1024*1024))
	}
}
