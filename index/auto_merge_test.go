package index_test

import (
	"fmt"
	"strings"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/search"
	"gosearch/store"
)

func newTestWriterWithPolicy(t *testing.T, bufferSize int, policy index.MergePolicy) (*index.IndexWriter, store.Directory) {
	t.Helper()
	dir, _ := store.NewFSDirectory(t.TempDir())
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	w := index.NewIndexWriter(dir, analyzer, bufferSize)
	w.SetMergePolicy(policy)
	return w, dir
}

// AC1: Commit() triggers auto-merge when MergePolicy is set.
func TestAutoMergeOnCommit(t *testing.T) {
	policy := index.NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 5

	writer, dir := newTestWriterWithPolicy(t, 100, policy)

	// Create 10 segments by flushing each doc individually
	for i := range 10 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("term%d common", i), document.FieldTypeText)
		writer.AddDocument(doc)
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// At this point, Flush() should have already triggered merges.
	// Commit should also trigger merge on any remaining segments.
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// With SegmentsPerTier=3, we should have far fewer than 10 segments
	leaves := reader.Leaves()
	if len(leaves) >= 10 {
		t.Errorf("expected auto-merge to reduce segment count below 10, got %d", len(leaves))
	}

	// All 10 docs should still be searchable
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(search.NewTermQuery("body", "common"), search.NewTopKCollector(20))
	if len(results) != 10 {
		t.Errorf("expected 10 results after auto-merge, got %d", len(results))
	}
}

// AC2: Flush() (including auto-flush from AddDocument) triggers auto-merge.
func TestAutoMergeOnFlush(t *testing.T) {
	policy := index.NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 5

	// bufferSize=1 means every AddDocument triggers auto-flush
	writer, _ := newTestWriterWithPolicy(t, 1, policy)

	for i := range 10 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("term%d common", i), document.FieldTypeText)
		writer.AddDocument(doc) // auto-flush after each doc
	}

	// After 10 auto-flushes with merge policy, segments should have been merged
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	leaves := reader.Leaves()
	if len(leaves) >= 10 {
		t.Errorf("expected auto-merge to reduce segment count below 10, got %d", len(leaves))
	}

	// All docs should be searchable
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(search.NewTermQuery("body", "common"), search.NewTopKCollector(20))
	if len(results) != 10 {
		t.Errorf("expected 10 results after auto-merge on flush, got %d", len(results))
	}
}

// AC3: NRT reader (nrtSegments) triggers auto-merge.
func TestAutoMergeOnNRTReader(t *testing.T) {
	policy := index.NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 5

	writer, _ := newTestWriterWithPolicy(t, 100, policy)

	// Add 10 docs, flushing each manually (merge policy won't run yet
	// if we set mergePolicy after creating segments... but we set it upfront)
	for i := range 10 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("term%d common", i), document.FieldTypeText)
		writer.AddDocument(doc)
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Opening NRT reader triggers flush + maybeMerge
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all docs are searchable (merge should not lose data)
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(search.NewTermQuery("body", "common"), search.NewTopKCollector(20))
	if len(results) != 10 {
		t.Errorf("expected 10 results after NRT auto-merge, got %d", len(results))
	}
}

// AC4: No MergePolicy set = no auto-merge (backward compatible).
func TestNoAutoMergeWithoutPolicy(t *testing.T) {
	// No merge policy set
	writer, dir := newTestWriter(t, 1) // bufferSize=1 → auto-flush every doc

	for i := range 10 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("term%d common", i), document.FieldTypeText)
		writer.AddDocument(doc) // auto-flush
	}

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Without merge policy, all 10 segments should remain
	leaves := reader.Leaves()
	if len(leaves) != 10 {
		t.Errorf("expected 10 segments without merge policy, got %d", len(leaves))
	}
}

// AC5: Search results are correct after auto-merge with deletions.
func TestAutoMergeWithDeletions(t *testing.T) {
	policy := index.NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 10

	writer, dir := newTestWriterWithPolicy(t, 2, policy)

	for _, text := range []string{
		"alpha beta",    // doc0 — seg0
		"alpha gamma",   // doc1 — seg0
		"beta delta",    // doc2 — seg1
		"gamma epsilon", // doc3 — seg1
		"alpha zeta",    // doc4 — seg2
		"beta eta",      // doc5 — seg2
	} {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc)
	}

	// Delete "alpha" docs (doc0, doc1, doc4)
	writer.DeleteDocuments("body", "alpha")

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := search.NewIndexSearcher(reader)

	// "alpha" should return 0 results (all deleted)
	results := searcher.Search(search.NewTermQuery("body", "alpha"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for 'alpha' (all deleted), got %d", len(results))
	}

	// "beta" should return 2 results (doc2, doc5 survive)
	results = searcher.Search(search.NewTermQuery("body", "beta"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'beta' after delete+auto-merge, got %d", len(results))
	}

	// Total live docs should be 3
	if reader.LiveDocCount() != 3 {
		t.Errorf("expected 3 live docs, got %d", reader.LiveDocCount())
	}
}

// Verify auto-merge produces correct stored fields.
func TestAutoMergePreservesStoredFields(t *testing.T) {
	policy := index.NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 10

	writer, dir := newTestWriterWithPolicy(t, 1, policy)

	texts := []string{"alpha beta", "gamma delta", "epsilon zeta", "eta theta",
		"iota kappa", "lambda mu", "nu xi", "omicron pi", "rho sigma", "tau upsilon"}

	for _, text := range texts {
		doc := document.NewDocument()
		doc.AddField("body", text, document.FieldTypeText)
		writer.AddDocument(doc) // auto-flush + auto-merge
	}

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all stored fields are preserved through merge
	searcher := search.NewIndexSearcher(reader)
	for _, text := range texts {
		// Search for first word of each text
		var term strings.Builder
		for _, c := range text {
			if c == ' ' {
				break
			}
			term.WriteString(string(c))
		}
		results := searcher.Search(search.NewTermQuery("body", term.String()), search.NewTopKCollector(10))
		if len(results) != 1 {
			t.Errorf("expected 1 result for %q, got %d", term.String(), len(results))
			continue
		}
		if results[0].Fields["body"] != text {
			t.Errorf("stored field mismatch for %q: got %q, want %q",
				term.String(), results[0].Fields["body"], text)
		}
	}
}
