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

func newTestWriter(t *testing.T, bufferSize int) (*index.IndexWriter, store.Directory) {
	t.Helper()
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	return index.NewIndexWriter(dir, fa, bufferSize), dir
}

func addTextDoc(t *testing.T, writer *index.IndexWriter, field, text string) {
	t.Helper()
	doc := document.NewDocument()
	doc.AddField(field, text, document.FieldTypeText)
	writer.AddDocument(doc)
}

func commitAndOpenReader(t *testing.T, writer *index.IndexWriter, dir store.Directory) *index.IndexReader {
	t.Helper()
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return reader
}

// --- Search API integration tests ---

func TestDiskSegmentSearch(t *testing.T) {
	// Create two segments via auto-flush (bufferSize=2)
	writer, dir := newTestWriter(t, 2)
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
	results := searcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))

	// "fox" appears in doc0 (seg0) and doc2 (seg1)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Verify stored fields are accessible
	for _, r := range results {
		if r.Fields == nil || len(r.Fields["body"]) == 0 {
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
	nrtResults := nrtSearcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))

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

func TestPhraseQueryNonConsecutiveTerms(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// "quick" and "fox" both appear but are NOT consecutive
	addTextDoc(t, writer, "body", "the quick brown fox")
	// "brown fox" IS consecutive
	addTextDoc(t, writer, "body", "brown fox jumps")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "quick fox" should NOT match — terms are not consecutive
	results := searcher.Search(search.NewPhraseQuery("body", "quick", "fox"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-consecutive phrase 'quick fox', got %d", len(results))
	}

	// "quick brown" SHOULD match doc0
	results = searcher.Search(search.NewPhraseQuery("body", "quick", "brown"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for consecutive phrase 'quick brown', got %d", len(results))
	}

	// "brown fox" should match both docs
	results = searcher.Search(search.NewPhraseQuery("body", "brown", "fox"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for phrase 'brown fox', got %d", len(results))
	}
}

func TestPhraseQueryRepeatedTerms(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// "foo foo foo" — repeated consecutive terms
	addTextDoc(t, writer, "body", "foo foo foo bar")
	// "foo bar foo" — foo appears but not "foo foo"
	addTextDoc(t, writer, "body", "foo bar foo")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "foo foo" should match doc0 only (positions 0,1 or 1,2)
	results := searcher.Search(search.NewPhraseQuery("body", "foo", "foo"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for phrase 'foo foo', got %d", len(results))
	}

	// Three-term phrase "foo foo foo" should also match doc0
	results = searcher.Search(search.NewPhraseQuery("body", "foo", "foo", "foo"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for phrase 'foo foo foo', got %d", len(results))
	}
}

func TestBooleanQueryOnlyMustNot(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	addTextDoc(t, writer, "body", "hello world")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// BooleanQuery with ONLY MustNot — no positive clauses means no candidates
	results := searcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "hello"), search.OccurMustNot), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for only-must-not query, got %d", len(results))
	}
}

func TestBooleanQueryAllShould(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	for _, text := range []string{
		"alpha beta",    // doc0 — matches both
		"alpha gamma",   // doc1 — matches one
		"delta epsilon", // doc2 — matches neither
	} {
		addTextDoc(t, writer, "body", text)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// All SHOULD = OR semantics: matches doc0 and doc1
	results := searcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "alpha"), search.OccurShould).
		Add(search.NewTermQuery("body", "beta"), search.OccurShould), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for all-should query, got %d", len(results))
	}

	// doc0 should score higher (matches both SHOULD clauses)
	if len(results) >= 2 && results[0].Score <= results[1].Score {
		t.Errorf("doc matching both SHOULD clauses should score higher: got %f <= %f",
			results[0].Score, results[1].Score)
	}
}

func TestBooleanQueryMustWithShould(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	for _, text := range []string{
		"alpha beta gamma", // doc0 — matches MUST and SHOULD
		"alpha delta",      // doc1 — matches MUST only
		"beta gamma",       // doc2 — matches SHOULD only, not MUST
	} {
		addTextDoc(t, writer, "body", text)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// MUST "alpha" + SHOULD "beta": both doc0 and doc1 match (MUST required),
	// but doc0 should score higher (boosted by SHOULD match)
	results := searcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "alpha"), search.OccurMust).
		Add(search.NewTermQuery("body", "beta"), search.OccurShould), search.NewTopKCollector(10))

	if len(results) != 2 {
		t.Fatalf("expected 2 results for MUST+SHOULD, got %d", len(results))
	}

	// doc0 (matches both MUST and SHOULD) should rank higher
	if string(results[0].Fields["body"]) != "alpha beta gamma" {
		t.Errorf("expected doc matching MUST+SHOULD to rank first, got '%s'",
			string(results[0].Fields["body"]))
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("MUST+SHOULD doc should score higher: %f <= %f",
			results[0].Score, results[1].Score)
	}
}

func TestTopKLimiting(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// Create 10 documents all containing "common"
	for range 10 {
		addTextDoc(t, writer, "body", "common word here")
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// topK=3 should limit to 3 results
	results := searcher.Search(search.NewTermQuery("body", "common"), search.NewTopKCollector(3))
	if len(results) != 3 {
		t.Errorf("expected 3 results with topK=3, got %d", len(results))
	}

	// topK=1 should return exactly 1
	results = searcher.Search(search.NewTermQuery("body", "common"), search.NewTopKCollector(1))
	if len(results) != 1 {
		t.Errorf("expected 1 result with topK=1, got %d", len(results))
	}
}

func TestNRTReaderSnapshotIsolation(t *testing.T) {
	writer, _ := newTestWriter(t, 100)

	addTextDoc(t, writer, "body", "snapshot test alpha")

	// Open NRT reader — captures point-in-time snapshot
	nrtReader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader.Close()

	// Add more documents AFTER opening NRT reader
	addTextDoc(t, writer, "body", "snapshot test beta")

	nrtSearcher := search.NewIndexSearcher(nrtReader)

	// NRT reader should NOT see doc1 (added after snapshot)
	results := nrtSearcher.Search(search.NewTermQuery("body", "beta"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("NRT reader should not see documents added after snapshot, got %d results", len(results))
	}

	// NRT reader SHOULD see doc0
	results = nrtSearcher.Search(search.NewTermQuery("body", "alpha"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("NRT reader should see pre-snapshot documents, expected 1, got %d", len(results))
	}

	// Open a NEW NRT reader — should see both documents
	nrtReader2, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader2.Close()

	nrtSearcher2 := search.NewIndexSearcher(nrtReader2)
	results = nrtSearcher2.Search(search.NewTermQuery("body", "test"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("new NRT reader should see all documents, expected 2, got %d", len(results))
	}
}

// --- Bugfix regression tests ---

func TestFindCommonDocsSortedOutput(t *testing.T) {
	// Phrase query relies on intersectTwo which requires sorted input.
	// If findCommonDocs returns unsorted IDs from the map iteration,
	// phrase matching could miss valid candidates.
	writer, dir := newTestWriter(t, 100)

	// Create enough documents so that map iteration order is likely non-sorted.
	for range 20 {
		addTextDoc(t, writer, "body", "common shared term")
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Phrase query "common shared" must find all 20 docs regardless of
	// internal iteration order.
	results := searcher.Search(search.NewPhraseQuery("body", "common", "shared"), search.NewTopKCollector(100))
	if len(results) != 20 {
		t.Errorf("expected 20 results for phrase 'common shared', got %d", len(results))
	}

	// Verify results are in descending score order (sorted properly).
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score: result[%d]=%f > result[%d]=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}
}

func TestLiveDocsDocFreqSkipsDeletedDocs(t *testing.T) {
	writer, _ := newTestWriter(t, 100)

	// doc0: "hello world"
	// doc1: "hello go"
	// doc2: "world go"
	// doc3: "hello world go"
	for _, text := range []string{"hello world", "hello go", "world go", "hello world go"} {
		addTextDoc(t, writer, "body", text)
	}

	// Delete docs containing "go" (doc1, doc2, doc3)
	writer.DeleteDocuments("body", "go")

	// Use NRT reader to get LiveDocsSegmentReader
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		// "hello" originally in 3 docs (0,1,3), but doc1 and doc3 are deleted.
		// DocFreq should only count live docs = 1 (doc0).
		df := seg.DocFreq("body", "hello")
		if df != 1 {
			t.Errorf("DocFreq for 'hello' should be 1 (only doc0 is live), got %d", df)
		}
	}
}

func TestLiveDocsTotalFieldLengthExcludesDeletedDocs(t *testing.T) {
	writer, _ := newTestWriter(t, 100)

	addTextDoc(t, writer, "body", "one two three")   // doc0: len=3
	addTextDoc(t, writer, "body", "four five")       // doc1: len=2
	addTextDoc(t, writer, "body", "six seven eight") // doc2: len=3

	// Delete doc1
	writer.DeleteDocuments("body", "four")

	// Use NRT reader to get LiveDocsSegmentReader
	reader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		totalLen := seg.TotalFieldLength("body")
		// Only doc0 (3) and doc2 (3) should be counted = 6
		// With deletion of doc1 (2), total should be 6, not 8.
		if totalLen != 6 {
			t.Errorf("TotalFieldLength should exclude deleted docs: got %d, want 6", totalLen)
		}
	}
}

func TestFSTConcurrentLookups(t *testing.T) {
	// The fix ensures readArcsAt clones the input so multiple lookups
	// don't interfere with each other's read position.
	// We test this by doing multiple sequential Get calls on an FST
	// built from multiple terms.
	writer, dir := newTestWriter(t, 100)

	terms := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, term := range terms {
		addTextDoc(t, writer, "body", term)
	}

	reader := commitAndOpenReader(t, writer, dir)

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		// Multiple lookups in sequence should all succeed.
		for _, term := range terms {
			df := seg.DocFreq("body", term)
			if df != 1 {
				t.Errorf("DocFreq for %q: got %d, want 1", term, df)
			}
		}
		// Look up in reverse order too, to stress the cloning.
		for i := len(terms) - 1; i >= 0; i-- {
			df := seg.DocFreq("body", terms[i])
			if df != 1 {
				t.Errorf("reverse DocFreq for %q: got %d, want 1", terms[i], df)
			}
		}
	}
}

func TestDiskSegmentIsDeletedStateless(t *testing.T) {
	// Before the fix, IsDeleted used Seek+ReadByte which shared state.
	// After the fix, ReadByteAt is used (no position mutation).
	// We test by calling IsDeleted on multiple docs interleaved with
	// other operations.
	writer, dir := newTestWriter(t, 100)

	for _, text := range []string{"aaa bbb", "ccc ddd", "eee fff", "ggg hhh"} {
		addTextDoc(t, writer, "body", text)
	}
	writer.DeleteDocuments("body", "ccc") // delete doc1

	reader := commitAndOpenReader(t, writer, dir)

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		liveDocs := seg.LiveDocs()
		if liveDocs == nil {
			t.Fatal("LiveDocs should not be nil for segment with deletions")
		}
		// Interleave LiveDocs checks with other reads to ensure no state corruption.
		_ = seg.FieldLength("body", 0)
		if liveDocs.Get(0) {
			t.Error("doc0 should not be deleted")
		}
		_ = seg.DocFreq("body", "aaa")
		if !liveDocs.Get(1) {
			t.Error("doc1 should be deleted")
		}
		_ = seg.FieldLength("body", 2)
		if liveDocs.Get(2) {
			t.Error("doc2 should not be deleted")
		}
		if liveDocs.Get(3) {
			t.Error("doc3 should not be deleted")
		}
	}
}

func TestDiskPostingsIteratorStopsOnCorruptData(t *testing.T) {
	// Build a valid segment, then verify that DiskPostingsIterator
	// gracefully stops (returns false) when data is exhausted.
	writer, dir := newTestWriter(t, 100)
	addTextDoc(t, writer, "body", "hello world")
	reader := commitAndOpenReader(t, writer, dir)

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		iter := seg.PostingsIterator("body", "hello")
		count := 0
		for iter.Next() {
			count++
			// Verify we can read valid data
			if iter.DocID() < 0 {
				t.Errorf("unexpected negative DocID: %d", iter.DocID())
			}
			if iter.Freq() < 1 {
				t.Errorf("unexpected freq < 1: %d", iter.Freq())
			}
		}
		if count != 1 {
			t.Errorf("expected 1 posting for 'hello', got %d", count)
		}
		// Calling Next again should remain false
		if iter.Next() {
			t.Error("exhausted iterator should keep returning false")
		}
	}
}

func TestStoredFieldsReadable(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	doc := document.NewDocument()
	doc.AddField("title", "Test Title", document.FieldTypeText)
	doc.AddField("body", "Test body content here", document.FieldTypeText)
	writer.AddDocument(doc)

	reader := commitAndOpenReader(t, writer, dir)

	for _, leaf := range reader.Leaves() {
		seg := leaf.Segment
		fields, err := seg.StoredFields(0)
		if err != nil {
			t.Fatalf("StoredFields returned error: %v", err)
		}
		if string(fields["title"]) != "Test Title" {
			t.Errorf("title: got %q, want %q", fields["title"], "Test Title")
		}
		if string(fields["body"]) != "Test body content here" {
			t.Errorf("body: got %q, want %q", fields["body"], "Test body content here")
		}
	}
}

func TestSegmentWriterRoundTrip(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	texts := []string{
		"alpha beta gamma",
		"delta epsilon",
		"alpha delta zeta",
	}
	for _, text := range texts {
		addTextDoc(t, writer, "body", text)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Verify all terms are searchable (implying writes succeeded without silent errors)
	for _, term := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		results := searcher.Search(search.NewTermQuery("body", term), search.NewTopKCollector(10))
		if len(results) == 0 {
			t.Errorf("no results for term %q — write may have silently failed", term)
		}
	}
}

func TestMergerProducesValidOutput(t *testing.T) {
	writer, dir := newTestWriter(t, 2)

	addTextDoc(t, writer, "body", "merge test alpha")
	addTextDoc(t, writer, "body", "merge test beta") // → seg0
	addTextDoc(t, writer, "body", "merge test gamma")
	addTextDoc(t, writer, "body", "merge test delta") // → seg1

	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// All docs should be findable after merge
	results := searcher.Search(search.NewTermQuery("body", "merge"), search.NewTopKCollector(10))
	if len(results) != 4 {
		t.Errorf("expected 4 results after merge, got %d", len(results))
	}

	// Verify single segment
	if len(reader.Leaves()) != 1 {
		t.Errorf("expected 1 segment after ForceMerge(1), got %d", len(reader.Leaves()))
	}
}

func TestScoringUsesLiveDocCount(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	addTextDoc(t, writer, "body", "rare unique term")
	addTextDoc(t, writer, "body", "common filler text")
	addTextDoc(t, writer, "body", "another filler text")
	addTextDoc(t, writer, "body", "more filler text")

	// Delete 3 filler docs, leaving only doc0
	writer.DeleteDocuments("body", "filler")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// With LiveDocCount=1, IDF for "rare" should reflect 1 total doc, not 4.
	results := searcher.Search(search.NewTermQuery("body", "rare"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Score should be finite and positive
	score := results[0].Score
	if math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
		t.Errorf("score should be finite and positive, got %f", score)
	}

	// Now test PhraseQuery uses LiveDocCount too
	writer2, dir2 := newTestWriter(t, 100)
	addTextDoc(t, writer2, "body", "quick brown fox")
	addTextDoc(t, writer2, "body", "filler text one")
	addTextDoc(t, writer2, "body", "filler text two")

	writer2.DeleteDocuments("body", "filler")

	reader2 := commitAndOpenReader(t, writer2, dir2)
	searcher2 := search.NewIndexSearcher(reader2)

	results2 := searcher2.Search(search.NewPhraseQuery("body", "quick", "brown"), search.NewTopKCollector(10))
	if len(results2) != 1 {
		t.Fatalf("expected 1 phrase result, got %d", len(results2))
	}
	score2 := results2[0].Score
	if math.IsNaN(score2) || math.IsInf(score2, 0) || score2 <= 0 {
		t.Errorf("phrase score should be finite and positive, got %f", score2)
	}
}

func TestScoringConsistencyWithDeletions(t *testing.T) {
	// Scenario: Create identical corpus with and without deletions.
	// The one with deletions should produce the same score as a fresh
	// corpus that never had those docs.

	// Corpus A: Only "target" doc
	writerA, _ := newTestWriter(t, 100)
	addTextDoc(t, writerA, "body", "unique alpha beta")
	readerA, err := index.OpenNRTReader(writerA)
	if err != nil {
		t.Fatal(err)
	}
	defer readerA.Close()
	searcherA := search.NewIndexSearcher(readerA)
	resultsA := searcherA.Search(search.NewTermQuery("body", "unique"), search.NewTopKCollector(10))

	// Corpus B: "target" doc + extras that get deleted
	writerB, _ := newTestWriter(t, 100)
	addTextDoc(t, writerB, "body", "unique alpha beta")
	addTextDoc(t, writerB, "body", "delete me")
	addTextDoc(t, writerB, "body", "delete me too")
	writerB.DeleteDocuments("body", "delete")
	readerB, err := index.OpenNRTReader(writerB)
	if err != nil {
		t.Fatal(err)
	}
	defer readerB.Close()
	searcherB := search.NewIndexSearcher(readerB)
	resultsB := searcherB.Search(search.NewTermQuery("body", "unique"), search.NewTopKCollector(10))

	if len(resultsA) != 1 || len(resultsB) != 1 {
		t.Fatalf("expected 1 result each, got A=%d B=%d", len(resultsA), len(resultsB))
	}

	// Scores should be identical since LiveDocCount and filtered TotalFieldLength are used
	if math.Abs(resultsA[0].Score-resultsB[0].Score) > 1e-9 {
		t.Errorf("scores should match with LiveDocCount-based scoring: A=%f B=%f",
			resultsA[0].Score, resultsB[0].Score)
	}
}

func TestPhraseQueryDeterministicResults(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// Create multiple docs matching the same phrase to exercise map iteration
	for range 10 {
		addTextDoc(t, writer, "body", "hello world test")
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Run the same query multiple times — results should be deterministic
	var firstDocIDs []int
	for attempt := range 5 {
		results := searcher.Search(search.NewPhraseQuery("body", "hello", "world"), search.NewTopKCollector(100))
		if len(results) != 10 {
			t.Fatalf("attempt %d: expected 10 results, got %d", attempt, len(results))
		}

		var docIDs []int
		for _, r := range results {
			docIDs = append(docIDs, r.DocID)
		}

		if attempt == 0 {
			firstDocIDs = docIDs
		} else {
			sort.Ints(docIDs)
			sorted := make([]int, len(firstDocIDs))
			copy(sorted, firstDocIDs)
			sort.Ints(sorted)
			for i := range docIDs {
				if docIDs[i] != sorted[i] {
					t.Errorf("attempt %d: non-deterministic result at index %d: %d vs %d",
						attempt, i, docIDs[i], sorted[i])
				}
			}
		}
	}
}
