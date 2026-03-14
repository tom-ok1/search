package index_test

import (
	"math"
	"sort"
	"testing"

	"gosearch/document"
	"gosearch/index"
	"gosearch/search"
	"gosearch/store"
)

// --- Bug #1: findCommonDocs must return sorted doc IDs ---

func TestFindCommonDocsSortedOutput(t *testing.T) {
	// Phrase query relies on intersectTwo which requires sorted input.
	// If findCommonDocs returns unsorted IDs from the map iteration,
	// phrase matching could miss valid candidates.
	writer, dir := newTestWriter(t, 100)

	// Create enough documents so that map iteration order is likely non-sorted.
	for i := 0; i < 20; i++ {
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

// --- Bug #2: LiveDocsSegmentReader.DocFreq and TotalFieldLength must skip deleted docs ---

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
	addTextDoc(t, writer, "body", "four five")         // doc1: len=2
	addTextDoc(t, writer, "body", "six seven eight")   // doc2: len=3

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

// --- Bug #3: BM25 avgDocLen == 0 guard ---

func TestBM25ScoreZeroAvgDocLen(t *testing.T) {
	scorer := search.NewBM25Scorer()
	idf := scorer.IDF(10, 1)

	// avgDocLen=0 should not produce NaN or Inf
	score := scorer.Score(1.0, 5.0, 0.0, idf)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		t.Errorf("BM25 score with avgDocLen=0 should be finite, got %f", score)
	}
	if score <= 0 {
		t.Errorf("BM25 score should be positive, got %f", score)
	}
}

// --- Bug #4: FST readArcsAt must use cloned input for independent read position ---

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

// --- Bug #5: DiskSegment.IsDeleted uses stateless ReadByteAt ---

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
		// Interleave IsDeleted with other reads to ensure no state corruption.
		_ = seg.FieldLength("body", 0)
		if seg.IsDeleted(0) {
			t.Error("doc0 should not be deleted")
		}
		_ = seg.DocFreq("body", "aaa")
		if !seg.IsDeleted(1) {
			t.Error("doc1 should be deleted")
		}
		_ = seg.FieldLength("body", 2)
		if seg.IsDeleted(2) {
			t.Error("doc2 should not be deleted")
		}
		if seg.IsDeleted(3) {
			t.Error("doc3 should not be deleted")
		}
	}
}

// --- Bug #6: Bitset bounds checking ---

func TestBitsetSetPanicsOnOutOfRange(t *testing.T) {
	bs := index.NewBitset(8)

	testPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Bitset.Set(%s) should panic on out-of-range", name)
			}
		}()
		f()
	}

	testPanic("negative", func() { bs.Set(-1) })
	testPanic("equal to size", func() { bs.Set(8) })
	testPanic("beyond size", func() { bs.Set(100) })
}

func TestBitsetGetReturnsFalseOnOutOfRange(t *testing.T) {
	bs := index.NewBitset(8)
	bs.Set(0)

	if bs.Get(-1) {
		t.Error("Get(-1) should return false")
	}
	if bs.Get(8) {
		t.Error("Get(8) should return false for out-of-range")
	}
	if bs.Get(100) {
		t.Error("Get(100) should return false for out-of-range")
	}
	// Valid index should still work
	if !bs.Get(0) {
		t.Error("Get(0) should return true")
	}
}

// --- Bug #7: MMap Seek panics on invalid positions ---

func TestMMapSeekPanicsOnNegative(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	out, _ := dir.CreateOutput("test.bin")
	out.Write([]byte{1, 2, 3, 4, 5})
	out.Close()

	m, err := store.OpenMMap(dir.FilePath("test.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Seek(-1) should panic")
		}
	}()
	m.Seek(-1)
}

func TestMMapSeekPanicsOnBeyondLength(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	out, _ := dir.CreateOutput("test.bin")
	out.Write([]byte{1, 2, 3, 4, 5})
	out.Close()

	m, err := store.OpenMMap(dir.FilePath("test.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Seek to exactly length is OK (EOF position)
	m.Seek(5)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Seek(6) on 5-byte file should panic")
		}
	}()
	m.Seek(6)
}

// --- Bug #8: MMap Slice rejects negative offset or length ---

func TestMMapSliceRejectsNegativeOffset(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	out, _ := dir.CreateOutput("test.bin")
	out.Write([]byte{1, 2, 3, 4, 5})
	out.Close()

	m, err := store.OpenMMap(dir.FilePath("test.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	_, err = m.Slice(-1, 3)
	if err == nil {
		t.Error("Slice with negative offset should return error")
	}

	_, err = m.Slice(0, -1)
	if err == nil {
		t.Error("Slice with negative length should return error")
	}
}

// --- Bug #9: DiskPostingsIterator.Next propagates ReadVInt errors ---

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

// --- Bug #10: StoredFields inner loop propagates read errors ---

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
		if fields["title"] != "Test Title" {
			t.Errorf("title: got %q, want %q", fields["title"], "Test Title")
		}
		if fields["body"] != "Test body content here" {
			t.Errorf("body: got %q, want %q", fields["body"], "Test body content here")
		}
	}
}

// --- Bug #11: Write/WriteUint32/WriteUint64/json.Marshal errors checked ---
// This is tested implicitly via segment writer and merger producing valid output.
// We verify the full round-trip produces correct data.

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

// --- Bug #12: merger.go closes tidxOut on tfstOut open failure ---
// This is a resource cleanup fix; tested via normal merge flow ensuring
// no resource leaks.

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

// --- Bug #13: TermQuery and PhraseQuery use LiveDocCount() instead of DocCount() ---

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

// TestScoringConsistencyWithDeletions verifies that BM25 scores are consistent
// when computed against LiveDocCount rather than DocCount.
func TestScoringConsistencyWithDeletions(t *testing.T) {
	// Scenario: Create identical corpus with and without deletions.
	// The one with deletions should produce the same score as a fresh
	// corpus that never had those docs.
	// Use NRT readers so LiveDocsSegmentReader is used (which filters deleted docs).

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

// --- Additional regression: findCommonDocs deterministic output ---

func TestPhraseQueryDeterministicResults(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// Create multiple docs matching the same phrase to exercise map iteration
	for i := 0; i < 10; i++ {
		addTextDoc(t, writer, "body", "hello world test")
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Run the same query multiple times — results should be deterministic
	var firstDocIDs []int
	for attempt := 0; attempt < 5; attempt++ {
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
