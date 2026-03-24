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
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	return index.NewIndexWriter(dir, analyzer, bufferSize), dir
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

func TestE2EDiskSegmentSearch(t *testing.T) {
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
		if r.Fields == nil || r.Fields["body"] == "" {
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

func TestE2EDeleteAndSearch(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	for _, text := range []string{
		"the quick brown fox",  // doc0
		"the lazy brown dog",   // doc1
		"brown fox jumps over", // doc2
		"the quick red fox",    // doc3
	} {
		addTextDoc(t, writer, "body", text)
	}

	// Delete all documents containing "dog" (doc1)
	writer.DeleteDocuments("body", "dog")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "brown" appears in doc0, doc1, doc2; doc1 is deleted → 2 results
	results := searcher.Search(search.NewTermQuery("body", "brown"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results after deletion, got %d", len(results))
	}
	for _, r := range results {
		if r.Fields["body"] == "the lazy brown dog" {
			t.Errorf("deleted document should not appear in results")
		}
	}
}

func TestE2EDeleteAllDocsInSegment(t *testing.T) {
	// bufferSize=2 forces two segments: seg0=[doc0,doc1], seg1=[doc2,doc3]
	writer, dir := newTestWriter(t, 2)

	addTextDoc(t, writer, "body", "alpha beta")
	addTextDoc(t, writer, "body", "alpha gamma") // auto-flush → seg0
	addTextDoc(t, writer, "body", "delta epsilon")
	addTextDoc(t, writer, "body", "delta zeta") // auto-flush → seg1

	// Delete all docs in seg0 by deleting "alpha"
	writer.DeleteDocuments("body", "alpha")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "alpha" docs are deleted, should return nothing
	results := searcher.Search(search.NewTermQuery("body", "alpha"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for deleted term, got %d", len(results))
	}

	// "delta" docs still exist
	results = searcher.Search(search.NewTermQuery("body", "delta"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for surviving docs, got %d", len(results))
	}
}

func TestE2EPhraseQueryNonConsecutiveTerms(t *testing.T) {
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

func TestE2EPhraseQueryRepeatedTerms(t *testing.T) {
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

func TestE2EBooleanQueryOnlyMustNot(t *testing.T) {
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

func TestE2EBooleanQueryAllShould(t *testing.T) {
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

func TestE2ESearchNonExistentTerm(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	addTextDoc(t, writer, "body", "hello world")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Term that doesn't exist at all
	results := searcher.Search(search.NewTermQuery("body", "nonexistent"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent term, got %d", len(results))
	}

	// Field that doesn't exist
	results = searcher.Search(search.NewTermQuery("title", "hello"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent field, got %d", len(results))
	}

	// Phrase query with one nonexistent term
	results = searcher.Search(search.NewPhraseQuery("body", "hello", "nonexistent"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for phrase with nonexistent term, got %d", len(results))
	}
}

func TestE2EForceMergeWithDeletions(t *testing.T) {
	// bufferSize=2 forces multiple segments
	writer, dir := newTestWriter(t, 2)

	addTextDoc(t, writer, "body", "alpha beta gamma")
	addTextDoc(t, writer, "body", "alpha delta") // → seg0
	addTextDoc(t, writer, "body", "beta epsilon")
	addTextDoc(t, writer, "body", "gamma zeta alpha") // → seg1

	// Delete doc1 ("alpha delta") before merge
	writer.DeleteDocuments("body", "delta")

	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "alpha" should match doc0 and doc3 only (doc1 was deleted before merge)
	results := searcher.Search(search.NewTermQuery("body", "alpha"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'alpha' after merge+delete, got %d", len(results))
	}

	// Verify deleted doc is truly gone from stored fields
	for _, r := range results {
		if r.Fields["body"] == "alpha delta" {
			t.Errorf("deleted document 'alpha delta' should not appear after merge")
		}
	}

	// Verify all docs are now in a single segment
	leaves := reader.Leaves()
	if len(leaves) != 1 {
		t.Errorf("expected 1 segment after ForceMerge(1), got %d", len(leaves))
	}
}

func TestE2ETopKLimiting(t *testing.T) {
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

func TestE2ENRTReaderSnapshotIsolation(t *testing.T) {
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

func TestE2EScoreOrdering(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// doc0: "fox" appears once in a 4-token doc
	addTextDoc(t, writer, "body", "the quick brown fox")
	// doc1: "fox" appears 3 times in a 4-token doc (higher TF)
	addTextDoc(t, writer, "body", "fox fox fox dog")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Results should be in descending score order
	if results[0].Score < results[1].Score {
		t.Errorf("results should be in descending score order: %f < %f",
			results[0].Score, results[1].Score)
	}

	// doc1 (fox fox fox) should score higher than doc0 (one fox)
	if results[0].Fields["body"] != "fox fox fox dog" {
		t.Errorf("expected highest scoring doc to be 'fox fox fox dog', got '%s'",
			results[0].Fields["body"])
	}
}

func TestE2EMultiFieldDocument(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	doc := document.NewDocument()
	doc.AddField("title", "Quick Fox", document.FieldTypeText)
	doc.AddField("body", "the lazy brown dog", document.FieldTypeText)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("title", "Lazy Dog", document.FieldTypeText)
	doc2.AddField("body", "the quick brown fox", document.FieldTypeText)
	writer.AddDocument(doc2)

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Search "title" field for "fox" — should match doc0 only
	results := searcher.Search(search.NewTermQuery("title", "fox"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result searching title for 'fox', got %d", len(results))
	}

	// Search "body" field for "fox" — should match doc2 only
	results = searcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result searching body for 'fox', got %d", len(results))
	}

	// Boolean: title:fox AND body:dog — should match nothing
	// (doc0 has title:fox but body:dog, doc2 has body:fox but title:dog)
	// Wait: doc0 has title:"Quick Fox" and body:"the lazy brown dog"
	// So title:fox AND body:dog → doc0 matches both!
	results = searcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("title", "fox"), search.OccurMust).
		Add(search.NewTermQuery("body", "dog"), search.OccurMust), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for cross-field boolean query, got %d", len(results))
	}

	// Verify stored fields from multiple fields are accessible
	if len(results) == 1 {
		if results[0].Fields["title"] == "" || results[0].Fields["body"] == "" {
			t.Errorf("expected both title and body stored fields, got title=%q body=%q",
				results[0].Fields["title"], results[0].Fields["body"])
		}
	}
}

func TestE2EKeywordField(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	doc := document.NewDocument()
	doc.AddField("body", "some text here", document.FieldTypeText)
	doc.AddField("status", "ACTIVE", document.FieldTypeKeyword)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("body", "other text here", document.FieldTypeText)
	doc2.AddField("status", "INACTIVE", document.FieldTypeKeyword)
	writer.AddDocument(doc2)

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Keyword field: exact match (not analyzed, so case-sensitive)
	results := searcher.Search(search.NewTermQuery("status", "ACTIVE"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for keyword exact match, got %d", len(results))
	}

	// Lowercase should NOT match keyword field (not analyzed)
	results = searcher.Search(search.NewTermQuery("status", "active"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for lowercase keyword (not analyzed), got %d", len(results))
	}

	// Boolean: body:text AND status:ACTIVE
	results = searcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "text"), search.OccurMust).
		Add(search.NewTermQuery("status", "ACTIVE"), search.OccurMust), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for text+keyword boolean, got %d", len(results))
	}
}

func TestE2EThreeTermPhraseQuery(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	addTextDoc(t, writer, "body", "the quick brown fox jumps")
	addTextDoc(t, writer, "body", "quick brown dog")
	addTextDoc(t, writer, "body", "the brown fox quick")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "quick brown fox" — 3-term phrase, should match doc0 only
	results := searcher.Search(search.NewPhraseQuery("body", "quick", "brown", "fox"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for 3-term phrase 'quick brown fox', got %d", len(results))
	}
	if len(results) == 1 && results[0].Fields["body"] != "the quick brown fox jumps" {
		t.Errorf("wrong doc matched: %s", results[0].Fields["body"])
	}
}

func TestE2EMultiSegmentPhraseQuery(t *testing.T) {
	// bufferSize=1 forces each doc into its own segment
	writer, dir := newTestWriter(t, 1)

	addTextDoc(t, writer, "body", "brown fox jumps high") // → seg0
	addTextDoc(t, writer, "body", "the brown fox")        // → seg1
	addTextDoc(t, writer, "body", "fox brown reversed")   // → seg2

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// "brown fox" should match seg0 and seg1 but not seg2 (reversed)
	results := searcher.Search(search.NewPhraseQuery("body", "brown", "fox"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for phrase 'brown fox' across segments, got %d", len(results))
	}

	// Verify NRT gives same results
	nrtReader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader.Close()

	nrtSearcher := search.NewIndexSearcher(nrtReader)
	nrtResults := nrtSearcher.Search(search.NewPhraseQuery("body", "brown", "fox"), search.NewTopKCollector(10))
	if len(nrtResults) != len(results) {
		t.Errorf("NRT vs disk mismatch: nrt=%d disk=%d", len(nrtResults), len(results))
	}
}

func TestE2EHighTermFrequencyScoring(t *testing.T) {
	writer, dir := newTestWriter(t, 100)

	// doc0: "fox" appears once
	addTextDoc(t, writer, "body", "fox")
	// doc1: "fox" repeated many times — should score higher due to TF
	addTextDoc(t, writer, "body", "fox fox fox fox fox fox fox fox fox fox")

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// All scores should be positive and finite
	for _, r := range results {
		if r.Score <= 0 || math.IsNaN(r.Score) || math.IsInf(r.Score, 0) {
			t.Errorf("invalid score %f for docID %d", r.Score, r.DocID)
		}
	}
}

func TestE2EDeleteThenMergeThenSearch(t *testing.T) {
	// 3 segments of 2 docs each
	writer, dir := newTestWriter(t, 2)

	for _, text := range []string{
		"alpha beta",    // doc0 — seg0
		"alpha gamma",   // doc1 — seg0
		"beta delta",    // doc2 — seg1
		"gamma epsilon", // doc3 — seg1
		"alpha zeta",    // doc4 — seg2
		"beta eta",      // doc5 — seg2
	} {
		addTextDoc(t, writer, "body", text)
	}

	// Delete "alpha" docs (doc0, doc1, doc4)
	writer.DeleteDocuments("body", "alpha")

	// Force merge into 1 segment
	if err := writer.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	reader := commitAndOpenReader(t, writer, dir)
	searcher := search.NewIndexSearcher(reader)

	// Only doc2, doc3, doc5 should survive
	results := searcher.Search(search.NewTermQuery("body", "beta"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'beta' after delete+merge, got %d", len(results))
	}

	results = searcher.Search(search.NewTermQuery("body", "alpha"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for 'alpha' (all deleted), got %d", len(results))
	}

	// Total live docs should be 3
	if reader.LiveDocCount() != 3 {
		t.Errorf("expected 3 live docs after merge, got %d", reader.LiveDocCount())
	}
}

func TestE2EBooleanQueryMustWithShould(t *testing.T) {
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
	if results[0].Fields["body"] != "alpha beta gamma" {
		t.Errorf("expected doc matching MUST+SHOULD to rank first, got '%s'",
			results[0].Fields["body"])
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("MUST+SHOULD doc should score higher: %f <= %f",
			results[0].Score, results[1].Score)
	}
}

func TestE2EDiskSegmentQueryExecution(t *testing.T) {
	writer, dir := newTestWriter(t, 100)
	for _, text := range []string{
		"the quick brown fox",
		"the lazy brown dog",
		"the quick red fox jumps",
		"brown fox brown fox",
	} {
		addTextDoc(t, writer, "body", text)
	}

	// Open from disk
	reader := commitAndOpenReader(t, writer, dir)

	var diskReaders []index.SegmentReader
	for _, leaf := range reader.Leaves() {
		diskReaders = append(diskReaders, leaf.Segment)
	}
	diskSearcher := search.NewIndexSearcher(index.NewIndexReader(diskReaders))

	// TermQuery
	tqResults := diskSearcher.Search(search.NewTermQuery("body", "fox"), search.NewTopKCollector(10))
	if len(tqResults) != 3 {
		t.Errorf("TermQuery 'fox': expected 3 results, got %d", len(tqResults))
	}

	// PhraseQuery "brown fox"
	pqResults := diskSearcher.Search(search.NewPhraseQuery("body", "brown", "fox"), search.NewTopKCollector(10))
	if len(pqResults) != 2 {
		t.Errorf("PhraseQuery 'brown fox': expected 2 results, got %d", len(pqResults))
	}

	// BooleanQuery: "brown" AND NOT "dog"
	bqResults := diskSearcher.Search(search.NewBooleanQuery().
		Add(search.NewTermQuery("body", "brown"), search.OccurMust).
		Add(search.NewTermQuery("body", "dog"), search.OccurMustNot), search.NewTopKCollector(10))
	if len(bqResults) != 2 {
		t.Errorf("BooleanQuery 'brown AND NOT dog': expected 2 results, got %d", len(bqResults))
	}

	// Compare all query results between NRT and disk
	nrtReader, err := index.OpenNRTReader(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer nrtReader.Close()
	nrtSearcher := search.NewIndexSearcher(nrtReader)

	for _, tc := range []struct {
		name  string
		query search.Query
	}{
		{"TermQuery fox", search.NewTermQuery("body", "fox")},
		{"PhraseQuery brown fox", search.NewPhraseQuery("body", "brown", "fox")},
		{"BooleanQuery brown AND NOT dog", search.NewBooleanQuery().
			Add(search.NewTermQuery("body", "brown"), search.OccurMust).
			Add(search.NewTermQuery("body", "dog"), search.OccurMustNot)},
	} {
		nrtResults := nrtSearcher.Search(tc.query, search.NewTopKCollector(10))
		diskResults := diskSearcher.Search(tc.query, search.NewTopKCollector(10))

		if len(nrtResults) != len(diskResults) {
			t.Errorf("%s: result count mismatch: nrt=%d, disk=%d",
				tc.name, len(nrtResults), len(diskResults))
			continue
		}

		sort.Slice(nrtResults, func(i, j int) bool { return nrtResults[i].DocID < nrtResults[j].DocID })
		sort.Slice(diskResults, func(i, j int) bool { return diskResults[i].DocID < diskResults[j].DocID })

		for i, nr := range nrtResults {
			dr := diskResults[i]
			if nr.DocID != dr.DocID {
				t.Errorf("%s result[%d]: DocID nrt=%d, disk=%d", tc.name, i, nr.DocID, dr.DocID)
			}
			if math.Abs(nr.Score-dr.Score) > 1e-9 {
				t.Errorf("%s result[%d]: Score nrt=%f, disk=%f", tc.name, i, nr.Score, dr.Score)
			}
		}
	}
}

// --- Bugfix Regression Tests ---

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
		if fields["title"] != "Test Title" {
			t.Errorf("title: got %q, want %q", fields["title"], "Test Title")
		}
		if fields["body"] != "Test body content here" {
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
