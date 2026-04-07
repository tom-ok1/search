package index

import (
	"math"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// buildTestSegment creates an in-memory segment with test data.
// Returns the segment directly before flushing, for use in DiskSegment tests.
func buildTestSegment(t *testing.T) *InMemorySegment {
	t.Helper()
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)

	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	docs := []struct {
		title string
	}{
		{"The Quick Brown Fox"},
		{"The Lazy Dog"},
		{"Brown Fox Lazy"},
	}

	for _, d := range docs {
		doc := document.NewDocument()
		doc.AddField("title", d.title, document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	return dwpt.segment
}

// writeAndOpenDiskSegment writes a segment with V2 format and opens it as DiskSegment.
func writeAndOpenDiskSegment(t *testing.T, seg *InMemorySegment) (*DiskSegment, string) {
	t.Helper()
	tmpDir := t.TempDir()

	dir, _ := store.NewFSDirectory(tmpDir)
	if _, _, err := WriteSegmentV2(dir, seg); err != nil {
		t.Fatal(err)
	}

	ds, err := OpenDiskSegment(tmpDir, seg.name)
	if err != nil {
		t.Fatal(err)
	}
	return ds, tmpDir
}

func TestDiskSegmentBasicMetadata(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	if ds.Name() != seg.name {
		t.Errorf("Name: got %q, want %q", ds.Name(), seg.name)
	}
	if ds.DocCount() != seg.docCount {
		t.Errorf("DocCount: got %d, want %d", ds.DocCount(), seg.docCount)
	}
	// DiskSegment.LiveDocs() is always nil (deletions are handled by LiveDocsSegmentReader)
	if ds.LiveDocs() != nil {
		t.Error("LiveDocs: expected nil for DiskSegment")
	}
}

func TestDiskSegmentDocFreq(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	tests := []struct {
		field, term string
	}{
		{"title", "the"},
		{"title", "quick"},
		{"title", "brown"},
		{"title", "fox"},
		{"title", "lazy"},
		{"title", "dog"},
	}

	for _, tt := range tests {
		want := len(seg.fields[tt.field].postings[tt.term].Postings)
		got := ds.DocFreq(tt.field, tt.term)
		if got != want {
			t.Errorf("DocFreq(%q, %q): got %d, want %d", tt.field, tt.term, got, want)
		}
	}

	// Non-existent term
	if ds.DocFreq("title", "nonexistent") != 0 {
		t.Error("expected 0 for non-existent term")
	}
}

func TestDiskSegmentFieldLength(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	lengths := seg.fieldLengths["title"]
	for docID := 0; docID < seg.docCount; docID++ {
		want := lengths[docID]
		got := ds.FieldLength("title", docID)
		if got != want {
			t.Errorf("FieldLength(title, %d): got %d, want %d", docID, got, want)
		}
	}
}

func TestDiskSegmentTotalFieldLength(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	want := 0
	for _, l := range seg.fieldLengths["title"] {
		want += l
	}
	got := ds.TotalFieldLength("title")
	if got != want {
		t.Errorf("TotalFieldLength: got %d, want %d", got, want)
	}
}

func TestDiskSegmentStoredFields(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	for docID := 0; docID < seg.docCount; docID++ {
		want := seg.storedFields[docID]
		got, err := ds.StoredFields(docID)
		if err != nil {
			t.Fatalf("StoredFields(%d) error: %v", docID, err)
		}
		if string(got["title"]) != string(want["title"]) {
			t.Errorf("StoredFields(%d)[title]: got %q, want %q", docID, got["title"], want["title"])
		}
	}
}

func TestDiskSegmentPostingsIterator(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	terms := []string{"the", "quick", "brown", "fox", "lazy", "dog"}

	for _, term := range terms {
		// Expected postings from in-memory segment
		memPostings := seg.fields["title"].postings[term].Postings

		// Collect postings from disk segment
		diskIter := ds.PostingsIterator("title", term)
		var diskPostings []Posting
		for diskIter.Next() {
			// Copy positions since DiskPostingsIterator reuses the slice.
			pos := append([]int(nil), diskIter.Positions()...)
			diskPostings = append(diskPostings, Posting{
				DocID:     diskIter.DocID(),
				Freq:      diskIter.Freq(),
				Positions: pos,
			})
		}

		if len(diskPostings) != len(memPostings) {
			t.Errorf("term %q: disk postings count %d != mem postings count %d",
				term, len(diskPostings), len(memPostings))
			continue
		}

		for i, dp := range diskPostings {
			mp := memPostings[i]
			if dp.DocID != mp.DocID {
				t.Errorf("term %q posting[%d]: DocID got %d, want %d", term, i, dp.DocID, mp.DocID)
			}
			if dp.Freq != mp.Freq {
				t.Errorf("term %q posting[%d]: Freq got %d, want %d", term, i, dp.Freq, mp.Freq)
			}
			if len(dp.Positions) != len(mp.Positions) {
				t.Errorf("term %q posting[%d]: Positions length got %d, want %d",
					term, i, len(dp.Positions), len(mp.Positions))
			} else {
				for j, pos := range dp.Positions {
					if pos != mp.Positions[j] {
						t.Errorf("term %q posting[%d] pos[%d]: got %d, want %d",
							term, i, j, pos, mp.Positions[j])
					}
				}
			}
		}
	}

	// Non-existent term should return empty iterator
	iter := ds.PostingsIterator("title", "nonexistent")
	if iter.Next() {
		t.Error("expected empty iterator for non-existent term")
	}
}

func TestDiskSegmentBM25ScoresMatch(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	// Compute BM25 scores for "fox" via both raw in-memory data and DiskSegment
	term := "fox"
	field := "title"
	k1 := 1.2
	b := 0.75

	// Expected scores from in-memory segment's raw fields
	fi := seg.fields[field]
	pl := fi.postings[term]
	docFreq := len(pl.Postings)
	totalFieldLen := 0
	for _, l := range seg.fieldLengths[field] {
		totalFieldLen += l
	}
	avgDocLen := float64(totalFieldLen) / float64(seg.docCount)
	idf := math.Log(1 + (float64(seg.docCount)-float64(docFreq)+0.5)/(float64(docFreq)+0.5))

	memScores := make(map[int]float64)
	for _, p := range pl.Postings {
		tf := float64(p.Freq)
		docLen := float64(seg.fieldLengths[field][p.DocID])
		tfNorm := (tf * (k1 + 1)) / (tf + k1*(1-b+b*docLen/avgDocLen))
		memScores[p.DocID] = idf * tfNorm
	}

	// Scores from DiskSegment via SegmentReader interface
	computeScores := func(sr SegmentReader) map[int]float64 {
		scores := make(map[int]float64)
		docCount := sr.DocCount()
		df := sr.DocFreq(field, term)
		if df == 0 {
			return scores
		}

		tfl := sr.TotalFieldLength(field)
		adl := float64(tfl) / float64(docCount)
		idf := math.Log(1 + (float64(docCount)-float64(df)+0.5)/(float64(df)+0.5))

		iter := sr.PostingsIterator(field, term)
		for iter.Next() {
			tf := float64(iter.Freq())
			docLen := float64(sr.FieldLength(field, iter.DocID()))
			tfNorm := (tf * (k1 + 1)) / (tf + k1*(1-b+b*docLen/adl))
			scores[iter.DocID()] = idf * tfNorm
		}
		return scores
	}

	diskScores := computeScores(ds)

	if len(memScores) != len(diskScores) {
		t.Fatalf("score count mismatch: mem=%d, disk=%d", len(memScores), len(diskScores))
	}

	for docID, memScore := range memScores {
		diskScore, ok := diskScores[docID]
		if !ok {
			t.Errorf("doc %d: missing from disk scores", docID)
			continue
		}
		if math.Abs(memScore-diskScore) > 1e-9 {
			t.Errorf("doc %d: score mismatch: mem=%f, disk=%f", docID, memScore, diskScore)
		}
	}
}

func TestDiskSegmentNonExistentField(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	if ds.DocFreq("nonexistent", "the") != 0 {
		t.Error("expected 0 DocFreq for non-existent field")
	}
	if ds.FieldLength("nonexistent", 0) != 0 {
		t.Error("expected 0 FieldLength for non-existent field")
	}
	if ds.TotalFieldLength("nonexistent") != 0 {
		t.Error("expected 0 TotalFieldLength for non-existent field")
	}
	iter := ds.PostingsIterator("nonexistent", "the")
	if iter.Next() {
		t.Error("expected empty iterator for non-existent field")
	}
}

func TestDiskSegmentStoredFieldsOutOfRange(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	fields, err := ds.StoredFields(100)
	if err != nil {
		t.Errorf("StoredFields out of range should not error, got: %v", err)
	}
	if fields != nil {
		t.Error("expected nil for out-of-range docID")
	}
}

func TestDiskSegmentLiveDocsAlwaysNil(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	// DiskSegment is a pure data reader — LiveDocs() is always nil
	if ds.LiveDocs() != nil {
		t.Error("DiskSegment.LiveDocs() should always be nil")
	}
}

func TestDiskSegmentMultipleFields(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	writer := NewIndexWriter(dir, fa, 100)

	doc := document.NewDocument()
	doc.AddField("title", "Quick Fox", document.FieldTypeText)
	doc.AddField("body", "The quick brown fox jumps", document.FieldTypeText)
	writer.AddDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("title", "Lazy Dog", document.FieldTypeText)
	doc2.AddField("body", "The lazy brown dog sleeps", document.FieldTypeText)
	writer.AddDocument(doc2)

	writer.Flush()

	ds, err := OpenDiskSegment(tmpDir, writer.segmentInfos.Segments[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Title field
	if ds.DocFreq("title", "quick") != 1 {
		t.Errorf("title DocFreq 'quick': got %d, want 1", ds.DocFreq("title", "quick"))
	}
	// Body field
	if ds.DocFreq("body", "jumps") != 1 {
		t.Errorf("body DocFreq 'jumps': got %d, want 1", ds.DocFreq("body", "jumps"))
	}
	// Cross-field: "jumps" not in title
	if ds.DocFreq("title", "jumps") != 0 {
		t.Error("'jumps' should not appear in title")
	}

	// Field lengths for different fields
	if ds.FieldLength("title", 0) != 2 {
		t.Errorf("title FieldLength doc0: got %d, want 2", ds.FieldLength("title", 0))
	}
	if ds.FieldLength("body", 0) != 5 {
		t.Errorf("body FieldLength doc0: got %d, want 5", ds.FieldLength("body", 0))
	}
}

func TestDiskSegmentJapanese(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	docs := []string{"東京 大阪", "東京 名古屋", "大阪 京都"}
	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("title", text, document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	// "東京" appears in doc 0 and 1
	if ds.DocFreq("title", "東京") != 2 {
		t.Errorf("DocFreq(東京): got %d, want 2", ds.DocFreq("title", "東京"))
	}
	// "大阪" appears in doc 0 and 2
	if ds.DocFreq("title", "大阪") != 2 {
		t.Errorf("DocFreq(大阪): got %d, want 2", ds.DocFreq("title", "大阪"))
	}
	// "名古屋" appears in doc 1 only
	if ds.DocFreq("title", "名古屋") != 1 {
		t.Errorf("DocFreq(名古屋): got %d, want 1", ds.DocFreq("title", "名古屋"))
	}

	// Verify postings iterator for "東京"
	iter := ds.PostingsIterator("title", "東京")
	var docIDs []int
	for iter.Next() {
		docIDs = append(docIDs, iter.DocID())
	}
	if len(docIDs) != 2 || docIDs[0] != 0 || docIDs[1] != 1 {
		t.Errorf("PostingsIterator(東京): got docIDs %v, want [0,1]", docIDs)
	}

	// Verify stored fields roundtrip
	for i, text := range docs {
		stored, err := ds.StoredFields(i)
		if err != nil {
			t.Fatalf("StoredFields(%d): %v", i, err)
		}
		if string(stored["title"]) != text {
			t.Errorf("StoredFields(%d)[title]: got %q, want %q", i, stored["title"], text)
		}
	}

	// Field lengths: each doc has 2 tokens
	for i := range docs {
		if ds.FieldLength("title", i) != 2 {
			t.Errorf("FieldLength(title, %d): got %d, want 2", i, ds.FieldLength("title", i))
		}
	}
}

func TestDiskSegmentSpecialChars(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	docs := []string{
		"user@example.com #tag",
		"state-of-the-art node.js",
		"café résumé naïve",
	}
	for _, text := range docs {
		doc := document.NewDocument()
		doc.AddField("title", text, document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	// Verify DocFreq for special char terms
	if ds.DocFreq("title", "user@example.com") != 1 {
		t.Errorf("DocFreq(user@example.com): got %d, want 1", ds.DocFreq("title", "user@example.com"))
	}
	if ds.DocFreq("title", "state-of-the-art") != 1 {
		t.Errorf("DocFreq(state-of-the-art): got %d, want 1", ds.DocFreq("title", "state-of-the-art"))
	}
	if ds.DocFreq("title", "café") != 1 {
		t.Errorf("DocFreq(café): got %d, want 1", ds.DocFreq("title", "café"))
	}

	// Verify stored fields roundtrip with special chars
	for i, text := range docs {
		stored, err := ds.StoredFields(i)
		if err != nil {
			t.Fatalf("StoredFields(%d): %v", i, err)
		}
		if string(stored["title"]) != text {
			t.Errorf("StoredFields(%d)[title]: got %q, want %q", i, stored["title"], text)
		}
	}
}

func TestDiskSegmentEmoji(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("title", "hello 🔍 world", document.FieldTypeText)
	dwpt.addDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("title", "🔍 search 🔎", document.FieldTypeText)
	dwpt.addDocument(doc2)

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	if ds.DocFreq("title", "🔍") != 2 {
		t.Errorf("DocFreq(🔍): got %d, want 2", ds.DocFreq("title", "🔍"))
	}

	// Stored field roundtrip
	stored, err := ds.StoredFields(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored["title"]) != "🔍 search 🔎" {
		t.Errorf("stored: got %q, want %q", stored["title"], "🔍 search 🔎")
	}
}

func TestDiskSegmentCJKExtensionB(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("title", "𠮷野家 テスト", document.FieldTypeText)
	dwpt.addDocument(doc)

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	if ds.DocFreq("title", "𠮷野家") != 1 {
		t.Errorf("DocFreq(𠮷野家): got %d, want 1", ds.DocFreq("title", "𠮷野家"))
	}

	stored, err := ds.StoredFields(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored["title"]) != "𠮷野家 テスト" {
		t.Errorf("stored: got %q, want %q", stored["title"], "𠮷野家 テスト")
	}
}

func TestDiskSegmentKeywordWithSpaces(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	doc := document.NewDocument()
	doc.AddField("city", "New York", document.FieldTypeKeyword)
	doc.AddField("body", "test", document.FieldTypeText)
	dwpt.addDocument(doc)

	doc2 := document.NewDocument()
	doc2.AddField("city", "C++", document.FieldTypeKeyword)
	doc2.AddField("body", "test", document.FieldTypeText)
	dwpt.addDocument(doc2)

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	if ds.DocFreq("city", "New York") != 1 {
		t.Errorf("DocFreq(New York): got %d, want 1", ds.DocFreq("city", "New York"))
	}
	if ds.DocFreq("city", "C++") != 1 {
		t.Errorf("DocFreq(C++): got %d, want 1", ds.DocFreq("city", "C++"))
	}
	// Partial should not match
	if ds.DocFreq("city", "New") != 0 {
		t.Error("partial keyword 'New' should not match")
	}
}

func TestDiskSegmentStoredFieldSpecialCharsRoundtrip(t *testing.T) {
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
	dwpt := newDWPT("_seg0", fa, newDeleteQueue())

	values := []string{
		"hello\tworld\nnewline",
		"path\\to\\file",
		"{\"key\": \"value\"}",
		"",
		"🔍🔎",
		"𠮷野家",
	}
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddField("data", val, document.FieldTypeStored)
		doc.AddField("body", "searchable", document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	ds, _ := writeAndOpenDiskSegment(t, dwpt.segment)
	defer ds.Close()

	for i, val := range values {
		stored, err := ds.StoredFields(i)
		if err != nil {
			t.Fatalf("StoredFields(%d): %v", i, err)
		}
		if string(stored["data"]) != val {
			t.Errorf("doc %d: stored = %q, want %q", i, stored["data"], val)
		}
	}
}

func TestOpenDiskSegmentNonExistentPath(t *testing.T) {
	_, err := OpenDiskSegment("/nonexistent/path", "_seg0")
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}
