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
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)

	dwpt := newDWPT("_seg0", analyzer)

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
	wantLive := seg.docCount - len(seg.deletedDocs)
	if ds.LiveDocCount() != wantLive {
		t.Errorf("LiveDocCount: got %d, want %d", ds.LiveDocCount(), wantLive)
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
		if got["title"] != want["title"] {
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
			diskPostings = append(diskPostings, Posting{
				DocID:     diskIter.DocID(),
				Freq:      diskIter.Freq(),
				Positions: diskIter.Positions(),
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

func TestDiskSegmentIsDeletedWithoutDelFile(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	// No .del file, so all docs should be alive
	for i := range ds.DocCount() {
		if ds.IsDeleted(i) {
			t.Errorf("doc %d should not be deleted (no .del file)", i)
		}
	}
}

func TestDiskSegmentLiveDocCountNoDeletions(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	if ds.LiveDocCount() != ds.DocCount() {
		t.Errorf("LiveDocCount without deletions: got %d, want %d", ds.LiveDocCount(), ds.DocCount())
	}
}

func TestDiskSegmentMultipleFields(t *testing.T) {
	tmpDir := t.TempDir()
	dir, _ := store.NewFSDirectory(tmpDir)
	analyzer := analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
	writer := NewIndexWriter(dir, analyzer, 100)

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

func TestOpenDiskSegmentNonExistentPath(t *testing.T) {
	_, err := OpenDiskSegment("/nonexistent/path", "_seg0")
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}
