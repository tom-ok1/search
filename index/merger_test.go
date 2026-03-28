package index

import (
	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
	"testing"
)

func createTestWriter(t *testing.T) (*IndexWriter, store.Directory) {
	t.Helper()
	dir, _ := store.NewFSDirectory(t.TempDir())
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
	))
	w := NewIndexWriter(dir, fa, 1000)
	return w, dir
}

func addDoc(t *testing.T, w *IndexWriter, fields map[string]string) {
	t.Helper()
	doc := document.NewDocument()
	for name, value := range fields {
		doc.AddField(name, value, document.FieldTypeText)
	}
	if err := w.AddDocument(doc); err != nil {
		t.Fatal(err)
	}
}

func TestMergeSegmentsTwoSegments(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "hello world", "id": "doc0"})
	addDoc(t, w, map[string]string{"body": "hello search", "id": "doc1"})
	w.Flush()

	addDoc(t, w, map[string]string{"body": "world search engine", "id": "doc2"})
	w.Flush()

	if len(w.segmentInfos.Segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(w.segmentInfos.Segments))
	}

	dirPath := dir.FilePath("")
	inputs := make([]MergeInput, 2)
	for i, info := range w.segmentInfos.Segments {
		ds, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		inputs[i] = MergeInput{
			Segment:   ds,
			IsDeleted: func(docID int) bool { return false },
		}
	}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	if result.DocCount != 3 {
		t.Errorf("merged docCount = %d, want 3", result.DocCount)
	}

	// Open merged segment and verify postings.
	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	// "hello" should have 2 docs
	if df := merged.DocFreq("body", "hello"); df != 2 {
		t.Errorf("hello doc_freq = %d, want 2", df)
	}

	// "world" should have 2 docs
	if df := merged.DocFreq("body", "world"); df != 2 {
		t.Errorf("world doc_freq = %d, want 2", df)
	}
}

func TestMergeSegmentsWithDeletions(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "hello world"})
	addDoc(t, w, map[string]string{"body": "goodbye world"})
	w.Flush()

	dirPath := dir.FilePath("")
	info := w.segmentInfos.Segments[0]
	ds, err := OpenDiskSegment(dirPath, info.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	inputs := []MergeInput{{
		Segment: ds,
		IsDeleted: func(docID int) bool {
			return docID == 1
		},
	}}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	if result.DocCount != 1 {
		t.Errorf("merged docCount = %d, want 1", result.DocCount)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	if df := merged.DocFreq("body", "goodbye"); df != 0 {
		t.Errorf("deleted doc's term 'goodbye' should not appear, got doc_freq=%d", df)
	}

	if df := merged.DocFreq("body", "hello"); df != 1 {
		t.Errorf("hello doc_freq = %d, want 1", df)
	}
}

func TestMergeSegmentsPositionsPreserved(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "hello world hello"})
	w.Flush()

	dirPath := dir.FilePath("")
	info := w.segmentInfos.Segments[0]
	ds, err := OpenDiskSegment(dirPath, info.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	inputs := []MergeInput{{
		Segment:   ds,
		IsDeleted: func(docID int) bool { return false },
	}}

	_, err = MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	pi := merged.PostingsIterator("body", "hello")
	if !pi.Next() {
		t.Fatal("expected 1 posting for 'hello'")
	}

	if pi.Freq() != 2 {
		t.Errorf("freq = %d, want 2", pi.Freq())
	}
	positions := pi.Positions()
	if len(positions) != 2 || positions[0] != 0 || positions[1] != 2 {
		t.Errorf("positions = %v, want [0, 2]", positions)
	}
}

func TestMergeSegmentsJapanese(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "東京 大阪"})
	addDoc(t, w, map[string]string{"body": "東京 名古屋"})
	w.Flush()

	addDoc(t, w, map[string]string{"body": "大阪 京都"})
	w.Flush()

	dirPath := dir.FilePath("")
	inputs := make([]MergeInput, 2)
	for i, info := range w.segmentInfos.Segments {
		ds, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		inputs[i] = MergeInput{
			Segment:   ds,
			IsDeleted: func(docID int) bool { return false },
		}
	}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	if result.DocCount != 3 {
		t.Errorf("merged docCount = %d, want 3", result.DocCount)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	if df := merged.DocFreq("body", "東京"); df != 2 {
		t.Errorf("東京 doc_freq = %d, want 2", df)
	}
	if df := merged.DocFreq("body", "大阪"); df != 2 {
		t.Errorf("大阪 doc_freq = %d, want 2", df)
	}
	if df := merged.DocFreq("body", "京都"); df != 1 {
		t.Errorf("京都 doc_freq = %d, want 1", df)
	}
}

func TestMergeSegmentsPositionsPreservedJapanese(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "東京 大阪 東京"})
	w.Flush()

	dirPath := dir.FilePath("")
	info := w.segmentInfos.Segments[0]
	ds, err := OpenDiskSegment(dirPath, info.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	inputs := []MergeInput{{
		Segment:   ds,
		IsDeleted: func(docID int) bool { return false },
	}}

	_, err = MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	pi := merged.PostingsIterator("body", "東京")
	if !pi.Next() {
		t.Fatal("expected 1 posting for '東京'")
	}
	if pi.Freq() != 2 {
		t.Errorf("freq = %d, want 2", pi.Freq())
	}
	positions := pi.Positions()
	if len(positions) != 2 || positions[0] != 0 || positions[1] != 2 {
		t.Errorf("positions = %v, want [0, 2]", positions)
	}
}

func TestMergeSegmentsSpecialChars(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "café résumé"})
	addDoc(t, w, map[string]string{"body": "user@example.com test"})
	w.Flush()

	addDoc(t, w, map[string]string{"body": "🔍 search"})
	addDoc(t, w, map[string]string{"body": "𠮷野家 テスト"})
	w.Flush()

	dirPath := dir.FilePath("")
	inputs := make([]MergeInput, 2)
	for i, info := range w.segmentInfos.Segments {
		ds, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		inputs[i] = MergeInput{
			Segment:   ds,
			IsDeleted: func(docID int) bool { return false },
		}
	}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	if result.DocCount != 4 {
		t.Errorf("merged docCount = %d, want 4", result.DocCount)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	// Verify special char terms survive merge
	for _, term := range []string{"café", "résumé", "user@example.com", "🔍", "𠮷野家", "テスト"} {
		if merged.DocFreq("body", term) != 1 {
			t.Errorf("after merge, DocFreq(%q) = %d, want 1", term, merged.DocFreq("body", term))
		}
	}
}

func TestMergeSegmentsWithDeletionsSpecialChars(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	addDoc(t, w, map[string]string{"body": "café test"})
	addDoc(t, w, map[string]string{"body": "🔍 search"})
	w.Flush()

	addDoc(t, w, map[string]string{"body": "𠮷野家 テスト"})
	w.Flush()

	dirPath := dir.FilePath("")
	inputs := make([]MergeInput, 2)
	for i, info := range w.segmentInfos.Segments {
		ds, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		localI := i
		inputs[i] = MergeInput{
			Segment: ds,
			// Delete doc1 from segment 0 (the emoji doc)
			IsDeleted: func(docID int) bool {
				return localI == 0 && docID == 1
			},
		}
	}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	if result.DocCount != 2 {
		t.Errorf("merged docCount = %d, want 2 (one deleted)", result.DocCount)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	// café and 𠮷野家 should survive
	if merged.DocFreq("body", "café") != 1 {
		t.Errorf("DocFreq(café) = %d, want 1", merged.DocFreq("body", "café"))
	}
	if merged.DocFreq("body", "𠮷野家") != 1 {
		t.Errorf("DocFreq(𠮷野家) = %d, want 1", merged.DocFreq("body", "𠮷野家"))
	}
	// 🔍 was deleted
	if merged.DocFreq("body", "🔍") != 0 {
		t.Errorf("DocFreq(🔍) = %d, want 0 (deleted)", merged.DocFreq("body", "🔍"))
	}
}

func TestMergeSegmentsDifferentFields(t *testing.T) {
	w, dir := createTestWriter(t)
	defer w.Close()

	// Segment 1 has "title" field
	doc1 := document.NewDocument()
	doc1.AddField("title", "hello world", document.FieldTypeText)
	w.AddDocument(doc1)
	w.Flush()

	// Segment 2 has "body" field
	doc2 := document.NewDocument()
	doc2.AddField("body", "search engine", document.FieldTypeText)
	w.AddDocument(doc2)
	w.Flush()

	dirPath := dir.FilePath("")
	inputs := make([]MergeInput, 2)
	for i, info := range w.segmentInfos.Segments {
		ds, err := OpenDiskSegment(dirPath, info.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		inputs[i] = MergeInput{
			Segment:   ds,
			IsDeleted: func(docID int) bool { return false },
		}
	}

	result, err := MergeSegmentsToDisk(dir, inputs, "_merged")
	if err != nil {
		t.Fatal(err)
	}

	if result.DocCount != 2 {
		t.Errorf("merged docCount = %d, want 2", result.DocCount)
	}

	merged, err := OpenDiskSegment(dirPath, "_merged")
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	// Both fields should exist
	if df := merged.DocFreq("title", "hello"); df != 1 {
		t.Errorf("title/hello doc_freq = %d, want 1", df)
	}
	if df := merged.DocFreq("body", "search"); df != 1 {
		t.Errorf("body/search doc_freq = %d, want 1", df)
	}
}
