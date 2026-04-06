package index

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func TestNumericDocValuesRoundTrip(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	// Add documents with numeric doc values
	for _, price := range []int64{100, 250, 50, 300, 75} {
		doc := document.NewDocument()
		doc.AddField("title", "product", document.FieldTypeText)
		doc.AddNumericDocValuesField("price", price)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Open reader and verify
	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	leaves := reader.Leaves()
	if len(leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(leaves))
	}

	seg := leaves[0].Segment
	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues for price")
	}

	expected := []int64{100, 250, 50, 300, 75}
	for i, want := range expected {
		got, err := ndv.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got != want {
			t.Errorf("Get(%d) = %d, want %d", i, got, want)
		}
	}

	// Field without DV should return nil
	if seg.NumericDocValues("nonexistent") != nil {
		t.Error("expected nil for nonexistent field")
	}

	writer.Close()
}

func TestSortedDocValuesRoundTrip(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	categories := []string{"electronics", "books", "electronics", "clothing", "books"}
	for _, cat := range categories {
		doc := document.NewDocument()
		doc.AddField("title", "item", document.FieldTypeText)
		doc.AddSortedDocValuesField("category", cat)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("category")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues for category")
	}

	// Should have 3 unique values: books, clothing, electronics (sorted)
	if sdv.ValueCount() != 3 {
		t.Fatalf("ValueCount() = %d, want 3", sdv.ValueCount())
	}

	// Verify ordinal-to-value mapping (sorted order)
	expectedDict := []string{"books", "clothing", "electronics"}
	for i, want := range expectedDict {
		got, err := sdv.LookupOrd(i)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", i, err)
		}
		if string(got) != want {
			t.Errorf("LookupOrd(%d) = %q, want %q", i, got, want)
		}
	}

	// Verify per-document ordinals
	expectedOrds := []int{2, 0, 2, 1, 0} // electronics=2, books=0, clothing=1
	for i, wantOrd := range expectedOrds {
		gotOrd, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		if gotOrd != wantOrd {
			t.Errorf("OrdValue(%d) = %d, want %d", i, gotOrd, wantOrd)
		}
	}

	writer.Close()
}

func TestDocValuesMerge(t *testing.T) {
	dir := createTempDir(t)

	// Use buffer size 2 to force multiple segments
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	// Add 4 documents across 2 segments
	prices := []int64{100, 200, 300, 400}
	cats := []string{"a", "b", "c", "a"}
	for i := range prices {
		doc := document.NewDocument()
		doc.AddField("title", "item", document.FieldTypeText)
		doc.AddNumericDocValuesField("price", prices[i])
		doc.AddSortedDocValuesField("cat", cats[i])
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Should have 2 segments now
	reader1, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	if len(reader1.Leaves()) != 2 {
		t.Fatalf("expected 2 leaves before merge, got %d", len(reader1.Leaves()))
	}

	// Force merge into 1 segment
	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader2, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader after merge: %v", err)
	}
	if len(reader2.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf after merge, got %d", len(reader2.Leaves()))
	}

	seg := reader2.Leaves()[0].Segment

	// Verify numeric DV survived merge
	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues after merge")
	}
	for i, want := range prices {
		got, err := ndv.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got != want {
			t.Errorf("numeric DV Get(%d) = %d, want %d", i, got, want)
		}
	}

	// Verify sorted DV survived merge
	sdv := seg.SortedDocValues("cat")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues after merge")
	}
	for i, wantCat := range cats {
		ord, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		val, err := sdv.LookupOrd(ord)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", ord, err)
		}
		if string(val) != wantCat {
			t.Errorf("sorted DV doc %d = %q, want %q", i, val, wantCat)
		}
	}

	writer.Close()
}

func TestDocValuesMergeWithDeletions(t *testing.T) {
	dir := createTempDir(t)

	// Buffer size 2 to force 2 segments
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	// Add 4 docs: doc0(id=a,price=100), doc1(id=b,price=200), doc2(id=c,price=300), doc3(id=d,price=400)
	ids := []string{"a", "b", "c", "d"}
	prices := []int64{100, 200, 300, 400}
	cats := []string{"x", "y", "x", "z"}
	for i := range ids {
		doc := document.NewDocument()
		doc.AddField("id", ids[i], document.FieldTypeKeyword)
		doc.AddField("body", "item", document.FieldTypeText)
		doc.AddNumericDocValuesField("price", prices[i])
		doc.AddSortedDocValuesField("cat", cats[i])
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Delete doc1 (id=b, price=200) and doc2 (id=c, price=300)
	if err := writer.DeleteDocuments("id", "b"); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if err := writer.DeleteDocuments("id", "c"); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit deletes: %v", err)
	}

	// Force merge
	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	// Only 2 docs should survive: doc0 (price=100, cat=x) and doc3 (price=400, cat=z)
	if seg.DocCount() != 2 {
		t.Fatalf("expected 2 docs after merge with deletions, got %d", seg.DocCount())
	}

	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues after merge")
	}

	// Surviving docs remapped to 0,1: prices should be 100, 400
	expectedPrices := []int64{100, 400}
	for i, want := range expectedPrices {
		got, err := ndv.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got != want {
			t.Errorf("numeric DV Get(%d) = %d, want %d", i, got, want)
		}
	}

	sdv := seg.SortedDocValues("cat")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues after merge")
	}

	expectedCats := []string{"x", "z"}
	for i, wantCat := range expectedCats {
		ord, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		val, err := sdv.LookupOrd(ord)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", ord, err)
		}
		if string(val) != wantCat {
			t.Errorf("sorted DV doc %d = %q, want %q", i, val, wantCat)
		}
	}

	writer.Close()
}

func TestSortedDocValuesWithMissingValues(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	// Doc0 has category, doc1 does not, doc2 has category
	doc0 := document.NewDocument()
	doc0.AddField("body", "hello", document.FieldTypeText)
	doc0.AddSortedDocValuesField("category", "alpha")
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("body", "world", document.FieldTypeText)
	// No sorted DV field
	writer.AddDocument(doc1)

	doc2 := document.NewDocument()
	doc2.AddField("body", "test", document.FieldTypeText)
	doc2.AddSortedDocValuesField("category", "beta")
	writer.AddDocument(doc2)

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("category")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues")
	}

	// Doc0 should have a valid ordinal
	ord0, err := sdv.OrdValue(0)
	if err != nil {
		t.Fatalf("OrdValue(0): %v", err)
	}
	if ord0 < 0 {
		t.Errorf("expected valid ordinal for doc0, got %d", ord0)
	}
	val0, err := sdv.LookupOrd(ord0)
	if err != nil {
		t.Fatalf("LookupOrd(%d): %v", ord0, err)
	}
	if string(val0) != "alpha" {
		t.Errorf("doc0 category = %q, want %q", val0, "alpha")
	}

	// Doc1 should have ordinal -1 (missing)
	ord1, err := sdv.OrdValue(1)
	if err != nil {
		t.Fatalf("OrdValue(1): %v", err)
	}
	if ord1 != -1 {
		t.Errorf("expected ordinal -1 for missing doc, got %d", ord1)
	}

	// Doc2 should have a valid ordinal
	ord2, err := sdv.OrdValue(2)
	if err != nil {
		t.Fatalf("OrdValue(2): %v", err)
	}
	val2, err := sdv.LookupOrd(ord2)
	if err != nil {
		t.Fatalf("LookupOrd(%d): %v", ord2, err)
	}
	if string(val2) != "beta" {
		t.Errorf("doc2 category = %q, want %q", val2, "beta")
	}

	writer.Close()
}

func TestNumericDocValuesZeroValue(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	// Explicitly add a numeric DV field with value 0
	doc := document.NewDocument()
	doc.AddField("body", "item", document.FieldTypeText)
	doc.AddNumericDocValuesField("price", 0)
	writer.AddDocument(doc)

	// Add a doc with non-zero value for comparison
	doc2 := document.NewDocument()
	doc2.AddField("body", "item2", document.FieldTypeText)
	doc2.AddNumericDocValuesField("price", 42)
	writer.AddDocument(doc2)

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	seg := reader.Leaves()[0].Segment
	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues")
	}

	// Doc0 has explicit value 0
	v0, err := ndv.Get(0)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if v0 != 0 {
		t.Errorf("Get(0) = %d, want 0", v0)
	}

	// Doc1 has value 42
	v1, err := ndv.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 != 42 {
		t.Errorf("Get(1) = %d, want 42", v1)
	}

	writer.Close()
}

func TestSortedDocValuesMerge(t *testing.T) {
	dir := createTempDir(t)

	// Buffer size 2 to force multiple segments.
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	// 6 docs across 3 segments, with overlapping and unique values.
	cats := []string{"cherry", "apple", "banana", "apple", "date", "cherry"}
	for _, cat := range cats {
		doc := document.NewDocument()
		doc.AddField("body", "item", document.FieldTypeText)
		doc.AddSortedDocValuesField("cat", cat)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Force merge into 1 segment.
	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf after merge, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("cat")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues after merge")
	}

	// Verify each doc maps to the correct category.
	for i, wantCat := range cats {
		ord, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		val, err := sdv.LookupOrd(ord)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", ord, err)
		}
		if string(val) != wantCat {
			t.Errorf("doc %d = %q, want %q", i, val, wantCat)
		}
	}

	// Verify dictionary is sorted and deduplicated.
	expectedDict := []string{"apple", "banana", "cherry", "date"}
	if sdv.ValueCount() != len(expectedDict) {
		t.Fatalf("ValueCount() = %d, want %d", sdv.ValueCount(), len(expectedDict))
	}
	for i, want := range expectedDict {
		got, err := sdv.LookupOrd(i)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", i, err)
		}
		if string(got) != want {
			t.Errorf("LookupOrd(%d) = %q, want %q", i, got, want)
		}
	}

	writer.Close()
}

func TestSortedDocValuesSpecialChars(t *testing.T) {
	dir := createTempDir(t)
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	// Special character values for sorted doc values
	values := []string{
		"café",
		"path\\to\\file",
		"🔍emoji",
		"𠮷野家",
		"café", // duplicate
		"C++",
	}
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddField("body", "item", document.FieldTypeText)
		doc.AddSortedDocValuesField("tag", val)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("tag")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues")
	}

	// Verify each doc maps to its correct value
	for i, wantVal := range values {
		ord, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		got, err := sdv.LookupOrd(ord)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", ord, err)
		}
		if string(got) != wantVal {
			t.Errorf("doc %d: got %q, want %q", i, got, wantVal)
		}
	}

	// Duplicate values should share the same ordinal
	ord0, _ := sdv.OrdValue(0) // "café"
	ord4, _ := sdv.OrdValue(4) // "café" again
	if ord0 != ord4 {
		t.Errorf("duplicate values should share ordinal: doc0=%d, doc4=%d", ord0, ord4)
	}

	writer.Close()
}

func TestSortedDocValuesEmptyStringIsMissing(t *testing.T) {
	dir := createTempDir(t)
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	doc := document.NewDocument()
	doc.AddField("body", "item", document.FieldTypeText)
	doc.AddSortedDocValuesField("tag", "")
	if err := writer.AddDocument(doc); err != nil {
		t.Fatalf("AddDocument: %v", err)
	}

	doc2 := document.NewDocument()
	doc2.AddField("body", "item2", document.FieldTypeText)
	doc2.AddSortedDocValuesField("tag", "nonempty")
	if err := writer.AddDocument(doc2); err != nil {
		t.Fatalf("AddDocument: %v", err)
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("tag")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues")
	}

	// Empty string sorted doc values are treated as missing (ordinal -1)
	ord, err := sdv.OrdValue(0)
	if err != nil {
		t.Fatalf("OrdValue(0): %v", err)
	}
	if ord != -1 {
		t.Errorf("expected ordinal -1 for empty string (treated as missing), got %d", ord)
	}

	// Doc1 has non-empty value
	ord1, err := sdv.OrdValue(1)
	if err != nil {
		t.Fatalf("OrdValue(1): %v", err)
	}
	if ord1 < 0 {
		t.Errorf("expected valid ordinal for 'nonempty', got %d", ord1)
	}

	writer.Close()
}

func TestSortedDocValuesMergeSpecialChars(t *testing.T) {
	dir := createTempDir(t)
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	values := []string{"🔍emoji", "café", "𠮷野家", "café"} // 2 segments: [🔍emoji, café], [𠮷野家, café]
	for _, val := range values {
		doc := document.NewDocument()
		doc.AddField("body", "item", document.FieldTypeText)
		doc.AddSortedDocValuesField("tag", val)
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("AddDocument: %v", err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf after merge, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	sdv := seg.SortedDocValues("tag")
	if sdv == nil {
		t.Fatal("expected non-nil SortedDocValues after merge")
	}

	for i, wantVal := range values {
		ord, err := sdv.OrdValue(i)
		if err != nil {
			t.Fatalf("OrdValue(%d): %v", i, err)
		}
		got, err := sdv.LookupOrd(ord)
		if err != nil {
			t.Fatalf("LookupOrd(%d): %v", ord, err)
		}
		if string(got) != wantVal {
			t.Errorf("doc %d: got %q, want %q", i, got, wantVal)
		}
	}

	writer.Close()
}

func TestNumericDocValuesSparseRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	// 5 docs, only docs 0 and 2 have values
	values := []int64{100, 0, 300, 0, 0}
	presence := map[int]struct{}{0: {}, 2: {}}
	docCount := 5

	if err := writeNumericDocValues(dir, "seg0", "price", values, docCount, presence); err != nil {
		t.Fatalf("writeNumericDocValues: %v", err)
	}

	data, err := store.OpenMMap(tmpDir + "/seg0.price.ndv")
	if err != nil {
		t.Fatalf("OpenMMap: %v", err)
	}
	defer data.Close()

	dv, err := readNumericDocValues(data)
	if err != nil {
		t.Fatalf("readNumericDocValues: %v", err)
	}

	// Doc 0 has value 100
	if !dv.HasValue(0) {
		t.Error("expected HasValue(0) = true")
	}
	v, err := dv.Get(0)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if v != 100 {
		t.Errorf("Get(0) = %d, want 100", v)
	}

	// Doc 1 has no value
	if dv.HasValue(1) {
		t.Error("expected HasValue(1) = false")
	}

	// Doc 2 has value 300
	if !dv.HasValue(2) {
		t.Error("expected HasValue(2) = true")
	}
	v, err = dv.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v != 300 {
		t.Errorf("Get(2) = %d, want 300", v)
	}

	// Docs 3 and 4 have no value
	if dv.HasValue(3) {
		t.Error("expected HasValue(3) = false")
	}
	if dv.HasValue(4) {
		t.Error("expected HasValue(4) = false")
	}
}

func TestNumericDocValuesDenseRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	values := []int64{10, 20, 30}
	docCount := 3

	// presence=nil means dense
	if err := writeNumericDocValues(dir, "seg0", "price", values, docCount, nil); err != nil {
		t.Fatalf("writeNumericDocValues: %v", err)
	}

	data, err := store.OpenMMap(tmpDir + "/seg0.price.ndv")
	if err != nil {
		t.Fatalf("OpenMMap: %v", err)
	}
	defer data.Close()

	dv, err := readNumericDocValues(data)
	if err != nil {
		t.Fatalf("readNumericDocValues: %v", err)
	}

	for i, want := range values {
		if !dv.HasValue(i) {
			t.Errorf("expected HasValue(%d) = true", i)
		}
		got, err := dv.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got != want {
			t.Errorf("Get(%d) = %d, want %d", i, got, want)
		}
	}
}

func TestNumericDocValuesEmptyRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}

	values := []int64{0, 0, 0}
	presence := map[int]struct{}{} // empty presence = no docs have values
	docCount := 3

	if err := writeNumericDocValues(dir, "seg0", "price", values, docCount, presence); err != nil {
		t.Fatalf("writeNumericDocValues: %v", err)
	}

	data, err := store.OpenMMap(tmpDir + "/seg0.price.ndv")
	if err != nil {
		t.Fatalf("OpenMMap: %v", err)
	}
	defer data.Close()

	dv, err := readNumericDocValues(data)
	if err != nil {
		t.Fatalf("readNumericDocValues: %v", err)
	}

	for i := range docCount {
		if dv.HasValue(i) {
			t.Errorf("expected HasValue(%d) = false", i)
		}
	}
}

func TestNumericDocValuesPointFieldSparse(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 100)

	// Doc 0: has price (point field)
	doc0 := document.NewDocument()
	doc0.AddLongPoint("price", 100)
	writer.AddDocument(doc0)

	// Doc 1: no price
	doc1 := document.NewDocument()
	doc1.AddField("name", "test", document.FieldTypeKeyword)
	writer.AddDocument(doc1)

	// Doc 2: has price
	doc2 := document.NewDocument()
	doc2.AddLongPoint("price", 200)
	writer.AddDocument(doc2)

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	defer reader.Close()

	seg := reader.Leaves()[0].Segment
	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues for price")
	}

	// Doc 0 has value 100
	v0, err := ndv.Get(0)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if v0 != 100 {
		t.Errorf("Get(0) = %d, want 100", v0)
	}
	if !ndv.HasValue(0) {
		t.Error("expected HasValue(0) = true")
	}

	// Doc 1 has no value
	if ndv.HasValue(1) {
		t.Error("expected HasValue(1) = false")
	}

	// Doc 2 has value 200
	v2, err := ndv.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 != 200 {
		t.Errorf("Get(2) = %d, want 200", v2)
	}
	if !ndv.HasValue(2) {
		t.Error("expected HasValue(2) = true")
	}

	writer.Close()
}

func TestNumericDocValuesPointFieldMerge(t *testing.T) {
	dir := createTempDir(t)

	// Buffer size 2 to force multiple segments.
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	// Seg1: doc0 has price, doc1 does not
	doc0 := document.NewDocument()
	doc0.AddLongPoint("price", 100)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("name", "noprice", document.FieldTypeKeyword)
	writer.AddDocument(doc1)

	// Seg2: doc2 has price, doc3 has price
	doc2 := document.NewDocument()
	doc2.AddLongPoint("price", 200)
	writer.AddDocument(doc2)

	doc3 := document.NewDocument()
	doc3.AddLongPoint("price", 300)
	writer.AddDocument(doc3)

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Force merge.
	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	defer reader.Close()

	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	ndv := seg.NumericDocValues("price")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues after merge")
	}

	// After merge: doc0=100, doc1=no value, doc2=200, doc3=300
	if !ndv.HasValue(0) {
		t.Error("expected HasValue(0) = true")
	}
	v0, _ := ndv.Get(0)
	if v0 != 100 {
		t.Errorf("Get(0) = %d, want 100", v0)
	}

	if ndv.HasValue(1) {
		t.Error("expected HasValue(1) = false")
	}

	if !ndv.HasValue(2) {
		t.Error("expected HasValue(2) = true")
	}
	v2, _ := ndv.Get(2)
	if v2 != 200 {
		t.Errorf("Get(2) = %d, want 200", v2)
	}

	if !ndv.HasValue(3) {
		t.Error("expected HasValue(3) = true")
	}
	v3, _ := ndv.Get(3)
	if v3 != 300 {
		t.Errorf("Get(3) = %d, want 300", v3)
	}

	writer.Close()
}

// TestNumericDocValuesSparseNonPointFieldMerge verifies that a non-point numeric
// doc values field that only some documents have is written as sparse after merge.
// Currently the merger uses isPoint to decide dense vs sparse, which means
// non-point fields are always written as dense — causing HasValue to return true
// for documents that never had a value.
func TestNumericDocValuesSparseNonPointFieldMerge(t *testing.T) {
	dir := createTempDir(t)

	// Buffer size 2 to force multiple segments.
	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 2)

	// Seg1: doc0 has "score", doc1 does not.
	doc0 := document.NewDocument()
	doc0.AddField("title", "first", document.FieldTypeText)
	doc0.AddNumericDocValuesField("score", 10)
	writer.AddDocument(doc0)

	doc1 := document.NewDocument()
	doc1.AddField("title", "second", document.FieldTypeText)
	// no "score" field
	writer.AddDocument(doc1)

	// Seg2: doc2 does not have "score", doc3 has "score".
	doc2 := document.NewDocument()
	doc2.AddField("title", "third", document.FieldTypeText)
	// no "score" field
	writer.AddDocument(doc2)

	doc3 := document.NewDocument()
	doc3.AddField("title", "fourth", document.FieldTypeText)
	doc3.AddNumericDocValuesField("score", 40)
	writer.AddDocument(doc3)

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Force merge into 1 segment.
	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	defer reader.Close()

	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	ndv := seg.NumericDocValues("score")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues after merge")
	}

	// doc0 has score=10
	if !ndv.HasValue(0) {
		t.Error("expected HasValue(0) = true")
	}
	v0, _ := ndv.Get(0)
	if v0 != 10 {
		t.Errorf("Get(0) = %d, want 10", v0)
	}

	// doc1 does NOT have score
	if ndv.HasValue(1) {
		t.Error("expected HasValue(1) = false (doc1 has no score field)")
	}

	// doc2 does NOT have score
	if ndv.HasValue(2) {
		t.Error("expected HasValue(2) = false (doc2 has no score field)")
	}

	// doc3 has score=40
	if !ndv.HasValue(3) {
		t.Error("expected HasValue(3) = true")
	}
	v3, _ := ndv.Get(3)
	if v3 != 40 {
		t.Errorf("Get(3) = %d, want 40", v3)
	}

	writer.Close()
}

func TestPointFieldMerge_LargeWithDeletions(t *testing.T) {
	dir := createTempDir(t)

	writer := NewIndexWriter(dir, analysis.NewFieldAnalyzers(analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{})), 500)

	for i := range 2000 {
		doc := document.NewDocument()
		doc.AddLongPoint("score", int64(i))
		doc.AddField("id", fmt.Sprintf("%d", i), document.FieldTypeKeyword)
		writer.AddDocument(doc)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for i := 1; i < 2000; i += 2 {
		writer.DeleteDocuments("id", fmt.Sprintf("%d", i))
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after delete: %v", err)
	}

	if err := writer.ForceMerge(1); err != nil {
		t.Fatalf("ForceMerge: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit after merge: %v", err)
	}

	reader, err := OpenDirectoryReader(dir)
	if err != nil {
		t.Fatalf("OpenDirectoryReader: %v", err)
	}
	defer reader.Close()

	if len(reader.Leaves()) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(reader.Leaves()))
	}

	seg := reader.Leaves()[0].Segment
	ndv := seg.NumericDocValues("score")
	if ndv == nil {
		t.Fatal("expected non-nil NumericDocValues after merge")
	}

	count := 0
	for d := 0; d < seg.DocCount(); d++ {
		if ndv.HasValue(d) {
			count++
		}
	}
	if count != 1000 {
		t.Fatalf("docs with score = %d, want 1000", count)
	}
}

func createTempDir(t *testing.T) store.Directory {
	t.Helper()
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("NewFSDirectory: %v", err)
	}
	return dir
}
