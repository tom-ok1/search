package index

import (
	"testing"

	"gosearch/store"
)

func createSkipper(t *testing.T, docIDs []int, values []int64) *DocValuesSkipper {
	t.Helper()
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := writeDocValuesSkipIndex(dir, "seg0", "f", "ndvs", docIDs, values); err != nil {
		t.Fatal(err)
	}

	path := dir.FilePath("seg0.f.ndvs")
	data, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })

	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		t.Fatal(err)
	}
	return skipper
}

// denseDocIDs returns [0, 1, 2, ..., n-1].
func denseDocIDs(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

func TestSkipIndexRoundtrip(t *testing.T) {
	values := make([]int64, 300)
	for i := range values {
		values[i] = int64(i * 10)
	}
	skipper := createSkipper(t, denseDocIDs(300), values)

	if skipper.NumLevels() < 1 {
		t.Fatalf("expected at least 1 level, got %d", skipper.NumLevels())
	}
	if skipper.GlobalMin() != 0 {
		t.Errorf("globalMin = %d, want 0", skipper.GlobalMin())
	}
	if skipper.GlobalMax() != 2990 {
		t.Errorf("globalMax = %d, want 2990", skipper.GlobalMax())
	}
	if skipper.DocCount() != 300 {
		t.Errorf("docCount = %d, want 300", skipper.DocCount())
	}

	// 300 docs / 128 block = 3 blocks at level 0
	if skipper.BlockCount(0) != 3 {
		t.Errorf("level 0 blockCount = %d, want 3", skipper.BlockCount(0))
	}
}

func TestSkipIndexSingleDoc(t *testing.T) {
	skipper := createSkipper(t, []int{0}, []int64{42})

	if skipper.GlobalMin() != 42 || skipper.GlobalMax() != 42 {
		t.Errorf("global range = [%d, %d], want [42, 42]", skipper.GlobalMin(), skipper.GlobalMax())
	}
	if skipper.BlockCount(0) != 1 {
		t.Errorf("level 0 blockCount = %d, want 1", skipper.BlockCount(0))
	}
}

func TestSkipIndexAllSameValues(t *testing.T) {
	values := make([]int64, 200)
	for i := range values {
		values[i] = 7
	}
	skipper := createSkipper(t, denseDocIDs(200), values)

	if skipper.GlobalMin() != 7 || skipper.GlobalMax() != 7 {
		t.Errorf("global range = [%d, %d], want [7, 7]", skipper.GlobalMin(), skipper.GlobalMax())
	}
}

func TestSkipIndexEmpty(t *testing.T) {
	skipper := createSkipper(t, nil, nil)

	if skipper.NumLevels() != 0 {
		t.Errorf("numLevels = %d, want 0", skipper.NumLevels())
	}
}

func TestSkipperAdvance(t *testing.T) {
	// 300 docs: block 0 = [0,127], block 1 = [128,255], block 2 = [256,299]
	values := make([]int64, 300)
	for i := range values {
		values[i] = int64(i)
	}
	skipper := createSkipper(t, denseDocIDs(300), values)

	if !skipper.Advance(0) {
		t.Fatal("Advance(0) should succeed")
	}
	if skipper.MinDocID() != 0 || skipper.MaxDocID() != 127 {
		t.Errorf("block 0: [%d, %d], want [0, 127]", skipper.MinDocID(), skipper.MaxDocID())
	}

	if !skipper.Advance(130) {
		t.Fatal("Advance(130) should succeed")
	}
	if skipper.MinDocID() != 128 || skipper.MaxDocID() != 255 {
		t.Errorf("block 1: [%d, %d], want [128, 255]", skipper.MinDocID(), skipper.MaxDocID())
	}

	if !skipper.Advance(256) {
		t.Fatal("Advance(256) should succeed")
	}
	if skipper.MinDocID() != 256 || skipper.MaxDocID() != 299 {
		t.Errorf("block 2: [%d, %d], want [256, 299]", skipper.MinDocID(), skipper.MaxDocID())
	}

	if skipper.Advance(300) {
		t.Error("Advance(300) should fail — beyond last doc")
	}
}

func TestSkipperAdvanceToValue(t *testing.T) {
	// 3 blocks with distinct value ranges
	values := make([]int64, 300)
	for i := range 128 {
		values[i] = int64(100 + i) // block 0: [100, 227]
	}
	for i := 128; i < 256; i++ {
		values[i] = int64(500 + i - 128) // block 1: [500, 627]
	}
	for i := 256; i < 300; i++ {
		values[i] = int64(1000 + i - 256) // block 2: [1000, 1043]
	}
	skipper := createSkipper(t, denseDocIDs(300), values)

	// Advance to block containing values in [500, 600]
	skipper.Advance(0)
	if !skipper.AdvanceToValue(500, 600) {
		t.Fatal("AdvanceToValue(500, 600) should succeed")
	}
	if skipper.MinDocID() != 128 {
		t.Errorf("expected block 1 (minDocID=128), got minDocID=%d", skipper.MinDocID())
	}

	// Advance to value range that only matches block 2
	skipper.Advance(0) // reset
	if !skipper.AdvanceToValue(1000, 1100) {
		t.Fatal("AdvanceToValue(1000, 1100) should succeed")
	}
	if skipper.MinDocID() != 256 {
		t.Errorf("expected block 2 (minDocID=256), got minDocID=%d", skipper.MinDocID())
	}

	// Value range that matches nothing
	skipper.Advance(0)
	if skipper.AdvanceToValue(2000, 3000) {
		t.Error("AdvanceToValue(2000, 3000) should fail — no matching blocks")
	}
}

func TestSkipperHierarchicalLevels(t *testing.T) {
	// Create enough blocks to have multiple levels
	// 128 * 8 = 1024 docs → 8 level-0 blocks → 1 level-1 block
	docCount := 1024 + 128 // 9 level-0 blocks
	values := make([]int64, docCount)
	for i := range values {
		values[i] = int64(i)
	}
	skipper := createSkipper(t, denseDocIDs(docCount), values)

	if skipper.NumLevels() < 2 {
		t.Fatalf("expected at least 2 levels for %d docs, got %d", docCount, skipper.NumLevels())
	}
	if skipper.BlockCount(0) != 9 {
		t.Errorf("level 0 blockCount = %d, want 9", skipper.BlockCount(0))
	}
	if skipper.BlockCount(1) != 2 {
		t.Errorf("level 1 blockCount = %d, want 2", skipper.BlockCount(1))
	}
}

func TestSkipIndexFromNDV(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	values := []int64{10, 20, 30, 40, 50}
	if err := writeNumericDocValues(dir, "seg0", "f", values, 5, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeNumericDocValuesSkipIndexFromNDV(dir, "seg0", "f", 5); err != nil {
		t.Fatal(err)
	}

	path := dir.FilePath("seg0.f.ndvs")
	data, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		t.Fatal(err)
	}
	if skipper.GlobalMin() != 10 || skipper.GlobalMax() != 50 {
		t.Errorf("global range = [%d, %d], want [10, 50]", skipper.GlobalMin(), skipper.GlobalMax())
	}
}

func TestSkipperBlockDocCount(t *testing.T) {
	values := make([]int64, 300)
	for i := range values {
		values[i] = int64(i)
	}
	skipper := createSkipper(t, denseDocIDs(300), values)

	skipper.Advance(0)
	if got := skipper.BlockDocCount(); got != 128 {
		t.Errorf("block 0 docCount = %d, want 128", got)
	}

	skipper.Advance(128)
	if got := skipper.BlockDocCount(); got != 128 {
		t.Errorf("block 1 docCount = %d, want 128", got)
	}

	skipper.Advance(256)
	if got := skipper.BlockDocCount(); got != 44 {
		t.Errorf("block 2 docCount = %d, want 44", got)
	}
}

func TestSkipperSparseDocIDs(t *testing.T) {
	// Sparse: docIDs = [0, 5, 10, 15, ..., 635] (128 values, step 5)
	n := 128
	docIDs := make([]int, n)
	values := make([]int64, n)
	for i := range n {
		docIDs[i] = i * 5
		values[i] = int64(i * 100)
	}
	skipper := createSkipper(t, docIDs, values)

	if skipper.DocCount() != 128 {
		t.Errorf("docCount = %d, want 128", skipper.DocCount())
	}
	if skipper.BlockCount(0) != 1 {
		t.Errorf("level 0 blockCount = %d, want 1", skipper.BlockCount(0))
	}

	skipper.Advance(0)
	if skipper.MinDocID() != 0 {
		t.Errorf("minDocID = %d, want 0", skipper.MinDocID())
	}
	if skipper.MaxDocID() != 635 {
		t.Errorf("maxDocID = %d, want 635", skipper.MaxDocID())
	}
	if skipper.BlockDocCount() != 128 {
		t.Errorf("blockDocCount = %d, want 128", skipper.BlockDocCount())
	}
}

func TestSkipperSparseMultipleBlocks(t *testing.T) {
	// 200 values with sparse docIDs (step 10)
	n := 200
	docIDs := make([]int, n)
	values := make([]int64, n)
	for i := range n {
		docIDs[i] = i * 10
		values[i] = int64(i)
	}
	skipper := createSkipper(t, docIDs, values)

	if skipper.DocCount() != 200 {
		t.Errorf("docCount = %d, want 200", skipper.DocCount())
	}
	// 200 values / 128 = 2 blocks
	if skipper.BlockCount(0) != 2 {
		t.Errorf("level 0 blockCount = %d, want 2", skipper.BlockCount(0))
	}

	// Block 0: docIDs[0..127] = docs [0, 1270]
	skipper.Advance(0)
	if skipper.MinDocID() != 0 {
		t.Errorf("block 0 minDocID = %d, want 0", skipper.MinDocID())
	}
	if skipper.MaxDocID() != 1270 {
		t.Errorf("block 0 maxDocID = %d, want 1270", skipper.MaxDocID())
	}
	if skipper.BlockDocCount() != 128 {
		t.Errorf("block 0 docCount = %d, want 128", skipper.BlockDocCount())
	}

	// Block 1: docIDs[128..199] = docs [1280, 1990]
	skipper.Advance(1280)
	if skipper.MinDocID() != 1280 {
		t.Errorf("block 1 minDocID = %d, want 1280", skipper.MinDocID())
	}
	if skipper.MaxDocID() != 1990 {
		t.Errorf("block 1 maxDocID = %d, want 1990", skipper.MaxDocID())
	}
	if skipper.BlockDocCount() != 72 {
		t.Errorf("block 1 docCount = %d, want 72", skipper.BlockDocCount())
	}
}

func TestSortedDocValuesSkipIndexRoundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// 5 docs with string values: "cherry", "apple", "banana", "apple", "cherry"
	// Sorted unique: apple(0), banana(1), cherry(2)
	// Ordinals: [2, 0, 1, 0, 2]
	values := []string{"cherry", "apple", "banana", "apple", "cherry"}
	if err := writeSortedDocValues(dir, "seg0", "f", values, 5); err != nil {
		t.Fatal(err)
	}
	if err := writeSortedDocValuesSkipIndexFromOrd(dir, "seg0", "f", 5); err != nil {
		t.Fatal(err)
	}

	path := dir.FilePath("seg0.f.sdvs")
	data, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		t.Fatal(err)
	}

	if skipper.DocCount() != 5 {
		t.Errorf("docCount = %d, want 5", skipper.DocCount())
	}
	if skipper.GlobalMin() != 0 {
		t.Errorf("globalMin = %d, want 0 (apple)", skipper.GlobalMin())
	}
	if skipper.GlobalMax() != 2 {
		t.Errorf("globalMax = %d, want 2 (cherry)", skipper.GlobalMax())
	}
	if skipper.BlockCount(0) != 1 {
		t.Errorf("level 0 blockCount = %d, want 1", skipper.BlockCount(0))
	}
}

func TestSortedDocValuesSkipIndexWithMissingValues(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// 5 docs, docs 1 and 3 have no value (empty string)
	values := []string{"banana", "", "apple", "", "cherry"}
	if err := writeSortedDocValues(dir, "seg0", "f", values, 5); err != nil {
		t.Fatal(err)
	}
	if err := writeSortedDocValuesSkipIndexFromOrd(dir, "seg0", "f", 5); err != nil {
		t.Fatal(err)
	}

	path := dir.FilePath("seg0.f.sdvs")
	data, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		t.Fatal(err)
	}

	// Only 3 docs have values
	if skipper.DocCount() != 3 {
		t.Errorf("docCount = %d, want 3", skipper.DocCount())
	}
	// apple=0, banana=1, cherry=2
	if skipper.GlobalMin() != 0 {
		t.Errorf("globalMin = %d, want 0", skipper.GlobalMin())
	}
	if skipper.GlobalMax() != 2 {
		t.Errorf("globalMax = %d, want 2", skipper.GlobalMax())
	}
}

func TestSortedDocValuesSkipIndexFromOrd(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := store.NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Write sorted doc values first to create .sdvo file
	values := []string{"cherry", "apple", "", "banana", "apple"}
	if err := writeSortedDocValues(dir, "seg0", "f", values, 5); err != nil {
		t.Fatal(err)
	}

	// Build skip index from the .sdvo file
	if err := writeSortedDocValuesSkipIndexFromOrd(dir, "seg0", "f", 5); err != nil {
		t.Fatal(err)
	}

	path := dir.FilePath("seg0.f.sdvs")
	data, err := store.OpenMMap(path)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	skipper, err := NewDocValuesSkipper(data)
	if err != nil {
		t.Fatal(err)
	}

	// Doc 2 has no value, so only 4 docs
	if skipper.DocCount() != 4 {
		t.Errorf("docCount = %d, want 4", skipper.DocCount())
	}
	// apple=0, banana=1, cherry=2
	if skipper.GlobalMin() != 0 {
		t.Errorf("globalMin = %d, want 0", skipper.GlobalMin())
	}
	if skipper.GlobalMax() != 2 {
		t.Errorf("globalMax = %d, want 2", skipper.GlobalMax())
	}
}

func TestSkipperSparseAdvanceAcrossGap(t *testing.T) {
	// Two clusters of docs with a gap
	// Cluster 1: docIDs [0..127], values [0..127]
	// Cluster 2: docIDs [1000..1127], values [1000..1127]
	docIDs := make([]int, 256)
	values := make([]int64, 256)
	for i := range 128 {
		docIDs[i] = i
		values[i] = int64(i)
	}
	for i := range 128 {
		docIDs[128+i] = 1000 + i
		values[128+i] = int64(1000 + i)
	}
	skipper := createSkipper(t, docIDs, values)

	// Advance to doc 500 (in the gap) should land on block 1 (docID range [1000, 1127])
	if !skipper.Advance(500) {
		t.Fatal("Advance(500) should succeed")
	}
	if skipper.MinDocID() != 1000 {
		t.Errorf("expected minDocID=1000, got %d", skipper.MinDocID())
	}
	if skipper.MaxDocID() != 1127 {
		t.Errorf("expected maxDocID=1127, got %d", skipper.MaxDocID())
	}
}
