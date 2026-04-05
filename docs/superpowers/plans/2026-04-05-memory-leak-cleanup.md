# Memory Leak Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix remaining memory leaks (after flush queue fix in 0faeee4) so heap stabilizes regardless of document count.

**Architecture:** Three targeted fixes: (1) nil DWPT segment data after flush to release InMemorySegment, (2) nil RAU reader reference on Close to break GC retention, (3) call deleteStaleFiles after merge to clean orphaned files. Plus a regression test that asserts heap plateaus.

**Tech Stack:** Go, gosearch/index package

---

### Task 1: Release DWPT InMemorySegment after flush

After a DWPT is flushed to disk, its `segment` field still holds the full InMemorySegment with all document data. The DWPT is removed from the pool so it's GC-eligible, but proactively nilling the reference ensures the large allocation is freed immediately rather than waiting for GC to trace through potentially long reference chains.

**Files:**
- Modify: `index/documents_writer.go:74-85` (doFlush method)
- Test: `index/documents_writer_test.go`

- [ ] **Step 1: Write the failing test**

Add a test to `index/documents_writer_test.go` that verifies the DWPT's segment is nil after flush:

```go
func TestDoFlush_ReleasesSegmentMemory(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	w := NewIndexWriter(dir, fa, 5)
	defer w.Close()

	// Index enough docs to trigger a flush (bufferSize=5)
	for i := range 6 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("word%d", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
	}

	// The flushed DWPT should have had its segment nilled out.
	// Verify by checking that no DWPT in the pool retains a large segment.
	// We can verify indirectly: the pool's free DWPTs should either be
	// empty or have segments with 0 docs (reset segments).
	pool := w.docWriter.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, dwpt := range pool.free {
		if dwpt.segment != nil && dwpt.segment.docCount > 0 {
			// Free DWPTs should have been reset (fresh segment)
			// This is fine — free DWPTs are reused with fresh segments.
		}
	}
	// The key invariant: no leaked DWPT references with large segment data.
	// This test verifies the code path exists; the memory benchmark verifies effect.
}
```

Actually, the better test is to verify the DWPT's segment is nil after doFlush returns. We need to capture the DWPT reference. Let's test this differently — verify via the memory benchmark (Task 4). For the unit test, verify the segment is nilled:

```go
func TestDoFlush_NilsSegment(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))
	dq := newDeleteQueue()
	dwpt := newDWPT("_seg0", fa, dq)

	// Add some documents
	for i := range 5 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("word%d", i), document.FieldTypeText)
		dwpt.addDocument(doc)
	}

	if dwpt.segment == nil {
		t.Fatal("segment should not be nil before flush")
	}
	if dwpt.segment.docCount != 5 {
		t.Fatalf("expected 5 docs, got %d", dwpt.segment.docCount)
	}

	// Flush
	_, err = dwpt.flush(dir)
	if err != nil {
		t.Fatal(err)
	}

	if dwpt.segment != nil {
		t.Error("segment should be nil after flush to release memory")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestDoFlush_NilsSegment -v`
Expected: FAIL — `segment should be nil after flush to release memory`

- [ ] **Step 3: Write minimal implementation**

In `index/dwpt.go`, nil out the segment at the end of the `flush` method:

```go
// flush writes this DWPT's segment to disk and returns a SegmentCommitInfo.
// Buffered delete terms from pendingUpdates are applied using docIDUpto.
func (dwpt *DocumentsWriterPerThread) flush(dir store.Directory) (*SegmentCommitInfo, error) {
	seg := dwpt.segment
	if seg.docCount == 0 {
		return nil, nil
	}

	files, fields, err := WriteSegmentV2(dir, seg)
	if err != nil {
		return nil, fmt.Errorf("flush segment %s: %w", seg.name, err)
	}

	info := &SegmentCommitInfo{
		Name:   seg.name,
		MaxDoc: seg.docCount,
		Fields: fields,
		Files:  files,
	}

	if !dwpt.pendingUpdates.any() {
		dwpt.segment = nil // release InMemorySegment for GC
		return info, nil
	}

	// when deleted terms exist
	if err := dwpt.applyDeletes(dir, info); err != nil {
		return nil, err
	}
	dwpt.pendingUpdates.clear()

	dwpt.segment = nil // release InMemorySegment for GC
	return info, nil
}
```

The key change is adding `dwpt.segment = nil` before each return path (after the segment data has been fully written to disk and is no longer needed).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestDoFlush_NilsSegment -v`
Expected: PASS

- [ ] **Step 5: Run existing tests to ensure no regression**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -count=1 -timeout=120s`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak
git add index/dwpt.go index/documents_writer_test.go
git commit -m "fix: nil DWPT segment after flush to release InMemorySegment for GC"
```

---

### Task 2: Nil reader reference in ReadersAndUpdates.Close

When `ReadersAndUpdates.Close()` is called (during merge cleanup), it calls `reader.Close()` which decrements the DiskSegment's refCount. But the `rau.reader` field still holds a pointer to the DiskSegment struct. If anything accidentally retains a reference to the RAU, the DiskSegment (with all its parsed maps — `numericDVParsed`, `termFSTs`, `bkdReaders`) cannot be GC'd. Nilling the field ensures the DiskSegment is released even if the RAU outlives its expected lifetime.

**Files:**
- Modify: `index/readers_and_updates.go:77-83` (Close method)
- Test: `index/readers_and_updates_test.go`

- [ ] **Step 1: Write the failing test**

Add to `index/readers_and_updates_test.go`:

```go
func TestReadersAndUpdates_CloseNilsReader(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))

	// Create a segment on disk
	w := NewIndexWriter(dir, fa, 100)
	doc := document.NewDocument()
	doc.AddField("body", "hello world", document.FieldTypeText)
	w.AddDocument(doc)
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	segName := w.segmentInfos.Segments[0].Name
	w.Close()

	// Create RAU and open reader
	info := &SegmentCommitInfo{Name: segName, MaxDoc: 1}
	rau := NewReadersAndUpdates(info, dir.FilePath(""))
	_, err = rau.getReader()
	if err != nil {
		t.Fatal(err)
	}
	if rau.reader == nil {
		t.Fatal("reader should be set after getReader()")
	}

	// Close should nil the reader
	if err := rau.Close(); err != nil {
		t.Fatal(err)
	}
	if rau.reader != nil {
		t.Error("reader should be nil after Close() to release DiskSegment for GC")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestReadersAndUpdates_CloseNilsReader -v`
Expected: FAIL — `reader should be nil after Close()`

- [ ] **Step 3: Write minimal implementation**

In `index/readers_and_updates.go`, modify the `Close` method:

```go
// Close releases the underlying DiskSegment.
func (rau *ReadersAndUpdates) Close() error {
	if rau.reader != nil {
		err := rau.reader.Close()
		rau.reader = nil
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestReadersAndUpdates_CloseNilsReader -v`
Expected: PASS

- [ ] **Step 5: Run existing tests**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -count=1 -timeout=120s`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak
git add index/readers_and_updates.go index/readers_and_updates_test.go
git commit -m "fix: nil reader in ReadersAndUpdates.Close to release DiskSegment for GC"
```

---

### Task 3: Delete stale files after merge

`deleteStaleFiles()` is currently only called during `Commit()` (writer.go:189). After `executeMerge()` removes old segments from `segmentInfos`, their files remain on disk until the next Commit. In Lucene, `IndexWriter.commitMerge` calls `deleter.decRef(files)` immediately. Adding `deleteStaleFiles()` after merge ensures orphaned files are cleaned up promptly, reducing disk usage and preventing stale mmap handles from lingering.

**Files:**
- Modify: `index/writer.go:321-374` (executeMerge method)
- Test: `index/writer_test.go`

- [ ] **Step 1: Write the failing test**

Add to `index/writer_test.go`:

```go
func TestExecuteMerge_DeletesStaleFiles(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	))

	w := NewIndexWriter(dir, fa, 2) // flush every 2 docs
	defer w.Close()

	// Create 4 segments (8 docs, 2 per segment)
	for i := range 8 {
		doc := document.NewDocument()
		doc.AddField("body", fmt.Sprintf("word%d", i), document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Record segment files before merge
	preFiles, _ := dir.ListAll()
	var preSegFiles []string
	for _, f := range preFiles {
		if len(f) > 4 && f[:4] == "_seg" {
			preSegFiles = append(preSegFiles, f)
		}
	}

	// Force merge all segments into 1
	if err := w.ForceMerge(1); err != nil {
		t.Fatal(err)
	}

	// Without calling Commit, stale files from merged-away segments
	// should already be deleted (or at least scheduled for deletion)
	postFiles, _ := dir.ListAll()
	referenced := w.segmentInfos.ReferencedFiles()

	for _, f := range postFiles {
		if len(f) > 4 && f[:4] == "_seg" && !referenced[f] {
			t.Errorf("stale file %s not cleaned up after merge", f)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestExecuteMerge_DeletesStaleFiles -v`
Expected: FAIL — stale files remain after merge

- [ ] **Step 3: Write minimal implementation**

In `index/writer.go`, add `w.deleteStaleFiles()` at the end of `executeMerge`:

```go
// executeMerge performs a single merge operation. Caller must hold w.mu.
func (w *IndexWriter) executeMerge(candidate MergeCandidate) error {
	if len(candidate.Segments) < 2 {
		return nil
	}

	inputs := make([]MergeInput, len(candidate.Segments))
	for i, info := range candidate.Segments {
		rau := w.getOrCreateRAU(info)
		reader, err := rau.getReader()
		if err != nil {
			return fmt.Errorf("open segment %s for merge: %w", info.Name, err)
		}
		inputs[i] = MergeInput{
			Segment:   reader,
			IsDeleted: rau.IsDeleted,
		}
	}

	newName := w.nextSegmentName()
	result, err := MergeSegmentsToDisk(w.dir, inputs, newName)
	if err != nil {
		return fmt.Errorf("merge segments: %w", err)
	}

	newInfo := &SegmentCommitInfo{
		Name:   newName,
		MaxDoc: result.DocCount,
		Fields: result.Fields,
		Files:  result.Files,
	}

	mergedNames := make(map[string]bool)
	for _, info := range candidate.Segments {
		mergedNames[info.Name] = true
	}

	var remaining []*SegmentCommitInfo
	for _, info := range w.segmentInfos.Segments {
		if mergedNames[info.Name] {
			if rau, ok := w.readerMap[info.Name]; ok {
				rau.Close()
				delete(w.readerMap, info.Name)
			}
			continue
		}
		remaining = append(remaining, info)
	}
	remaining = append(remaining, newInfo)
	w.segmentInfos.Segments = remaining
	w.segmentInfos.Version++

	// Clean up orphaned files from merged-away segments immediately,
	// rather than waiting for the next Commit.
	w.deleteStaleFiles()

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestExecuteMerge_DeletesStaleFiles -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -count=1 -timeout=120s`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak
git add index/writer.go index/writer_test.go
git commit -m "fix: delete stale segment files immediately after merge"
```

---

### Task 4: Add memory stability regression test

Add a focused test that indexes enough documents to trigger multiple merge cycles and asserts that heap memory plateaus rather than growing linearly. This serves as a regression gate for future memory leaks. Unlike the benchmark in `scale_bench_test.go` which reports metrics, this test uses `t.Fatal` to enforce the invariant.

**Files:**
- Create: `index/memory_stability_test.go`

- [ ] **Step 1: Write the regression test**

Create `index/memory_stability_test.go`:

```go
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
```

- [ ] **Step 2: Run the test**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -run TestMemoryStability -v -timeout=600s`
Expected: PASS — heap should plateau after ramp-up

- [ ] **Step 3: Commit**

```bash
cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak
git add index/memory_stability_test.go
git commit -m "test: add memory stability regression test for issue #31"
```

---

### Task 5: Run full test suite and benchmarks

Final verification that all changes work together and nothing is broken.

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./... -count=1 -timeout=300s`
Expected: All tests PASS

- [ ] **Step 2: Run memory stability benchmark for comparison**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -bench=BenchmarkMemoryStability -benchmem -count=1 -timeout=600s`
Expected: Heap metrics should show plateau rather than linear growth

- [ ] **Step 3: Verify no regressions in existing benchmarks**

Run: `cd /Users/ookitomoya/Documents/DocumentMacbookAir/search/.claude/worktrees/fix-memory-leak && go test ./index/ -bench='BenchmarkIndexWriter_AddDocument|BenchmarkIndexWriter_Commit' -benchmem -count=1 -timeout=120s`
Expected: No significant performance regression
