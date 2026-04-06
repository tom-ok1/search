# Lazy Position Decoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DiskPostingsIterator` defer position decoding until `Positions()` is called, so merge skips allocation/decoding for deleted documents.

**Architecture:** Modify `DiskPostingsIterator.Next()` to read docID + freq + posCount then skip position VInts without allocating. Add lazy `Positions()` that seeks back to decode on demand. No changes to the merge loop or `PostingsIterator` interface.

**Tech Stack:** Go, mmap-based I/O (`store.MMapIndexInput`)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `index/postings.go` | Modify | Add lazy decoding fields and rewrite `Next()`/`Positions()` on `DiskPostingsIterator` |
| `index/postings_test.go` | Modify | Add unit test for lazy position decoding behavior |
| `index/disk_segment_test.go` | Unchanged | Existing `TestDiskSegmentPostingsIterator` validates end-to-end correctness |
| `index/merge_bench_test.go` | Unchanged | Existing `BenchmarkForceMergeWithDeletions` validates performance improvement |

---

### Task 1: Add Failing Test for Lazy Position Decoding

**Files:**
- Modify: `index/postings_test.go`

- [ ] **Step 1: Write test that verifies Positions() works after Next()**

This test creates a `DiskPostingsIterator` via a real disk segment and verifies that calling `Next()` without `Positions()` still allows the iterator to advance correctly, and that `Positions()` returns correct data when called.

```go
func TestDiskPostingsIteratorLazyPositions(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	// Use "the" which appears in multiple docs
	iter := ds.PostingsIterator("title", "the")

	// Advance through all postings, only calling Positions() on some
	var count int
	for iter.Next() {
		// Always check docID and freq are valid
		if iter.DocID() < 0 {
			t.Fatalf("posting %d: invalid DocID %d", count, iter.DocID())
		}
		if iter.Freq() <= 0 {
			t.Fatalf("posting %d: invalid Freq %d", count, iter.Freq())
		}

		// Only call Positions() on even-numbered postings
		if count%2 == 0 {
			positions := iter.Positions()
			if len(positions) != iter.Freq() {
				t.Errorf("posting %d: Positions length %d != Freq %d",
					count, len(positions), iter.Freq())
			}
		}
		count++
	}

	if count == 0 {
		t.Fatal("expected at least one posting for term 'the'")
	}
}
```

- [ ] **Step 2: Run the test to verify it passes with current implementation**

Run: `go test ./index/ -run TestDiskPostingsIteratorLazyPositions -v`
Expected: PASS (current implementation always decodes positions, so this works trivially)

- [ ] **Step 3: Add test that verifies positions are correct after skipping**

This test verifies that skipping `Positions()` on one posting doesn't corrupt the next posting's data. It compares lazy results against a reference iterator that reads everything.

```go
func TestDiskPostingsIteratorSkipPositionsCorrectness(t *testing.T) {
	seg := buildTestSegment(t)
	ds, _ := writeAndOpenDiskSegment(t, seg)
	defer ds.Close()

	terms := []string{"the", "quick", "brown", "fox", "lazy", "dog"}

	for _, term := range terms {
		// Reference: read all postings with positions
		refIter := ds.PostingsIterator("title", term)
		var refPostings []Posting
		for refIter.Next() {
			refPostings = append(refPostings, Posting{
				DocID:     refIter.DocID(),
				Freq:      refIter.Freq(),
				Positions: refIter.Positions(),
			})
		}

		// Test: skip Positions() on odd postings, verify even postings match
		testIter := ds.PostingsIterator("title", term)
		idx := 0
		for testIter.Next() {
			if testIter.DocID() != refPostings[idx].DocID {
				t.Errorf("term %q posting[%d]: DocID got %d, want %d",
					term, idx, testIter.DocID(), refPostings[idx].DocID)
			}
			if testIter.Freq() != refPostings[idx].Freq {
				t.Errorf("term %q posting[%d]: Freq got %d, want %d",
					term, idx, testIter.Freq(), refPostings[idx].Freq)
			}

			// Call Positions() only on even postings
			if idx%2 == 0 {
				positions := testIter.Positions()
				for j, pos := range positions {
					if pos != refPostings[idx].Positions[j] {
						t.Errorf("term %q posting[%d] pos[%d]: got %d, want %d",
							term, idx, j, pos, refPostings[idx].Positions[j])
					}
				}
			}
			idx++
		}

		if idx != len(refPostings) {
			t.Errorf("term %q: got %d postings, want %d", term, idx, len(refPostings))
		}
	}
}
```

- [ ] **Step 4: Run the new test**

Run: `go test ./index/ -run TestDiskPostingsIteratorSkipPositionsCorrectness -v`
Expected: PASS (current implementation decodes everything, so results match trivially)

- [ ] **Step 5: Commit**

```bash
git add index/postings_test.go
git commit -m "test: add tests for lazy position decoding in DiskPostingsIterator"
```

---

### Task 2: Implement Lazy Position Decoding

**Files:**
- Modify: `index/postings.go:94-159`

- [ ] **Step 1: Add lazy decoding fields to DiskPostingsIterator**

Replace the struct definition at lines 94-103:

```go
// DiskPostingsIterator reads postings from a mmap'd .tdat slice using delta decoding.
// Positions are decoded lazily: Next() skips position bytes, and Positions()
// seeks back to decode them on demand.
type DiskPostingsIterator struct {
	input     *store.MMapIndexInput
	remaining int // remaining postings to read
	prevDocID int // for delta decoding

	docID            int
	freq             int
	positions        []int
	posCount         int  // number of position VInts to decode
	posStartOffset   int  // file offset where position data begins
	positionsDecoded bool // true after Positions() has been called for current posting
}
```

- [ ] **Step 2: Rewrite Next() to skip positions**

Replace the `Next()` method at lines 105-146:

```go
func (it *DiskPostingsIterator) Next() bool {
	if it.remaining <= 0 {
		return false
	}
	it.remaining--

	// Read delta-encoded doc ID
	delta, err := it.input.ReadVInt()
	if err != nil {
		return false
	}
	it.docID = it.prevDocID + delta
	it.prevDocID = it.docID

	// Read frequency
	freq, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.freq = freq

	// Read position count, save offset, skip position VInts
	posCount, err := it.input.ReadVInt()
	if err != nil {
		it.remaining = 0
		return false
	}
	it.posCount = posCount
	it.posStartOffset = it.input.Position()
	it.positions = nil
	it.positionsDecoded = false

	// Skip past position VInts without decoding
	for range posCount {
		if _, err := it.input.ReadVInt(); err != nil {
			it.remaining = 0
			return false
		}
	}

	return true
}
```

- [ ] **Step 3: Rewrite Positions() for lazy decoding**

Replace the `Positions()` method at line 150:

```go
func (it *DiskPostingsIterator) Positions() []int {
	if it.positionsDecoded {
		return it.positions
	}
	it.positionsDecoded = true

	if it.posCount == 0 {
		return nil
	}

	// Save current position, seek back to position data, decode, restore
	savedPos := it.input.Position()
	it.input.Seek(it.posStartOffset)

	it.positions = make([]int, it.posCount)
	prevPos := 0
	for i := range it.posCount {
		posDelta, err := it.input.ReadVInt()
		if err != nil {
			it.input.Seek(savedPos)
			return it.positions[:i]
		}
		it.positions[i] = prevPos + posDelta
		prevPos = it.positions[i]
	}

	it.input.Seek(savedPos)
	return it.positions
}
```

- [ ] **Step 4: Run all postings and disk segment tests**

Run: `go test ./index/ -run "TestDiskPostingsIterator|TestSlicePostingsIterator|TestDiskSegment" -v`
Expected: All PASS

- [ ] **Step 5: Run the merge tests**

Run: `go test ./index/ -run "TestMerge" -v`
Expected: All PASS

- [ ] **Step 6: Run the full index test suite**

Run: `go test ./index/ -v -timeout=120s`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add index/postings.go
git commit -m "perf: lazy position decoding in DiskPostingsIterator

Next() now reads docID + freq + posCount and skips position VInts
without allocating. Positions() seeks back and decodes on demand.
During merge, deleted docs never call Positions(), avoiding wasted
allocation and decoding.

Fixes #35"
```

---

### Task 3: Verify Performance Improvement

**Files:**
- No file changes — benchmark verification only

- [ ] **Step 1: Run the deletion benchmark**

Run: `go test ./index/ -bench=BenchmarkForceMergeWithDeletions -benchmem -count=1 -timeout=300s`
Expected: `total-alloc-MB` should decrease as deletion rate increases. At 50% deletion, allocations should be noticeably lower than at 10%.

- [ ] **Step 2: Run the full benchmark suite to check for regressions**

Run: `go test ./index/ -bench=BenchmarkForceMerge -benchmem -count=1 -timeout=300s`
Expected: No significant regression in the 0%-deletion case. The lazy path adds one seek per live doc in `Positions()`, which should be negligible for mmap.

- [ ] **Step 3: Report results**

Compare before/after numbers. If allocations are no longer identical across deletion rates, the fix is working.
