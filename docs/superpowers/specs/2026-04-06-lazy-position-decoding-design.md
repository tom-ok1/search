# Lazy Position Decoding in DiskPostingsIterator

**Issue:** [#35 — Merge does not skip deleted documents, wasting CPU and I/O](https://github.com/tom-ok1/search/issues/35)

**Date:** 2026-04-06

## Problem

During merge, `DiskPostingsIterator.Next()` fully decodes every posting — docID, frequency, and all positions — including for deleted documents. Positions are the most expensive part: each call allocates a `[]int` slice and delta-decodes every position VInt. For deleted docs, this work is immediately discarded. The result is that merge time and memory are identical regardless of deletion rate.

## Solution

Make position decoding lazy in `DiskPostingsIterator`. `Next()` reads docID + freq + posCount and skips past position bytes without decoding or allocating. `Positions()` seeks back and decodes on demand.

## Detailed Design

### DiskPostingsIterator Changes

**New fields:**

```go
type DiskPostingsIterator struct {
    input     *store.MMapIndexInput
    remaining int
    prevDocID int

    docID           int
    freq            int
    positions       []int
    posCount        int  // NEW: number of positions to decode
    posStartOffset  int  // NEW: file offset where position VInts start
    positionsDecoded bool // NEW: whether Positions() has materialized the slice
}
```

**`Next()` — decode docID + freq, skip positions:**

1. Read delta-encoded docID (unchanged).
2. Read freq VInt (unchanged).
3. Read `posCount` VInt (was inlined into position decoding before).
4. Save `posStartOffset = input.Position()`.
5. Skip `posCount` VInts by calling `ReadVInt()` in a discard loop. This is cheap — just advancing a pointer through mmap'd memory, no allocation.
6. Set `positionsDecoded = false`, `positions = nil`.

**`Positions()` — decode on demand:**

1. If `positionsDecoded` is true, return cached `positions`.
2. Save `currentPos = input.Position()`.
3. `input.Seek(posStartOffset)`.
4. Allocate `[]int` of length `posCount`, delta-decode positions.
5. `input.Seek(currentPos)` to restore cursor.
6. Set `positionsDecoded = true`.

**`Advance()` — no change needed.** It calls `Next()` internally, which now uses the lazy path.

### Merge Loop

No changes. The existing code at `merger.go:477-487` naturally benefits:

```go
for pi.Next() {                      // decodes docID + freq only
    oldDoc := pi.DocID()
    if !mapper.IsLive(i, oldDoc) {
        continue                     // Positions() never called — no allocation
    }
    postings = append(postings, Posting{
        DocID:     mapper.Map(i, oldDoc),
        Freq:      pi.Freq(),
        Positions: pi.Positions(),   // decoded on demand for live docs only
    })
}
```

### Search Path

Search queries always call `Positions()` after `Next()`, so behavior is identical — just slightly deferred. No correctness change.

## Performance Impact

**Saved for each deleted doc:**
- One `make([]int, posCount)` allocation (main source of GC pressure)
- Position delta-decoding computation

**Still costs for each deleted doc:**
- Scanning posCount VInts to advance cursor (negligible — pointer arithmetic over mmap)

At 50% deletion rate, this should roughly halve allocations during merge.

## Verification

Run the existing benchmark and compare allocations across deletion rates:

```bash
go test ./index/ -bench=BenchmarkForceMergeWithDeletions -benchmem -count=3 -timeout=300s
```

Expected: `total-alloc-MB` should decrease proportionally with deletion rate. At 50% deletion, allocations should be ~50% lower than at 0%.
