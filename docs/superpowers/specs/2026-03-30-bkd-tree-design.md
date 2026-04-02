# BKD Tree for 1D Point Indexing — Design Spec

## Goal

Replace the doc values skip index (`.ndvs`) with a BKD tree (`.kd`) for point field range queries, matching Lucene's architecture. Doc values (`.ndv`) are kept alongside for per-doc random access.

## Scope

- 1D points only (`long`, `double`). Multi-dimensional support tracked in tom-ok1/search#17.
- 512 points per leaf (Lucene default).
- Visitor pattern for tree traversal (Lucene's `IntersectVisitor` + `Relation` enum).

## Architecture

The BKD tree is a binary space partitioning tree over 1D point values. Inner nodes store split values; leaf nodes store sorted (docID, value) pairs. The tree is built during segment flush and persisted to a `.kd` file. Range queries traverse the tree using a visitor pattern that classifies each node as `INSIDE`, `OUTSIDE`, or `CROSSES` the query range.

**Data flow:**
- **Write:** `IndexWriter` → `DWPT` buffers points → segment flush → `BKDWriter` sorts + partitions → `.kd` file
- **Read:** `PointRangeQuery` → `PointValues` from segment reader → `BKDReader` → `Intersect(tree, visitor)` → collected docIDs
- **Doc values:** `.ndv` files remain for per-doc access (sorting, aggregation). Independent of BKD tree.

## Package Structure

```
index/bkd/
  bkd.go           — Relation enum, IntersectVisitor, PointTree interface, Intersect()
  bkd_writer.go    — BKDWriter: builds tree from points, writes .kd file
  bkd_reader.go    — BKDReader: reads .kd file, provides BKDPointTree navigation
```

## Core Types

### Relation Enum (`index/bkd/bkd.go`)

```go
type Relation int
const (
    CellOutsideQuery  Relation = iota  // No overlap — skip subtree
    CellInsideQuery                     // Fully contained — bulk visit all docs
    CellCrossesQuery                    // Partial overlap — recurse or filter
)
```

### IntersectVisitor (`index/bkd/bkd.go`)

```go
type IntersectVisitor interface {
    Visit(docID int)                        // doc in cell fully inside query
    VisitValue(docID int, value int64)      // doc in cell crossing query boundary
    Compare(minValue, maxValue int64) Relation
}
```

### PointTree (`index/bkd/bkd.go`)

```go
type PointTree interface {
    MoveToChild() bool       // move to left child; false if already at leaf
    MoveToSibling() bool     // move to right sibling; false if none
    MoveToParent() bool      // move to parent; false if at root
    MinValue() int64
    MaxValue() int64
    Size() int               // points in subtree
    VisitDocIDs(v IntersectVisitor)
    VisitDocValues(v IntersectVisitor)
}
```

### Intersect Function (`index/bkd/bkd.go`)

```go
func Intersect(tree PointTree, visitor IntersectVisitor) {
    r := visitor.Compare(tree.MinValue(), tree.MaxValue())
    switch r {
    case CellOutsideQuery:
        return
    case CellInsideQuery:
        tree.VisitDocIDs(visitor)
    case CellCrossesQuery:
        if tree.MoveToChild() {
            Intersect(tree, visitor)
            if tree.MoveToSibling() {
                Intersect(tree, visitor)
            }
            tree.MoveToParent()
        } else {
            tree.VisitDocValues(visitor)
        }
    }
}
```

### PointValues Interface (`index/`)

```go
type PointValues interface {
    PointTree() bkd.PointTree
    MinValue() int64
    MaxValue() int64
    Size() int
    DocCount() int
}
```

Added to `SegmentReader` interface: `PointValues(field string) PointValues`.

## On-Disk Format (`.kd`)

Single file per point field per segment.

```
.{seg}.{field}.kd

┌─────────────────────────────────┐
│ Header (32 bytes)               │
│   maxPointsInLeaf  (uint32)     │  = 512
│   numLeaves        (uint32)     │
│   numPoints        (uint32)     │
│   docCount         (uint32)     │
│   globalMinValue   (int64)      │
│   globalMaxValue   (int64)      │
├─────────────────────────────────┤
│ Inner Node Index                │
│   Heap-ordered (node 1 = root)  │
│   Per node:                     │
│     splitValue     (int64)      │
│     numPoints      (uint32)     │
├─────────────────────────────────┤
│ Leaf Block Directory            │
│   Per leaf:                     │
│     offset         (uint64)     │
│     numPoints      (uint32)     │
│     minValue       (int64)      │
│     maxValue       (int64)      │
├─────────────────────────────────┤
│ Leaf Block Data                 │
│   Per leaf (sorted by value):   │
│     docIDs   [numPoints × uint32]
│     values   [numPoints × int64]│
└─────────────────────────────────┘
```

**Inner node layout:** Implicit binary heap. Node `i` has children `2i` and `2i+1`. No explicit pointers.

## BKDWriter Algorithm

1. Collect all `(docID, value)` pairs from the segment's buffered point data
2. Sort all points by value
3. Recursively partition:
   - If `numPoints <= 512`: write as leaf block
   - Otherwise: split at median, record split value as inner node, recurse left/right
4. Write: header → inner nodes (heap order) → leaf directory → leaf data

Simpler than Lucene's `BKDRadixSelector` — a single sort + recursive split is correct and efficient for 1D.

## BKDReader

```go
type BKDReader struct {
    meta       header
    innerNodes []innerNode       // heap-ordered
    leafDir    []leafDirEntry
    data       *store.MMapIndexInput
}
```

**BKDPointTree** — stateful cursor:

```go
type BKDPointTree struct {
    reader    *BKDReader
    nodeID    int        // 1-based heap index
    level     int
    nodeStack []int      // for MoveToParent
}
```

Navigation:
- `MoveToChild()`: `nodeID *= 2`. False if at leaf level (`nodeID >= numInnerNodes + 1`).
- `MoveToSibling()`: If even (left child), `nodeID++`, true. If odd, false.
- `MoveToParent()`: Pop from stack.
- `MinValue()`/`MaxValue()`: For leaves, from leaf directory. For inner nodes, computed by walking down to leftmost/rightmost descendant leaves in the heap (left child chain for min, right child chain for max). Can be cached on first access.
- `VisitDocIDs()`/`VisitDocValues()`: Read leaf data from `.kd` file at leaf's offset.

## Integration Changes

### Write Path (`index/segment_writer.go`)

For point fields, after writing `.ndv`:
- Remove: `writeNumericDocValuesSkipIndexFromNDV()` (no more `.ndvs`)
- Add: `bkd.NewBKDWriter()` → `Add(docID, value)` for each point → `Finish()` writes `.kd`

### Read Path (`index/disk_segment.go`)

- Add: `PointValues(field)` method — opens `.kd`, constructs `BKDReader`, returns as `PointValues`
- Remove: `DocValuesSkipper(field)` method

### InMemorySegment (`index/in_memory_segment.go`)

For NRT readers: build BKD tree in-memory from buffered `numericDocValues`. Implement `PointValues` interface over in-memory tree.

### PointRangeQuery (`search/point_range_query.go`)

Scorer changes from doc values iteration to BKD intersection:
1. Get `PointValues` from segment reader
2. `Intersect()` with a `pointRangeVisitor` → collects matching docIDs
3. Sort docIDs, iterate through them (skipping deleted docs)

### SegmentReader Interface (`index/segment.go`)

- Add: `PointValues(field string) PointValues`
- Remove: `DocValuesSkipper(field string) *DocValuesSkipper`

### Segment Metadata

- File list: adds `.kd`, removes `.ndvs`
- `point_fields` in `.meta` unchanged

### Cleanup

- Remove `doc_values_skipper.go` and `doc_values_skipper_test.go`
- Remove skip index writer functions
- Remove `DocValuesSkipper` from segment reader interface

## Testing

### Unit Tests (`index/bkd/`)

1. Writer/Reader roundtrip — write N points, read back, verify all recoverable
2. Tree structure — split values partition correctly, leaf sizes ≤ 512
3. PointTree navigation — all leaves reachable via MoveToChild/MoveToSibling
4. Intersect + visitor — full match, no match, partial match, single point, edge cases
5. Small dataset — fewer than 512 points (single leaf)
6. Large dataset — 10,000+ points, multiple tree levels
7. Duplicate values
8. Empty field — zero points

### Integration Tests (`search/`)

Existing tests updated to use BKD-backed implementation:
- `TestPointRangeQueryLong`
- `TestPointRangeQueryDouble`
- `TestPointRangeQuerySkipBlocks` (renamed, exercises multi-level tree)
- `TestPointRangeQueryDeletedDocs`

### Server Tests

No changes — `query_parser_test.go` tests parsing, not index internals.

## Known Divergences from Lucene

- **1D only** — Lucene supports 1-16 dimensions. Multi-dim tracked in tom-ok1/search#17.
- **Sort-based partitioning** — Lucene uses `BKDRadixSelector` for multi-dim efficiency. We sort once and split at medians, which is simpler and correct for 1D.
- **Single file format** — Lucene uses three files (`.poi`, `.idx`, `.kdd`). We use one `.kd` file for simplicity.
- **No prefix compression** — Lucene prefix-codes split values and leaf data. We store raw values. Compression can be added later if needed.
