# Benchmarking Guide

This document explains how to run the benchmark suites and interpret the results to identify scalability issues in the indexing and merging pipeline.

## Benchmark Files

| File | Purpose |
|---|---|
| `analysis/analyzer_bench_test.go` | Tokenizer, filter, and analyzer throughput |
| `index/writer_bench_test.go` | Single-document indexing and small batch commit |
| `index/merge_bench_test.go` | Large-scale indexing, force merge, auto merge, merge with deletions |
| `index/scale_bench_test.go` | Scalability tests: sustained throughput, memory stability, segment count growth, concurrent indexing |
| `search/search_bench_test.go` | Query performance (TermQuery, PhraseQuery, BooleanQuery, MatchAllQuery) |

## Quick Start

Run all benchmarks for a specific package:

```bash
go test ./index/ -bench=. -benchmem -count=1 -timeout=600s
```

Run a specific benchmark:

```bash
go test ./index/ -bench=BenchmarkMemoryStability -benchmem -count=1 -timeout=600s
```

> **Note:** Scale benchmarks (`scale_bench_test.go`) can take several minutes. Use `-timeout=600s` or higher.

## Custom Metrics

The benchmarks report custom metrics beyond the standard `ns/op`, `B/op`, and `allocs/op`:

| Metric | Meaning |
|---|---|
| `heap-MB` | Heap in use at measurement point |
| `heap-delta-MB` | Change in heap from before to after the measured operation |
| `total-alloc-MB` | Total bytes allocated (includes GC'd memory) |
| `peak-heap-MB` | Maximum heap observed during the benchmark |
| `final-heap-MB` | Heap after all operations complete and GC runs |
| `final-segments` | Number of segments at the end of indexing |
| `segments` | Number of segments at measurement point |
| `docs/sec` | Indexing throughput |
| `segments@{N}K` | Segment count at N thousand documents |
| `heap-MB@{N}K` | Heap at N thousand documents |
| `heap-MB@batch{N}` | Heap at batch N (each batch = 50K docs) |

## Identifying Specific Issues

### Issue 1: Unbounded Memory Growth

**Which benchmark:** `BenchmarkMemoryStability`

```bash
go test ./index/ -bench=BenchmarkMemoryStability -benchmem -count=1 -timeout=600s
```

**What to look for:** The `heap-MB@{N}K` metrics should plateau after the initial ramp-up. If they keep growing linearly with document count, memory is leaking.

**Healthy behavior:**
```
heap-MB@200K   ~500
heap-MB@400K   ~500
heap-MB@600K   ~500   # stays flat
```

**Problematic behavior (current):**
```
heap-MB@200K    526
heap-MB@400K   1050
heap-MB@600K   1574   # linear growth — ~2.5 MB per 1K docs
heap-MB@800K   2081
heap-MB@1000K  2593
```

**Root cause investigation:** After identifying growth, check:
- Whether `readerMap` entries are cleaned up after merge (`writer.go:executeMerge`)
- Whether `DiskSegment` mmap handles are released when segments are merged away
- Whether `ReadersAndUpdates` holds references to flushed segment data

### Issue 2: Segment Count Not Bounded

**Which benchmark:** `BenchmarkSegmentCountScaling`

```bash
go test ./index/ -bench=BenchmarkSegmentCountScaling -benchmem -count=1 -timeout=600s
```

**What to look for:** The `segments@{N}K` metric should stay below a reasonable upper bound (e.g., 20-30 segments) regardless of total document count.

**Healthy behavior:**
```
segments@10K     2
segments@100K   10
segments@500K   10    # bounded
segments@1000K  10
```

**Problematic behavior:** Segment count growing without bound means the merge policy cannot keep up, or `MaxMergedSegmentDocs` is preventing large segments from being merged.

**Key parameter:** `TieredMergePolicy.MaxMergedSegmentDocs` (default: 100,000). Segments with more live docs than this value are never eligible for merging. At 100M total docs, this creates a floor of ~1,000 segments.

To test with a higher limit:
```go
policy := NewTieredMergePolicy()
policy.MaxMergedSegmentDocs = 5_000_000  // allow merging up to 5M-doc segments
w.SetMergePolicy(policy)
```

### Issue 3: Merge Allocation Pressure

**Which benchmark:** `BenchmarkLargeSegmentMerge` or `BenchmarkMergeSegmentsToDisk`

```bash
go test ./index/ -bench=BenchmarkLargeSegmentMerge -benchmem -count=1 -timeout=600s
```

**What to look for:** Compare `total-alloc-MB` against the actual data size. If total-alloc is 10-20x the document count, the merge is creating excessive intermediate objects.

**Key metrics:**
- `heap-delta-MB` ≈ 0 means the merge itself streams correctly (good).
- `total-alloc-MB` being very large means many short-lived objects are created and GC'd (bad for throughput).

**Example (current):**
```
Segs_5_x_100K:  total-alloc = 9,661 MB for 500K docs
                → ~19 MB per 1K docs of merge allocation
```

**Root cause:** `Posting` structs and position slices are allocated per-term per-document during merge. Look at `merger.go:mergeFieldPostingsToDisk` — the `postings` slice is reused across terms but individual `Posting.Positions` slices are not pooled.

### Issue 4: Concurrent Indexing Doesn't Scale

**Which benchmark:** `BenchmarkConcurrentIndex`

```bash
go test ./index/ -bench=BenchmarkConcurrentIndex -benchmem -count=1 -timeout=300s
```

**What to look for:** `docs/sec` should increase (or at least stay constant) as goroutine count grows. If it drops, there is lock contention.

**Healthy behavior:**
```
Goroutines_1:  52,000 docs/sec
Goroutines_2:  90,000 docs/sec   # scales up
Goroutines_4: 150,000 docs/sec
```

**Problematic behavior (current):**
```
Goroutines_1:  52,023 docs/sec
Goroutines_2:  54,927 docs/sec   # barely improves
Goroutines_4:  25,951 docs/sec   # halves!
Goroutines_8:  22,853 docs/sec
```

**Root cause investigation:** Profile with `go test -cpuprofile`:
```bash
go test ./index/ -bench=BenchmarkConcurrentIndex/Goroutines_8 -cpuprofile=cpu.prof -timeout=300s
go tool pprof cpu.prof
# In pprof: top 20, or web for flamegraph
```

Likely contention points:
- `FlushControl.mu` — taken on every `doAfterDocument` call
- `IndexWriter.mu` — taken during commit and merge
- `perThreadPool` lock — taken when getting/returning DWPTs

### Issue 5: Throughput Degradation Over Time

**Which benchmark:** `BenchmarkSustainedThroughput`

```bash
go test ./index/ -bench='BenchmarkSustainedThroughput/Total_1M' -benchmem -count=1 -timeout=600s
```

**What to look for:** Compare `docs/sec` across runs at different scales. Also check whether `segments@batch{N}` keeps growing — more segments means more merge work per commit, which slows down indexing.

**Healthy behavior:** `docs/sec` stays roughly constant across batch numbers.

**Problematic behavior:** If later batches show significantly lower throughput, the merge work per commit is growing. This can be diagnosed by correlating `segments@batch{N}` with wall-clock time.

### Issue 6: Force Merge Cost

**Which benchmark:** `BenchmarkForceMerge`

```bash
go test ./index/ -bench=BenchmarkForceMerge -benchmem -count=1 -timeout=600s
```

**What to look for:** Time and `total-alloc-MB` scaling. Force merge is O(total docs) in the best case. If it scales worse than linearly, something is wrong.

### Issue 7: Delete-Heavy Merge Efficiency

**Which benchmark:** `BenchmarkForceMergeWithDeletions`

```bash
go test ./index/ -bench=BenchmarkForceMergeWithDeletions -benchmem -count=1 -timeout=300s
```

**What to look for:** If merge time and allocations are the same at 10% and 50% deletion rates, the merge is not skipping deleted documents efficiently. The merge reads all postings regardless of deletion state.

**Current behavior:** All deletion rates produce identical results (~227ms, 146 MB alloc for 10K docs). This confirms that deleted documents are still fully traversed during merge.

## Profiling

### CPU Profile

```bash
go test ./index/ -bench=BenchmarkSustainedThroughput/Total_1M \
  -cpuprofile=cpu.prof -timeout=600s
go tool pprof -http=:8080 cpu.prof
```

### Memory Profile

```bash
go test ./index/ -bench=BenchmarkMemoryStability \
  -memprofile=mem.prof -timeout=600s
go tool pprof -http=:8080 mem.prof
```

In pprof, use `top`, `list <function>`, or `web` to find allocation hotspots.

### Trace

For understanding goroutine scheduling and GC pauses:

```bash
go test ./index/ -bench=BenchmarkConcurrentIndex/Goroutines_8 \
  -trace=trace.out -timeout=300s
go tool trace trace.out
```

## Running the Full Suite

To run all benchmarks and save results for comparison:

```bash
# Run and save
go test ./analysis/ ./index/ ./search/ \
  -bench=. -benchmem -count=3 -timeout=1800s \
  | tee bench-$(date +%Y%m%d).txt

# Compare two runs (requires benchstat)
go install golang.org/x/perf/cmd/benchstat@latest
benchstat bench-before.txt bench-after.txt
```

## Summary of Issue Detection

| Issue | Benchmark | Key Metric | Failure Signal |
|---|---|---|---|
| Memory leak | `MemoryStability` | `heap-MB@{N}K` | Linear growth, no plateau |
| Too many segments | `SegmentCountScaling` | `segments@{N}K` | Unbounded growth |
| GC pressure in merge | `LargeSegmentMerge` | `total-alloc-MB` | >> 10x data size |
| Concurrency bottleneck | `ConcurrentIndex` | `docs/sec` | Drops with more goroutines |
| Throughput degradation | `SustainedThroughput` | `docs/sec`, `segments@batch{N}` | Declining throughput |
| Merge ignores deletes | `ForceMergeWithDeletions` | time across delete rates | Identical regardless of rate |
