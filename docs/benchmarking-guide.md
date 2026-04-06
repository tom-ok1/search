# Benchmarking Guide

This document explains how to run the benchmark suites and interpret the results to identify scalability issues in the indexing and merging pipeline.

## Benchmark Files

| File | Purpose |
|---|---|
| `analysis/analyzer_bench_test.go` | Tokenizer, filter, and analyzer throughput |
| `index/writer_bench_test.go` | Single-document indexing and small batch commit |
| `index/merge_bench_test.go` | Large-scale indexing, force merge, auto merge, merge with deletions |
| `index/scale_bench_test.go` | Scalability tests: sustained throughput, memory stability, segment count growth, concurrent indexing |
| `search/search_bench_test.go` | Query performance (TermQuery, PhraseQuery, BooleanQuery, MatchAllQuery, PointRangeQuery), sorted search (numeric/string/multi-field), stored field retrieval |

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
