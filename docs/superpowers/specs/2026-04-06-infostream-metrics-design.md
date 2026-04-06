# InfoStream & Metrics Design

Diagnostic instrumentation for the GoSearch indexing pipeline, modeled after Lucene's `InfoStream` with an additional always-on metrics layer.

## Motivation

Benchmark results (2026-04-06) show:
- Concurrent indexing degrades at 4+ goroutines (14K → 4.9K docs/sec)
- Segment count grows unbounded in sustained throughput tests (5 → 19)
- 2M-doc MemoryStability benchmark times out at 600s

There is currently no way to diagnose these issues without external profiling. Lucene solves this with `InfoStream` (185 log points across 16 files). GoSearch needs equivalent observability.

## Design: Two-Layer Architecture

### Layer 1: Metrics (always ON)

`IndexWriterMetrics` aggregates atomic counters and gauges. Cost is one `atomic.Int64.Add()` per measurement point (~2ns).

```go
type IndexWriterMetrics struct {
    // Flush
    FlushCount     atomic.Int64 // total flush executions
    FlushBytes     atomic.Int64 // cumulative bytes flushed
    FlushTimeNanos atomic.Int64 // cumulative flush duration

    // Stall
    StallCount     atomic.Int64 // stall occurrences
    StallTimeNanos atomic.Int64 // cumulative stall wait time

    // Merge
    MergeCount     atomic.Int64 // merge executions
    MergeDocCount  atomic.Int64 // cumulative docs merged
    MergeTimeNanos atomic.Int64 // cumulative merge duration

    // Documents
    DocsAdded   atomic.Int64 // documents added
    DocsDeleted atomic.Int64 // documents deleted

    // Current state (gauges)
    ActiveBytes       atomic.Int64 // RAM in actively-indexing DWPTs
    FlushPendingBytes atomic.Int64 // RAM in DWPTs pending flush
    SegmentCount      atomic.Int64 // current segment count
}
```

Accessed via `IndexWriter.Metrics() *IndexWriterMetrics`. A single `*IndexWriterMetrics` pointer is shared from `IndexWriter` → `DocumentsWriter` → `FlushControl`.

### Layer 2: InfoStream (default OFF)

Lucene-compatible diagnostic log interface. Zero cost when disabled (`IsEnabled()` returns false, no string formatting occurs).

```go
type InfoStream interface {
    Message(component string, message string)
    IsEnabled(component string) bool
}
```

Built-in implementations:

| Type | Description |
|---|---|
| `NoOpInfoStream` | Default. `IsEnabled()` always returns false |
| `PrintInfoStream` | Writes to an `io.Writer` with timestamp + component prefix |

Enabled via `IndexWriter.SetInfoStream(stream)`.

#### Component codes

| Code | Source | What it logs |
|---|---|---|
| `"IW"` | IndexWriter | merge start/end, commit start/end, maybeMerge candidates |
| `"DW"` | DocumentsWriter | stall start/end with memory state and duration |
| `"DWFC"` | FlushControl | flush trigger with RAM threshold comparison |
| `"DWPT"` | DWPT | per-DWPT flush: doc count, size, duration |
| `"MP"` | MergePolicy | segment count, selected merge candidates |
| `"IFD"` | FileDeleter | file deletion with refcount |

#### Output format

```
2026-04-06T12:34:56.789Z IW: merging _0(50000 docs) + _1(50000 docs)
2026-04-06T12:34:57.123Z IW: merge done: 100000 docs, took 334ms
2026-04-06T12:34:57.124Z DW: now stalling: activeBytes=128.5 MB flushBytes=96.2 MB
2026-04-06T12:34:57.458Z DW: done stalling for 334.0 ms
```

## Instrumentation Points

### Priority 1: FlushControl (concurrency bottleneck core)

| Method | Metrics | InfoStream |
|---|---|---|
| `doAfterDocument()` | `ActiveBytes` gauge update | `DWFC: flush triggered: ramBytes=X MB > limit=Y MB` |
| `waitIfStalled()` | `StallCount++`, `StallTimeNanos += elapsed` | `DW: stall start/end with memory state and duration` |
| `doAfterFlush()` | `FlushPendingBytes` gauge update | (none) |

### Priority 2: DocumentsWriter / DWPT

| Method | Metrics | InfoStream |
|---|---|---|
| `addDocument()` | `DocsAdded++` | (none — too high frequency) |
| `doFlush()` | `FlushCount++`, `FlushBytes += size`, `FlushTimeNanos += elapsed` | `DWPT: flush _2: 10000 docs, 25.6 MB, took 342ms` |

### Priority 3: IndexWriter

| Method | Metrics | InfoStream |
|---|---|---|
| `executeMerge()` | `MergeCount++`, `MergeDocCount += docs`, `MergeTimeNanos += elapsed` | `IW: merging .../merge done with doc count and duration` |
| `Commit()` | (none) | `IW: commit start/done with segment count and duration` |
| `DeleteDocuments()` | `DocsDeleted++` | (none) |
| `MaybeMerge()` | (none) | `IW: maybeMerge: N candidates found` |

### Priority 4: MergePolicy / FileDeleter

| Method | Metrics | InfoStream |
|---|---|---|
| `FindMerges()` | (none) | `MP: N segments, selected [...] for merge` |
| `deleteIfUnreferenced()` | (none) | `IFD: delete _0.seg: refcount=0` |

## Propagation

`InfoStream` and `Metrics` are owned by `IndexWriter` and passed down to child components:

```
IndexWriter (infoStream, metrics)
  └─ DocumentsWriter (infoStream, metrics)
       ├─ FlushControl (infoStream, metrics)
       └─ DWPT (infoStream)
```

MergePolicy does NOT receive InfoStream directly. Merge-related logging is done by `IndexWriter.MaybeMerge()` and `executeMerge()`, consistent with Lucene's design.

## File Changes

### New files

| File | Content |
|---|---|
| `index/infostream.go` | `InfoStream` interface, `NoOpInfoStream`, `PrintInfoStream` |
| `index/metrics.go` | `IndexWriterMetrics` struct |

### Modified files

| File | Changes |
|---|---|
| `index/writer.go` | Add `infoStream`, `metrics` fields. Add `SetInfoStream()`, `Metrics()`. Instrument `executeMerge()`, `Commit()`, `MaybeMerge()` |
| `index/documents_writer.go` | Accept `infoStream`/`metrics` in constructor, pass to children. Instrument `doFlush()` |
| `index/flush_control.go` | Add `infoStream`/`metrics` fields. Instrument `doAfterDocument()`, `waitIfStalled()`, `doAfterFlush()` |
| `index/dwpt.go` | Add `infoStream` field. Instrument `flush()` |
| `index/tiered_merge_policy.go` | No changes. Logging done at IndexWriter level |

### Not changed

- `InMemorySegment` — lock-free write buffer; no overhead added
- `merger.go` — merge timing measured by caller (`executeMerge`)
- `delete_queue.go` — low priority; can be added later
- Public API — `NewIndexWriter()` signature unchanged; `SetInfoStream()` is optional

## Testing

### CapturingInfoStream (test helper)

```go
type CapturingInfoStream struct {
    mu       sync.Mutex
    messages []string
    enabled  map[string]bool
}
```

### Test cases

**InfoStream tests:**
- `TestInfoStreamDisabledByDefault` — no output with default NoOpInfoStream
- `TestInfoStreamFlushMessages` — DWPT messages on flush
- `TestInfoStreamStallMessages` — DW messages on stall start/end
- `TestInfoStreamMergeMessages` — IW messages on merge
- `TestInfoStreamComponentFiltering` — only enabled components produce output

**Metrics tests:**
- `TestMetricsDocsAdded` — correct count after adding documents
- `TestMetricsFlushCount` — increments after Commit
- `TestMetricsMergeCount` — increments when merge occurs
- `TestMetricsStallTracking` — StallCount and StallTimeNanos updated on stall
- `TestMetricsConcurrentAccess` — accurate counts under concurrent indexing

### Benchmark integration

Existing scale benchmarks (`scale_bench_test.go`) gain Metrics output:

```go
m := w.Metrics()
b.ReportMetric(float64(m.StallCount.Load()), "stalls")
b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
b.ReportMetric(float64(m.MergeCount.Load()), "merges")
b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
```

This makes stall frequency and merge pressure visible in `go test -bench=` output without any code changes to the benchmarks themselves beyond appending these lines.
