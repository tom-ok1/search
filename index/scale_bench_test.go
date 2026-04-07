package index

import (
	"fmt"
	"runtime"
	"testing"

	"gosearch/analysis"
	"gosearch/store"
)

// --- Sustained throughput benchmark ---
// Indexes in batches and reports per-batch throughput, segment count, and memory.
// This reveals throughput degradation and memory growth trends at scale.

func BenchmarkSustainedThroughput(b *testing.B) {
	for _, totalDocs := range []int{1_000_000, 5_000_000} {
		b.Run(fmt.Sprintf("Total_%dM", totalDocs/1_000_000), func(b *testing.B) {
			const batchSize = 50_000
			const commitInterval = 10_000

			dir, err := store.NewFSDirectory(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			fa := analysis.NewFieldAnalyzers(
				analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
			)
			w := NewIndexWriter(dir, fa, commitInterval)
			w.SetMergePolicy(NewTieredMergePolicy())
			defer w.Close()

			b.ReportAllocs()
			b.ResetTimer()

			for i := range totalDocs {
				if err := w.AddDocument(makeBenchDoc(i)); err != nil {
					b.Fatal(err)
				}
				if (i+1)%commitInterval == 0 {
					if err := w.Commit(); err != nil {
						b.Fatal(err)
					}
				}
				// Report per-batch metrics
				if (i+1)%batchSize == 0 {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					batchNum := (i + 1) / batchSize
					b.ReportMetric(float64(m.HeapInuse)/(1024*1024), fmt.Sprintf("heap-MB@batch%d", batchNum))
					b.ReportMetric(float64(len(w.segmentInfos.Segments)), fmt.Sprintf("segments@batch%d", batchNum))
				}
			}
			if err := w.Commit(); err != nil {
				b.Fatal(err)
			}

			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			b.ReportMetric(float64(m.HeapInuse)/(1024*1024), "final-heap-MB")
			b.ReportMetric(float64(len(w.segmentInfos.Segments)), "final-segments")
			b.ReportMetric(float64(totalDocs)/b.Elapsed().Seconds(), "docs/sec")
			metrics := w.Metrics()
			b.ReportMetric(float64(metrics.StallCount.Load()), "stalls")
			b.ReportMetric(float64(metrics.StallTimeNanos.Load())/1e6, "stall-ms")
			b.ReportMetric(float64(metrics.MergeCount.Load()), "merges")
			b.ReportMetric(float64(metrics.FlushCount.Load()), "flushes")
		})
	}
}

// --- Segment count scaling benchmark ---
// Verifies that segment count stays bounded as document count grows.

func BenchmarkSegmentCountScaling(b *testing.B) {
	dir, err := store.NewFSDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 5000)
	w.SetMergePolicy(NewTieredMergePolicy())
	defer w.Close()

	const totalDocs = 1_000_000
	const commitInterval = 5000
	checkpoints := []int{10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000}
	checkIdx := 0

	b.ResetTimer()
	for i := range totalDocs {
		if err := w.AddDocument(makeBenchDoc(i)); err != nil {
			b.Fatal(err)
		}
		if (i+1)%commitInterval == 0 {
			if err := w.Commit(); err != nil {
				b.Fatal(err)
			}
		}
		if checkIdx < len(checkpoints) && (i+1) == checkpoints[checkIdx] {
			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			b.ReportMetric(float64(len(w.segmentInfos.Segments)), fmt.Sprintf("segments@%dK", checkpoints[checkIdx]/1000))
			b.ReportMetric(float64(m.HeapInuse)/(1024*1024), fmt.Sprintf("heap-MB@%dK", checkpoints[checkIdx]/1000))
			checkIdx++
		}
	}
}

// --- Memory stability benchmark ---
// Indexes a large number of docs and verifies heap does not grow unbounded.

func BenchmarkMemoryStability(b *testing.B) {
	dir, err := store.NewFSDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, 10_000)
	w.SetMergePolicy(NewTieredMergePolicy())
	defer w.Close()

	const totalDocs = 2_000_000
	const commitInterval = 10_000
	const sampleInterval = 200_000

	var peakHeap uint64

	b.ResetTimer()
	for i := range totalDocs {
		if err := w.AddDocument(makeBenchDoc(i)); err != nil {
			b.Fatal(err)
		}
		if (i+1)%commitInterval == 0 {
			if err := w.Commit(); err != nil {
				b.Fatal(err)
			}
		}
		if (i+1)%sampleInterval == 0 {
			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peakHeap {
				peakHeap = m.HeapInuse
			}
			b.ReportMetric(float64(m.HeapInuse)/(1024*1024), fmt.Sprintf("heap-MB@%dK", (i+1)/1000))
		}
	}
	b.ReportMetric(float64(peakHeap)/(1024*1024), "peak-heap-MB")
	b.ReportMetric(float64(len(w.segmentInfos.Segments)), "final-segments")
	m := w.Metrics()
	b.ReportMetric(float64(m.StallCount.Load()), "stalls")
	b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
	b.ReportMetric(float64(m.MergeCount.Load()), "merges")
	b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
}

// --- Large segment merge benchmark ---
// Tests merging segments with 100K+ docs each, which is the boundary
// where TieredMergePolicy stops merging (MaxMergedSegmentDocs=5M).

func BenchmarkLargeSegmentMerge(b *testing.B) {
	for _, docsPerSeg := range []int{50_000, 100_000} {
		for _, numSegs := range []int{2, 5} {
			name := fmt.Sprintf("Segs_%d_x_%dK", numSegs, docsPerSeg/1000)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					dir, err := store.NewFSDirectory(b.TempDir())
					if err != nil {
						b.Fatal(err)
					}
					fa := analysis.NewFieldAnalyzers(
						analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
					)
					w := NewIndexWriter(dir, fa, docsPerSeg)
					for s := range numSegs {
						for j := range docsPerSeg {
							if err := w.AddDocument(makeBenchDoc(s*docsPerSeg + j)); err != nil {
								b.Fatal(err)
							}
						}
						if err := w.Commit(); err != nil {
							b.Fatal(err)
						}
					}

					inputs := make([]MergeInput, len(w.segmentInfos.Segments))
					for idx, info := range w.segmentInfos.Segments {
						rau := w.getOrCreateRAU(info)
						reader, err := rau.getReader()
						if err != nil {
							b.Fatal(err)
						}
						inputs[idx] = MergeInput{
							Segment:   reader,
							IsDeleted: rau.IsDeleted,
						}
					}

					before := memStats()
					b.StartTimer()

					_, err = MergeSegmentsToDisk(dir, inputs, "_merged")
					if err != nil {
						b.Fatal(err)
					}

					b.StopTimer()
					after := memStats()
					reportMemory(b, before, after)
					w.Close()
					b.StartTimer()
				}
			})
		}
	}
}

// --- Concurrent indexing benchmark ---
// Tests throughput with multiple goroutines writing simultaneously.

func BenchmarkConcurrentIndex(b *testing.B) {
	for _, goroutines := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("Goroutines_%d", goroutines), func(b *testing.B) {
			const totalDocs = 100_000
			docsPerGoroutine := totalDocs / goroutines

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, err := store.NewFSDirectory(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				fa := analysis.NewFieldAnalyzers(
					analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
				)
				w := NewIndexWriter(dir, fa, 10_000)
				w.SetMergePolicy(NewTieredMergePolicy())

				before := memStats()
				b.StartTimer()

				errs := make(chan error, goroutines)
				for g := range goroutines {
					go func(offset int) {
						for j := range docsPerGoroutine {
							if err := w.AddDocument(makeBenchDoc(offset + j)); err != nil {
								errs <- err
								return
							}
						}
						errs <- nil
					}(g * docsPerGoroutine)
				}
				for range goroutines {
					if err := <-errs; err != nil {
						b.Fatal(err)
					}
				}
				if err := w.Commit(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				after := memStats()
				reportMemory(b, before, after)
				b.ReportMetric(float64(totalDocs)/b.Elapsed().Seconds(), "docs/sec")
				b.ReportMetric(float64(len(w.segmentInfos.Segments)), "segments")
				m := w.Metrics()
				b.ReportMetric(float64(m.StallCount.Load()), "stalls")
				b.ReportMetric(float64(m.StallTimeNanos.Load())/1e6, "stall-ms")
				b.ReportMetric(float64(m.MergeCount.Load()), "merges")
				b.ReportMetric(float64(m.FlushCount.Load()), "flushes")
				w.Close()
				b.StartTimer()
			}
		})
	}
}
