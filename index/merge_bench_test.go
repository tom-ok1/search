package index

import (
	"fmt"
	"runtime"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

// memStats captures heap memory at a point in time.
func memStats() runtime.MemStats {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func reportMemory(b *testing.B, before, after runtime.MemStats) {
	heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)
	b.ReportMetric(float64(after.HeapInuse)/(1024*1024), "heap-MB")
	b.ReportMetric(float64(heapDelta)/(1024*1024), "heap-delta-MB")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/(1024*1024), "total-alloc-MB")
}

// buildCommittedSegments creates an IndexWriter with n documents committed
// across multiple segments of segSize docs each.
func buildCommittedSegments(b *testing.B, dir store.Directory, n, segSize int) *IndexWriter {
	b.Helper()
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := NewIndexWriter(dir, fa, segSize)

	for i := range n {
		doc := makeBenchDoc(i)
		if err := w.AddDocument(doc); err != nil {
			b.Fatal(err)
		}
		if (i+1)%segSize == 0 {
			if err := w.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	// Commit any remaining docs
	if n%segSize != 0 {
		if err := w.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return w
}

// --- Large-scale indexing benchmarks ---

func BenchmarkLargeIndex(b *testing.B) {
	for _, n := range []int{10_000, 50_000, 100_000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
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
				w := NewIndexWriter(dir, fa, 5000)
				before := memStats()
				b.StartTimer()

				for j := range n {
					if err := w.AddDocument(makeBenchDoc(j)); err != nil {
						b.Fatal(err)
					}
				}
				if err := w.Commit(); err != nil {
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

// --- ForceMerge benchmarks ---

func BenchmarkForceMerge(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, err := store.NewFSDirectory(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				// Create many small segments to force merge work
				w := buildCommittedSegments(b, dir, n, 1000)
				before := memStats()
				b.StartTimer()

				if err := w.ForceMerge(1); err != nil {
					b.Fatal(err)
				}
				if err := w.Commit(); err != nil {
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

// --- AutoMerge during sustained indexing ---

func BenchmarkAutoMerge(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
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
				w := NewIndexWriter(dir, fa, 1000)
				w.SetMergePolicy(NewTieredMergePolicy())
				before := memStats()
				b.StartTimer()

				for j := range n {
					if err := w.AddDocument(makeBenchDoc(j)); err != nil {
						b.Fatal(err)
					}
					// Commit every 1000 docs to trigger auto-merge checks
					if (j+1)%1000 == 0 {
						if err := w.Commit(); err != nil {
							b.Fatal(err)
						}
					}
				}
				if err := w.Commit(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				after := memStats()
				reportMemory(b, before, after)
				b.ReportMetric(float64(len(w.segmentInfos.Segments)), "segments")
				w.Close()
				b.StartTimer()
			}
		})
	}
}

// --- Direct MergeSegmentsToDisk benchmark ---

func BenchmarkMergeSegmentsToDisk(b *testing.B) {
	for _, numSegments := range []int{2, 5, 10} {
		for _, docsPerSeg := range []int{1000, 5000} {
			name := fmt.Sprintf("Segs_%d_Docs_%d", numSegments, docsPerSeg)
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
					// Create numSegments separate segments
					for s := range numSegments {
						for j := range docsPerSeg {
							docID := s*docsPerSeg + j
							if err := w.AddDocument(makeBenchDoc(docID)); err != nil {
								b.Fatal(err)
							}
						}
						if err := w.Commit(); err != nil {
							b.Fatal(err)
						}
					}

					// Prepare merge inputs
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

// --- Merge with deletions ---

func BenchmarkForceMergeWithDeletions(b *testing.B) {
	for _, deletePct := range []int{10, 30, 50} {
		b.Run(fmt.Sprintf("Delete_%dpct", deletePct), func(b *testing.B) {
			const totalDocs = 10_000
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, err := store.NewFSDirectory(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				w := buildCommittedSegments(b, dir, totalDocs, 1000)

				// Delete a percentage of docs
				numDeletes := totalDocs * deletePct / 100
				for j := range numDeletes {
					tag := fmt.Sprintf("document number %d about search engines", j)
					w.DeleteDocuments("title", tag)
				}
				if err := w.Commit(); err != nil {
					b.Fatal(err)
				}

				before := memStats()
				b.StartTimer()

				if err := w.ForceMerge(1); err != nil {
					b.Fatal(err)
				}
				if err := w.Commit(); err != nil {
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

// --- Large document benchmark (richer content) ---

func makeLargeBenchDoc(id int) *document.Document {
	doc := document.NewDocument()
	// Text fields
	doc.AddField("title", fmt.Sprintf("comprehensive document number %d covering search engine design patterns and implementation strategies", id), document.FieldTypeText)
	doc.AddField("body", fmt.Sprintf(
		"this is an extended body for document %d discussing full text search indexing strategies "+
			"including inverted index construction merge policies segment management query optimization "+
			"boolean retrieval phrase matching term frequency inverse document frequency scoring algorithms "+
			"relevance ranking document at a time scoring and skip list based posting list intersection "+
			"methods for efficient large scale information retrieval systems and search applications number %d",
		id, id), document.FieldTypeText)
	// Keyword fields
	doc.AddField("category", fmt.Sprintf("category_%d", id%20), document.FieldTypeKeyword)
	doc.AddField("tag", fmt.Sprintf("tag_%d", id%100), document.FieldTypeKeyword)
	// Stored field
	doc.AddField("raw_content", fmt.Sprintf("raw-content-for-document-%d-with-extra-data", id), document.FieldTypeStored)
	// Numeric doc values
	doc.AddNumericDocValuesField("popularity", int64(id%1000))
	// Sorted doc values
	doc.AddSortedDocValuesField("author", fmt.Sprintf("author_%d", id%50))
	// LongPoint
	doc.AddLongPoint("created_at", int64(1700000000+id))
	// DoublePoint
	doc.AddDoublePoint("rating", float64(id%50)*0.1)
	return doc
}

func BenchmarkLargeDocIndex(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
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
				w := NewIndexWriter(dir, fa, 5000)
				w.SetMergePolicy(NewTieredMergePolicy())
				before := memStats()
				b.StartTimer()

				for j := range n {
					if err := w.AddDocument(makeLargeBenchDoc(j)); err != nil {
						b.Fatal(err)
					}
					if (j+1)%5000 == 0 {
						if err := w.Commit(); err != nil {
							b.Fatal(err)
						}
					}
				}
				if err := w.Commit(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				after := memStats()
				reportMemory(b, before, after)
				b.ReportMetric(float64(len(w.segmentInfos.Segments)), "segments")
				w.Close()
				b.StartTimer()
			}
		})
	}
}
