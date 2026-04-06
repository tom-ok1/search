package index

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/store"
)

func newBenchWriter(b *testing.B, bufferSize int) *IndexWriter {
	b.Helper()
	dir, err := store.NewFSDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	return NewIndexWriter(dir, fa, bufferSize)
}

func makeBenchDoc(id int) *document.Document {
	doc := document.NewDocument()
	// Text fields (inverted index)
	doc.AddField("title", fmt.Sprintf("document number %d about search engines", id), document.FieldTypeText)
	doc.AddField("body", fmt.Sprintf("this is the body of document %d it contains several words about indexing and searching through text content for relevant results", id), document.FieldTypeText)
	// Keyword field
	doc.AddField("tag", "benchmark", document.FieldTypeKeyword)
	// Stored field
	doc.AddField("source", fmt.Sprintf("source-%d", id), document.FieldTypeStored)
	// Numeric doc values (for sorting)
	doc.AddNumericDocValuesField("priority", int64(id%100))
	// Sorted doc values (for sorting)
	doc.AddSortedDocValuesField("category", fmt.Sprintf("cat_%d", id%20))
	// LongPoint (for range queries)
	doc.AddLongPoint("timestamp", int64(1000000+id))
	// DoublePoint (for range queries)
	doc.AddDoublePoint("score", float64(id)*0.1)
	return doc
}

func BenchmarkIndexWriter_AddDocument(b *testing.B) {
	w := newBenchWriter(b, 1000)
	defer w.Close()

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if err := w.AddDocument(makeBenchDoc(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexWriter_BulkIndex(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				w := newBenchWriter(b, n)
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
				w.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkIndexWriter_Commit(b *testing.B) {
	w := newBenchWriter(b, 1000)
	defer w.Close()

	// Pre-index some docs
	for i := range 100 {
		w.AddDocument(makeBenchDoc(i))
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// Add a doc then commit to measure commit overhead
		w.AddDocument(makeBenchDoc(100 + i))
		if err := w.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
