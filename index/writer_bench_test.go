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
	doc.AddField("title", fmt.Sprintf("document number %d about search engines", id), document.FieldTypeText)
	doc.AddField("body", fmt.Sprintf("this is the body of document %d it contains several words about indexing and searching through text content for relevant results", id), document.FieldTypeText)
	doc.AddField("tag", "benchmark", document.FieldTypeKeyword)
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
