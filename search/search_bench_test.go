package search

import (
	"fmt"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/store"
)

// buildSearchIndex creates a committed index with n documents for benchmarking.
func buildSearchIndex(b *testing.B, n int) *index.IndexReader {
	b.Helper()
	dir, err := store.NewFSDirectory(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	fa := analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter()),
	)
	w := index.NewIndexWriter(dir, fa, n)

	words := []string{"search", "engine", "index", "query", "document", "score", "rank", "filter", "match", "result"}
	for i := range n {
		doc := document.NewDocument()
		title := fmt.Sprintf("title with %s and %s", words[i%len(words)], words[(i+3)%len(words)])
		body := fmt.Sprintf("body text about %s and %s plus some extra words for length to make scoring interesting for the benchmark test suite", words[(i+1)%len(words)], words[(i+5)%len(words)])
		doc.AddField("title", title, document.FieldTypeText)
		doc.AddField("body", body, document.FieldTypeText)
		if err := w.AddDocument(doc); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		b.Fatal(err)
	}
	w.Close()

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { reader.Close() })
	return reader
}

var sinkResults []SearchResult

func BenchmarkTermQuery(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			reader := buildSearchIndex(b, n)
			searcher := NewIndexSearcher(reader)
			q := NewTermQuery("body", "search")

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c := NewTopKCollector(10)
				sinkResults = searcher.Search(q, c)
			}
		})
	}
}

func BenchmarkPhraseQuery(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			reader := buildSearchIndex(b, n)
			searcher := NewIndexSearcher(reader)
			q := NewPhraseQuery("body", "extra", "words")

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c := NewTopKCollector(10)
				sinkResults = searcher.Search(q, c)
			}
		})
	}
}

func BenchmarkBooleanQuery(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			reader := buildSearchIndex(b, n)
			searcher := NewIndexSearcher(reader)
			q := NewBooleanQuery().
				Add(NewTermQuery("title", "search"), OccurMust).
				Add(NewTermQuery("body", "index"), OccurShould)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c := NewTopKCollector(10)
				sinkResults = searcher.Search(q, c)
			}
		})
	}
}

func BenchmarkMatchAllQuery(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("Docs_%d", n), func(b *testing.B) {
			reader := buildSearchIndex(b, n)
			searcher := NewIndexSearcher(reader)
			q := NewMatchAllQuery()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c := NewTopKCollector(10)
				sinkResults = searcher.Search(q, c)
			}
		})
	}
}
