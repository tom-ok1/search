package action

import (
	"encoding/json"
	"testing"

	"gosearch/analysis"
	"gosearch/index"
	"gosearch/search"
	"gosearch/server/mapping"
	"gosearch/store"
)

func TestRangeQuery_EndToEnd(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title":  {Type: mapping.FieldTypeText},
			"price":  {Type: mapping.FieldTypeLong},
			"rating": {Type: mapping.FieldTypeDouble},
		},
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stdAnalyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter())
	fieldAnalyzers := analysis.NewFieldAnalyzers(stdAnalyzer)
	writer := index.NewIndexWriter(dir, fieldAnalyzers, 1024*1024)

	docs := []struct {
		id     string
		source string
	}{
		{"1", `{"title":"cheap widget","price":10,"rating":3.5}`},
		{"2", `{"title":"mid widget","price":50,"rating":4.0}`},
		{"3", `{"title":"premium widget","price":100,"rating":4.5}`},
		{"4", `{"title":"luxury widget","price":200,"rating":5.0}`},
	}

	for _, d := range docs {
		doc, err := mapping.ParseDocument(d.id, []byte(d.source), m)
		if err != nil {
			t.Fatalf("parse doc %s: %v", d.id, err)
		}
		if err := writer.AddDocument(doc); err != nil {
			t.Fatalf("index doc %s: %v", d.id, err)
		}
	}

	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	searcher := search.NewIndexSearcher(reader)
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	t.Run("long range filter", func(t *testing.T) {
		q, err := parser.ParseQuery(QueryJSON{Range: &RangeQueryJSON{Field: "price", GTE: json.Number("50"), LTE: json.Number("100")}})
		if err != nil {
			t.Fatal(err)
		}

		collector := search.NewTopKCollector(10)
		results := searcher.Search(q, collector)
		if len(results) != 2 {
			t.Errorf("got %d hits, want 2 (price 50 and 100)", len(results))
		}
	})

	t.Run("double range filter", func(t *testing.T) {
		q, err := parser.ParseQuery(QueryJSON{Range: &RangeQueryJSON{Field: "rating", GT: json.Number("3.5"), LT: json.Number("5.0")}})
		if err != nil {
			t.Fatal(err)
		}

		collector := search.NewTopKCollector(10)
		results := searcher.Search(q, collector)
		if len(results) != 2 {
			t.Errorf("got %d hits, want 2 (rating 4.0 and 4.5)", len(results))
		}
	})

	t.Run("bool with range and match", func(t *testing.T) {
		q, err := parser.ParseQuery(QueryJSON{Bool: &BoolQueryJSON{
			Must:   []QueryJSON{{Match: &MatchQueryJSON{Field: "title", Text: "widget"}}},
			Filter: []QueryJSON{{Range: &RangeQueryJSON{Field: "price", LTE: json.Number("50")}}},
		}})
		if err != nil {
			t.Fatal(err)
		}

		collector := search.NewTopKCollector(10)
		results := searcher.Search(q, collector)
		if len(results) != 2 {
			t.Errorf("got %d hits, want 2 (price <= 50: cheap and mid)", len(results))
		}
	})
}
