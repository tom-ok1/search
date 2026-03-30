package action

import (
	"testing"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/mapping"
)

func newTestParser() *QueryParser {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title":  {Type: mapping.FieldTypeText},
			"status": {Type: mapping.FieldTypeKeyword},
			"count":  {Type: mapping.FieldTypeLong},
		},
	}
	registry := analysis.DefaultRegistry()
	return NewQueryParser(m, registry)
}

func TestQueryParser_MatchAll(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{"match_all": map[string]any{}}
	q, err := p.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	if _, ok := q.(*search.MatchAllQuery); !ok {
		t.Errorf("expected MatchAllQuery, got %T", q)
	}
}

func TestQueryParser_Term(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{
		"term": map[string]any{
			"status": "active",
		},
	}
	q, err := p.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	tq, ok := q.(*search.TermQuery)
	if !ok {
		t.Fatalf("expected TermQuery, got %T", q)
	}
	if tq.Field != "status" || tq.Term != "active" {
		t.Errorf("expected status=active, got %s=%s", tq.Field, tq.Term)
	}
}

func TestQueryParser_MatchSingleToken(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{
		"match": map[string]any{
			"title": "hello",
		},
	}
	q, err := p.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	tq, ok := q.(*search.TermQuery)
	if !ok {
		t.Fatalf("expected TermQuery for single token, got %T", q)
	}
	if tq.Field != "title" || tq.Term != "hello" {
		t.Errorf("expected title=hello, got %s=%s", tq.Field, tq.Term)
	}
}

func TestQueryParser_MatchMultipleTokens(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{
		"match": map[string]any{
			"title": "hello world",
		},
	}
	q, err := p.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery for multi-token match, got %T", q)
	}
	if len(bq.Clauses) != 2 {
		t.Fatalf("expected 2 clauses, got %d", len(bq.Clauses))
	}
	for _, c := range bq.Clauses {
		if c.Occur != search.OccurShould {
			t.Errorf("expected SHOULD clause, got %v", c.Occur)
		}
	}
}

func TestQueryParser_Bool(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{
		"bool": map[string]any{
			"must": []any{
				map[string]any{"term": map[string]any{"status": "active"}},
			},
			"must_not": []any{
				map[string]any{"term": map[string]any{"status": "archived"}},
			},
		},
	}
	q, err := p.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery, got %T", q)
	}
	if len(bq.Clauses) != 2 {
		t.Fatalf("expected 2 clauses, got %d", len(bq.Clauses))
	}

	mustCount := 0
	mustNotCount := 0
	for _, c := range bq.Clauses {
		switch c.Occur {
		case search.OccurMust:
			mustCount++
		case search.OccurMustNot:
			mustNotCount++
		}
	}
	if mustCount != 1 || mustNotCount != 1 {
		t.Errorf("expected 1 must + 1 must_not, got %d must + %d must_not", mustCount, mustNotCount)
	}
}

func TestQueryParser_UnknownQuery(t *testing.T) {
	p := newTestParser()
	raw := map[string]any{"unknown_type": map[string]any{}}
	_, err := p.ParseQuery(raw)
	if err == nil {
		t.Fatal("expected error for unknown query type")
	}
}

func TestQueryParser_TermRejectsObjectValue(t *testing.T) {
	p := newTestParser()

	// ES expanded form — should error since we don't support it yet
	_, err := p.ParseQuery(map[string]any{
		"term": map[string]any{
			"status": map[string]any{"value": "active", "boost": 1.5},
		},
	})
	if err == nil {
		t.Error("expected error for object value in term query, got nil")
	}
}

func TestQueryParser_TermRejectsArrayValue(t *testing.T) {
	p := newTestParser()

	_, err := p.ParseQuery(map[string]any{
		"term": map[string]any{
			"status": []any{"active", "pending"},
		},
	})
	if err == nil {
		t.Error("expected error for array value in term query, got nil")
	}
}

func TestQueryParser_MatchZeroTokensMatchesNothing(t *testing.T) {
	p := newTestParser()

	q, err := p.ParseQuery(map[string]any{
		"match": map[string]any{"title": ""},
	})
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}

	if _, ok := q.(*search.MatchNoneQuery); !ok {
		t.Errorf("expected MatchNoneQuery for empty match, got %T", q)
	}
}

func TestQueryParser_MatchPhrase(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match_phrase": map[string]any{
			"title": "quick brown fox",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pq, ok := q.(*search.PhraseQuery)
	if !ok {
		t.Fatalf("expected *search.PhraseQuery, got %T", q)
	}

	if pq.Field != "title" {
		t.Errorf("field = %q, want %q", pq.Field, "title")
	}

	wantTerms := []string{"quick", "brown", "fox"}
	if len(pq.Terms) != len(wantTerms) {
		t.Fatalf("terms = %v, want %v", pq.Terms, wantTerms)
	}
	for i, term := range pq.Terms {
		if term != wantTerms[i] {
			t.Errorf("term[%d] = %q, want %q", i, term, wantTerms[i])
		}
	}
}

func TestQueryParser_MatchPhrase_SingleToken(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match_phrase": map[string]any{
			"title": "hello",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Single token still produces PhraseQuery (ES behavior)
	pq, ok := q.(*search.PhraseQuery)
	if !ok {
		t.Fatalf("expected *search.PhraseQuery, got %T", q)
	}
	if len(pq.Terms) != 1 || pq.Terms[0] != "hello" {
		t.Errorf("terms = %v, want [hello]", pq.Terms)
	}
}

func TestQueryParser_MatchPhrase_EmptyTokens(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match_phrase": map[string]any{
			"title": "",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := q.(*search.MatchNoneQuery)
	if !ok {
		t.Fatalf("expected *search.MatchNoneQuery for empty input, got %T", q)
	}
}

func TestQueryParser_Exists(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"exists": map[string]any{
			"field": "title",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	feq, ok := q.(*search.FieldExistsQuery)
	if !ok {
		t.Fatalf("expected *search.FieldExistsQuery, got %T", q)
	}
	if feq.Field != "title" {
		t.Errorf("field = %q, want %q", feq.Field, "title")
	}
	if feq.Mode != search.FieldExistsNorms {
		t.Errorf("mode = %v, want FieldExistsNorms", feq.Mode)
	}
}

func TestQueryParser_ExistsKeyword(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"status": {Type: mapping.FieldTypeKeyword},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"exists": map[string]any{
			"field": "status",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	feq, ok := q.(*search.FieldExistsQuery)
	if !ok {
		t.Fatalf("expected *search.FieldExistsQuery, got %T", q)
	}
	if feq.Mode != search.FieldExistsDocValues {
		t.Errorf("mode = %v, want FieldExistsDocValues", feq.Mode)
	}
}

func TestQueryParser_ExistsUnmappedField(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"exists": map[string]any{
			"field": "nonexistent",
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := q.(*search.MatchNoneQuery)
	if !ok {
		t.Fatalf("expected *search.MatchNoneQuery for unmapped field, got %T", q)
	}
}

func TestQueryParser_Exists_MissingField(t *testing.T) {
	m := &mapping.MappingDefinition{}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"exists": map[string]any{},
	}

	_, err := parser.ParseQuery(queryJSON)
	if err == nil {
		t.Fatal("expected error for exists query without 'field'")
	}
}

func TestQueryParser_MultiMatch(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
			"body":  {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"multi_match": map[string]any{
			"query":  "hello world",
			"fields": []any{"title", "body"},
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected *search.BooleanQuery, got %T", q)
	}

	// Should have 2 SHOULD clauses (one match query per field)
	if len(bq.Clauses) != 2 {
		t.Fatalf("clauses = %d, want 2", len(bq.Clauses))
	}

	for _, clause := range bq.Clauses {
		if clause.Occur != search.OccurShould {
			t.Errorf("clause occur = %v, want SHOULD", clause.Occur)
		}
	}
}

func TestQueryParser_MultiMatch_SingleField(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"multi_match": map[string]any{
			"query":  "hello",
			"fields": []any{"title"},
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Single field still wrapped in BooleanQuery for consistency
	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected *search.BooleanQuery, got %T", q)
	}
	if len(bq.Clauses) != 1 {
		t.Fatalf("clauses = %d, want 1", len(bq.Clauses))
	}
}

func TestQueryParser_MultiMatch_MissingFields(t *testing.T) {
	m := &mapping.MappingDefinition{}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"multi_match": map[string]any{
			"query": "hello",
		},
	}

	_, err := parser.ParseQuery(queryJSON)
	if err == nil {
		t.Fatal("expected error for multi_match without 'fields'")
	}
}

func TestQueryParser_BoolFilter(t *testing.T) {
	p := newTestParser()

	q, err := p.ParseQuery(map[string]any{
		"bool": map[string]any{
			"filter": []any{
				map[string]any{"term": map[string]any{"status": "active"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}

	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected BooleanQuery, got %T", q)
	}
	if len(bq.Clauses) != 1 {
		t.Fatalf("expected 1 clause, got %d", len(bq.Clauses))
	}
	if bq.Clauses[0].Occur != search.OccurFilter {
		t.Errorf("expected OccurFilter for filter clause, got %v", bq.Clauses[0].Occur)
	}
}

func TestQueryParser_MatchObjectForm(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match": map[string]any{
			"title": map[string]any{
				"query": "hello world",
			},
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected *search.BooleanQuery, got %T", q)
	}
	if len(bq.Clauses) != 2 {
		t.Fatalf("clauses = %d, want 2", len(bq.Clauses))
	}

	// Verify that the terms are "hello" and "world", not garbage like "map[query:hello"
	terms := make([]string, 0)
	for _, clause := range bq.Clauses {
		if tq, ok := clause.Query.(*search.TermQuery); ok {
			terms = append(terms, tq.Term)
		}
	}
	if len(terms) != 2 {
		t.Fatalf("extracted %d terms, want 2", len(terms))
	}
	wantTerms := []string{"hello", "world"}
	for i, term := range terms {
		if term != wantTerms[i] {
			t.Errorf("term[%d] = %q, want %q", i, term, wantTerms[i])
		}
	}
}

func TestQueryParser_MatchObjectFormWithAnalyzer(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match": map[string]any{
			"title": map[string]any{
				"query":    "hello",
				"analyzer": "ngram",
			},
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// NGram analyzer should produce multiple tokens from "hello"
	bq, ok := q.(*search.BooleanQuery)
	if !ok {
		t.Fatalf("expected *search.BooleanQuery for ngram tokens, got %T", q)
	}
	if len(bq.Clauses) < 2 {
		t.Errorf("expected multiple ngram clauses, got %d", len(bq.Clauses))
	}
}

func TestQueryParser_MatchPhraseObjectForm(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
		},
	}
	registry := analysis.DefaultRegistry()
	parser := NewQueryParser(m, registry)

	queryJSON := map[string]any{
		"match_phrase": map[string]any{
			"title": map[string]any{
				"query": "quick brown fox",
			},
		},
	}

	q, err := parser.ParseQuery(queryJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pq, ok := q.(*search.PhraseQuery)
	if !ok {
		t.Fatalf("expected *search.PhraseQuery, got %T", q)
	}

	wantTerms := []string{"quick", "brown", "fox"}
	if len(pq.Terms) != len(wantTerms) {
		t.Fatalf("terms = %v, want %v", pq.Terms, wantTerms)
	}
	for i, term := range pq.Terms {
		if term != wantTerms[i] {
			t.Errorf("term[%d] = %q, want %q", i, term, wantTerms[i])
		}
	}
}
