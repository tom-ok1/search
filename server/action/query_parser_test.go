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
