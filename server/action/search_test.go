package action

import (
	"encoding/json"
	"fmt"
	"testing"

	"gosearch/search"
	"gosearch/server/mapping"
)

func TestTransportSearchAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title":  {Type: mapping.FieldTypeText},
				"status": {Type: mapping.FieldTypeKeyword},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["docs"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for i, title := range []string{"hello world", "hello go", "goodbye world"} {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "docs",
			ID:     fmt.Sprintf("%d", i+1),
			Source: json.RawMessage(fmt.Sprintf(`{"title":%q,"status":"active"}`, title)),
		})
		if err != nil {
			t.Fatalf("index %d: %v", i, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "docs"})

	searchAction := NewTransportSearchAction(cs, services, registry)

	// Test match query
	resp, err := searchAction.Execute(SearchRequest{
		Index:     "docs",
		QueryJSON: QueryJSON{Match: &MatchQueryJSON{Field: "title", Text: "hello"}},
		Size:      10,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Hits.Total.Value != 2 {
		t.Errorf("expected 2 hits, got %d", resp.Hits.Total.Value)
	}

	// Test term query on keyword
	resp, err = searchAction.Execute(SearchRequest{
		Index:     "docs",
		QueryJSON: QueryJSON{Term: &TermQueryJSON{Field: "status", Value: "active"}},
		Size:      10,
	})
	if err != nil {
		t.Fatalf("Execute term: %v", err)
	}
	if resp.Hits.Total.Value != 3 {
		t.Errorf("expected 3 hits for status=active, got %d", resp.Hits.Total.Value)
	}

	// Test match_all
	resp, err = searchAction.Execute(SearchRequest{
		Index:     "docs",
		QueryJSON: QueryJSON{MatchAll: &MatchAllQueryJSON{}},
		Size:      10,
	})
	if err != nil {
		t.Fatalf("Execute match_all: %v", err)
	}
	if resp.Hits.Total.Value != 3 {
		t.Errorf("expected 3 hits for match_all, got %d", resp.Hits.Total.Value)
	}
}

func TestTransportSearchAction_Size(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	createAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	defer services["docs"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for i := range 5 {
		indexAction.Execute(IndexDocumentRequest{
			Index:  "docs",
			ID:     fmt.Sprintf("%d", i),
			Source: json.RawMessage(`{"title":"hello"}`),
		})
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "docs"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index:     "docs",
		QueryJSON: QueryJSON{MatchAll: &MatchAllQueryJSON{}},
		Size:      2,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(resp.Hits.Hits) != 2 {
		t.Errorf("expected 2 hits with size=2, got %d", len(resp.Hits.Hits))
	}
	if resp.Hits.Total.Value != 5 {
		t.Errorf("expected total=5, got %d", resp.Hits.Total.Value)
	}
}

func TestSearch_MatchPhrase(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "articles",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["articles"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for _, tc := range []struct {
		id, title string
	}{
		{"1", "the quick brown fox"},
		{"2", "quick fox brown"},
		{"3", "brown fox quick"},
	} {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "articles",
			ID:     tc.id,
			Source: json.RawMessage(fmt.Sprintf(`{"title":%q}`, tc.title)),
		})
		if err != nil {
			t.Fatalf("index %s: %v", tc.id, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "articles"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index:     "articles",
		QueryJSON: QueryJSON{MatchPhrase: &MatchPhraseQueryJSON{Field: "title", Text: "quick brown"}},
		Size:      10,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}

	// Only doc 1 has "quick brown" as consecutive terms
	if resp.Hits.Total.Value != 1 {
		t.Errorf("total hits = %d, want 1", resp.Hits.Total.Value)
	}
	if len(resp.Hits.Hits) != 1 {
		t.Fatalf("hits = %d, want 1", len(resp.Hits.Hits))
	}
}

func TestSearch_MultiMatch(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "articles",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
				"body":  {Type: mapping.FieldTypeText},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["articles"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for _, tc := range []struct {
		id, source string
	}{
		{"1", `{"title": "golang tutorial", "body": "learn programming"}`},
		{"2", `{"title": "cooking recipes", "body": "golang tips"}`},
		{"3", `{"title": "travel guide", "body": "vacation spots"}`},
	} {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "articles",
			ID:     tc.id,
			Source: json.RawMessage(tc.source),
		})
		if err != nil {
			t.Fatalf("index %s: %v", tc.id, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "articles"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index: "articles",
		QueryJSON: QueryJSON{MultiMatch: &MultiMatchQueryJSON{Query: "golang", Fields: []string{"title", "body"}}},
		Size: 10,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}

	// Docs 1 (title match) and 2 (body match) should be found
	if resp.Hits.Total.Value != 2 {
		t.Errorf("total hits = %d, want 2", resp.Hits.Total.Value)
	}
}

func TestSearch_Exists(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "products",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"name":  {Type: mapping.FieldTypeText},
				"color": {Type: mapping.FieldTypeKeyword},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["products"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for _, tc := range []struct {
		id, source string
	}{
		{"1", `{"name": "widget", "color": "red"}`},
		{"2", `{"name": "gadget"}`},
		{"3", `{"name": "thing", "color": "blue"}`},
	} {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "products",
			ID:     tc.id,
			Source: json.RawMessage(tc.source),
		})
		if err != nil {
			t.Fatalf("index %s: %v", tc.id, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "products"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index: "products",
		QueryJSON: QueryJSON{Exists: &ExistsQueryJSON{Field: "color"}},
		Size: 10,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}

	// Only docs 1 and 3 have "color"
	if resp.Hits.Total.Value != 2 {
		t.Errorf("total hits = %d, want 2", resp.Hits.Total.Value)
	}
}

func TestMergeTopDocs_TieBreakByShardIndex(t *testing.T) {
	// All shards have docs with the same score - deterministic tie-breaking required
	shard0 := []search.SearchResult{
		{DocID: 10, Score: 5.0},
		{DocID: 11, Score: 3.0},
	}
	shard1 := []search.SearchResult{
		{DocID: 20, Score: 5.0},
		{DocID: 21, Score: 3.0},
	}
	shard2 := []search.SearchResult{
		{DocID: 30, Score: 5.0},
		{DocID: 31, Score: 3.0},
	}

	merged := mergeTopDocs([][]search.SearchResult{shard0, shard1, shard2}, 6)
	if len(merged) != 6 {
		t.Fatalf("expected 6 results, got %d", len(merged))
	}

	// Equal scores: lower shard index should come first (Lucene-style tie-breaking)
	// First 3 results all have score 5.0, should be in shard order: 10, 20, 30
	if merged[0].DocID != 10 {
		t.Errorf("merged[0].DocID = %d, want 10 (shard 0 wins tie)", merged[0].DocID)
	}
	if merged[1].DocID != 20 {
		t.Errorf("merged[1].DocID = %d, want 20 (shard 1 second)", merged[1].DocID)
	}
	if merged[2].DocID != 30 {
		t.Errorf("merged[2].DocID = %d, want 30 (shard 2 third)", merged[2].DocID)
	}

	// Next 3 results all have score 3.0, should also be in shard order: 11, 21, 31
	if merged[3].DocID != 11 {
		t.Errorf("merged[3].DocID = %d, want 11 (shard 0 wins tie)", merged[3].DocID)
	}
	if merged[4].DocID != 21 {
		t.Errorf("merged[4].DocID = %d, want 21 (shard 1 second)", merged[4].DocID)
	}
	if merged[5].DocID != 31 {
		t.Errorf("merged[5].DocID = %d, want 31 (shard 2 third)", merged[5].DocID)
	}
}
