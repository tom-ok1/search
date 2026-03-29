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
		QueryJSON: map[string]any{"match": map[string]any{"title": "hello"}},
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
		QueryJSON: map[string]any{"term": map[string]any{"status": "active"}},
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
		QueryJSON: map[string]any{"match_all": map[string]any{}},
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
		QueryJSON: map[string]any{"match_all": map[string]any{}},
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
