package action

import (
	"encoding/json"
	"fmt"
	"testing"

	"gosearch/server/mapping"
)

func TestSearch_TermsAggregation(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "agg-test",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"status": {Type: mapping.FieldTypeKeyword},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["agg-test"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for i, status := range []string{"active", "active", "inactive"} {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "agg-test",
			ID:     fmt.Sprintf("%d", i+1),
			Source: json.RawMessage(fmt.Sprintf(`{"status":%q}`, status)),
		})
		if err != nil {
			t.Fatalf("index %d: %v", i, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "agg-test"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index:     "agg-test",
		QueryJSON: QueryJSON{MatchAll: &MatchAllQueryJSON{}},
		AggsJSON: map[string]any{
			"status_counts": map[string]any{
				"terms": map[string]any{
					"field": "status",
				},
			},
		},
		Size: 10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if resp.Hits.Total.Value != 3 {
		t.Errorf("expected 3 hits, got %d", resp.Hits.Total.Value)
	}

	if resp.Aggregations == nil {
		t.Fatal("expected aggregations in response")
	}

	statusAgg, ok := resp.Aggregations["status_counts"]
	if !ok {
		t.Fatal("expected status_counts aggregation")
	}

	aggMap, ok := statusAgg.(map[string]any)
	if !ok {
		t.Fatal("expected aggregation to be a map")
	}

	bucketsRaw, ok := aggMap["buckets"]
	if !ok {
		t.Fatal("expected buckets in aggregation")
	}

	buckets, ok := bucketsRaw.([]map[string]any)
	if !ok {
		t.Fatalf("expected buckets to be []map[string]any, got %T", bucketsRaw)
	}

	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}

	// Buckets should be sorted by doc_count descending (active=2, inactive=1)
	if buckets[0]["key"] != "active" {
		t.Errorf("expected first bucket key=active, got %v", buckets[0]["key"])
	}
	if buckets[0]["doc_count"] != int64(2) {
		t.Errorf("expected first bucket doc_count=2, got %v", buckets[0]["doc_count"])
	}
	if buckets[1]["key"] != "inactive" {
		t.Errorf("expected second bucket key=inactive, got %v", buckets[1]["key"])
	}
	if buckets[1]["doc_count"] != int64(1) {
		t.Errorf("expected second bucket doc_count=1, got %v", buckets[1]["doc_count"])
	}
}

func TestSearch_ValueCountAggregation(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "agg-vc",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"status": {Type: mapping.FieldTypeKeyword},
			},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer services["agg-vc"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	for i := range 3 {
		_, err := indexAction.Execute(IndexDocumentRequest{
			Index:  "agg-vc",
			ID:     fmt.Sprintf("%d", i+1),
			Source: json.RawMessage(`{"status":"active"}`),
		})
		if err != nil {
			t.Fatalf("index %d: %v", i, err)
		}
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "agg-vc"})

	searchAction := NewTransportSearchAction(cs, services, registry)
	resp, err := searchAction.Execute(SearchRequest{
		Index:     "agg-vc",
		QueryJSON: QueryJSON{MatchAll: &MatchAllQueryJSON{}},
		AggsJSON: map[string]any{
			"status_count": map[string]any{
				"value_count": map[string]any{
					"field": "status",
				},
			},
		},
		Size: 10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if resp.Aggregations == nil {
		t.Fatal("expected aggregations in response")
	}

	statusAgg, ok := resp.Aggregations["status_count"]
	if !ok {
		t.Fatal("expected status_count aggregation")
	}

	aggMap, ok := statusAgg.(map[string]any)
	if !ok {
		t.Fatal("expected aggregation to be a map")
	}

	val, ok := aggMap["value"]
	if !ok {
		t.Fatal("expected value in aggregation")
	}

	if val != int64(3) {
		t.Errorf("expected value_count=3, got %v (type %T)", val, val)
	}
}
