package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"gosearch/server/node"
)

func startE2ENode(t *testing.T) string {
	t.Helper()
	cfg := node.NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	}
	n, err := node.NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { n.Stop() })
	return addr
}

func TestE2E_Smoke(t *testing.T) {
	addr := startE2ENode(t)

	resp, err := http.Get(fmt.Sprintf("http://%s/nonexistent", addr))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestE2E_FullLifecycle(t *testing.T) {
	addr := startE2ENode(t)

	// 1. Create index
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/products", addr),
		strings.NewReader(`{
			"settings": {"number_of_shards": 1},
			"mappings": {"properties": {
				"name": {"type": "text"},
				"category": {"type": "keyword"},
				"price": {"type": "double"}
			}}
		}`))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: expected 200, got %d", resp.StatusCode)
	}

	// 2. Bulk index documents
	bulkBody := `{"index":{"_index":"products","_id":"1"}}
{"name":"wireless mouse","category":"electronics","price":29.99}
{"index":{"_index":"products","_id":"2"}}
{"name":"mechanical keyboard","category":"electronics","price":89.99}
{"index":{"_index":"products","_id":"3"}}
{"name":"cotton t-shirt","category":"clothing","price":19.99}
{"index":{"_index":"products","_id":"4"}}
{"name":"wireless headphones","category":"electronics","price":59.99}
`
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/_bulk", addr),
		strings.NewReader(bulkBody))
	resp, _ = http.DefaultClient.Do(req)
	var bulkResult map[string]any
	json.NewDecoder(resp.Body).Decode(&bulkResult)
	resp.Body.Close()
	if bulkResult["errors"] != false {
		t.Fatalf("bulk index had errors: %v", bulkResult)
	}

	// 3. Refresh
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_refresh", addr), nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// 4. Search: match query
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_search", addr),
		strings.NewReader(`{"query":{"match":{"name":"wireless"}}}`))
	resp, _ = http.DefaultClient.Do(req)
	var searchResult map[string]any
	json.NewDecoder(resp.Body).Decode(&searchResult)
	resp.Body.Close()

	hits := searchResult["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	if total["value"] != float64(2) {
		t.Errorf("match 'wireless': expected 2 hits, got %v", total["value"])
	}

	// 5. Search: term query on keyword
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_search", addr),
		strings.NewReader(`{"query":{"term":{"category":"electronics"}}}`))
	resp, _ = http.DefaultClient.Do(req)
	json.NewDecoder(resp.Body).Decode(&searchResult)
	resp.Body.Close()

	hits = searchResult["hits"].(map[string]any)
	total = hits["total"].(map[string]any)
	if total["value"] != float64(3) {
		t.Errorf("term category=electronics: expected 3 hits, got %v", total["value"])
	}

	// 6. Search: bool query (must + must_not)
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_search", addr),
		strings.NewReader(`{"query":{"bool":{
			"must":[{"term":{"category":"electronics"}}],
			"must_not":[{"match":{"name":"wireless"}}]
		}}}`))
	resp, _ = http.DefaultClient.Do(req)
	json.NewDecoder(resp.Body).Decode(&searchResult)
	resp.Body.Close()

	hits = searchResult["hits"].(map[string]any)
	total = hits["total"].(map[string]any)
	if total["value"] != float64(1) {
		t.Errorf("bool (electronics NOT wireless): expected 1 hit, got %v", total["value"])
	}
	hitList := hits["hits"].([]any)
	if len(hitList) == 1 {
		hit := hitList[0].(map[string]any)
		source := hit["_source"].(map[string]any)
		if name, ok := source["name"].(string); !ok || name != "mechanical keyboard" {
			t.Errorf("expected 'mechanical keyboard', got %v", source["name"])
		}
	}

	// 7. Get document by ID
	resp, _ = http.Get(fmt.Sprintf("http://%s/products/_doc/1", addr))
	var getResult map[string]any
	json.NewDecoder(resp.Body).Decode(&getResult)
	resp.Body.Close()
	if getResult["found"] != true {
		t.Errorf("GET doc 1: expected found=true")
	}

	// 8. Delete document
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("http://%s/products/_doc/1", addr), nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// 9. Refresh and verify deletion
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_refresh", addr), nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/products/_search", addr),
		strings.NewReader(`{"query":{"match_all":{}}}`))
	resp, _ = http.DefaultClient.Do(req)
	json.NewDecoder(resp.Body).Decode(&searchResult)
	resp.Body.Close()

	hits = searchResult["hits"].(map[string]any)
	total = hits["total"].(map[string]any)
	if total["value"] != float64(3) {
		t.Errorf("after delete: expected 3 hits, got %v", total["value"])
	}

	// 10. Delete index
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("http://%s/products", addr), nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// 11. Verify index is gone
	resp, _ = http.Get(fmt.Sprintf("http://%s/products", addr))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("after delete index: expected 404, got %d", resp.StatusCode)
	}
}

func TestE2E_ErrorCases(t *testing.T) {
	addr := startE2ENode(t)

	// Index not found for document operations
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/nonexistent/_doc/1", addr),
		strings.NewReader(`{"title":"hello"}`))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("index doc to nonexistent: expected 404, got %d", resp.StatusCode)
	}

	// Search on nonexistent index
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/nonexistent/_search", addr),
		strings.NewReader(`{"query":{"match_all":{}}}`))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("search nonexistent: expected 404, got %d", resp.StatusCode)
	}

	// Invalid query DSL
	req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(`{}`))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_search", addr),
		strings.NewReader(`{"query":{"invalid_query_type":{}}}`))
	resp, _ = http.DefaultClient.Do(req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid query: expected 400, got %d: %s", resp.StatusCode, body)
	}
}

func TestE2E_SearchSize(t *testing.T) {
	addr := startE2ENode(t)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr),
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	http.DefaultClient.Do(req)

	// Bulk index 5 docs
	var bulkBody strings.Builder
	for i := 1; i <= 5; i++ {
		fmt.Fprintf(&bulkBody, "{\"index\":{\"_index\":\"myindex\",\"_id\":\"%d\"}}\n", i)
		fmt.Fprintf(&bulkBody, "{\"title\":\"document number %d\"}\n", i)
	}
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/_bulk", addr),
		strings.NewReader(bulkBody.String()))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_refresh", addr), nil)
	http.DefaultClient.Do(req)

	// Search with size=2
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_search", addr),
		strings.NewReader(`{"query":{"match_all":{}},"size":2}`))
	resp, _ := http.DefaultClient.Do(req)
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	hits := result["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	hitList := hits["hits"].([]any)

	if total["value"] != float64(5) {
		t.Errorf("expected total=5, got %v", total["value"])
	}
	if len(hitList) != 2 {
		t.Errorf("expected 2 hits with size=2, got %d", len(hitList))
	}
}
