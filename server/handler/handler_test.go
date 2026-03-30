package handler_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"gosearch/server/node"
)

func startTestNode(t *testing.T) string {
	t.Helper()
	n, err := node.NewNode(node.NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { n.Stop() })
	return "http://" + addr
}

func mustDo(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request %s %s failed: %v", req.Method, req.URL, err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return result
}

func TestCreateAndGetIndex(t *testing.T) {
	base := startTestNode(t)

	// Create index with mappings
	req, _ := http.NewRequest("PUT", base+"/test-index",
		strings.NewReader(`{
			"settings": {"number_of_shards": 1},
			"mappings": {"properties": {
				"title": {"type": "text"},
				"status": {"type": "keyword"}
			}}
		}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: expected 200, got %d", resp.StatusCode)
	}
	if result["acknowledged"] != true {
		t.Errorf("create index: expected acknowledged=true, got %v", result["acknowledged"])
	}

	// GET the index
	req, _ = http.NewRequest("GET", base+"/test-index", nil)
	resp = mustDo(t, req)
	result = decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get index: expected 200, got %d", resp.StatusCode)
	}

	// Verify the index info is present
	indexInfo, ok := result["test-index"].(map[string]any)
	if !ok {
		t.Fatalf("get index: expected 'test-index' key in response, got %v", result)
	}
	mappings, ok := indexInfo["mappings"].(map[string]any)
	if !ok {
		t.Fatalf("get index: expected 'mappings' in index info")
	}
	props, ok := mappings["properties"].(map[string]any)
	if !ok {
		t.Fatalf("get index: expected 'properties' in mappings")
	}
	if _, ok := props["title"]; !ok {
		t.Errorf("get index: expected 'title' property in mappings")
	}
	if _, ok := props["status"]; !ok {
		t.Errorf("get index: expected 'status' property in mappings")
	}
}

func TestIndexAndGetDocument(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/docs",
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"},"price":{"type":"double"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: expected 200, got %d", resp.StatusCode)
	}

	// PUT a document
	req, _ = http.NewRequest("PUT", base+"/docs/_doc/1",
		strings.NewReader(`{"title":"hello world","price":9.99}`))
	req.Header.Set("Content-Type", "application/json")
	resp = mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("index doc: expected 200/201, got %d; body: %v", resp.StatusCode, result)
	}
	if result["result"] != "created" {
		t.Errorf("index doc: expected result=created, got %v", result["result"])
	}

	// Refresh
	req, _ = http.NewRequest("POST", base+"/docs/_refresh", nil)
	resp = mustDo(t, req)
	resp.Body.Close()

	// GET the document
	req, _ = http.NewRequest("GET", base+"/docs/_doc/1", nil)
	resp = mustDo(t, req)
	result = decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get doc: expected 200, got %d", resp.StatusCode)
	}
	if result["found"] != true {
		t.Errorf("get doc: expected found=true, got %v", result["found"])
	}
	source, ok := result["_source"].(map[string]any)
	if !ok {
		t.Fatalf("get doc: expected _source in response")
	}
	if source["title"] != "hello world" {
		t.Errorf("get doc: expected title='hello world', got %v", source["title"])
	}
	if source["price"] != 9.99 {
		t.Errorf("get doc: expected price=9.99, got %v", source["price"])
	}
}

func TestSearchEndpoint(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/articles",
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"},"body":{"type":"text"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	resp.Body.Close()

	// Index 3 documents
	for i, doc := range []string{
		`{"title":"go programming","body":"Go is a great language"}`,
		`{"title":"rust programming","body":"Rust is also great"}`,
		`{"title":"go concurrency","body":"Go has goroutines"}`,
	} {
		req, _ = http.NewRequest("PUT", fmt.Sprintf("%s/articles/_doc/%d", base, i+1),
			strings.NewReader(doc))
		req.Header.Set("Content-Type", "application/json")
		resp = mustDo(t, req)
		resp.Body.Close()
	}

	// Refresh
	req, _ = http.NewRequest("POST", base+"/articles/_refresh", nil)
	resp = mustDo(t, req)
	resp.Body.Close()

	// Search with match query
	req, _ = http.NewRequest("POST", base+"/articles/_search",
		strings.NewReader(`{"query":{"match":{"title":"go"}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp = mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search: expected 200, got %d", resp.StatusCode)
	}

	hits := result["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	if total["value"] != float64(2) {
		t.Errorf("search: expected 2 hits for 'go', got %v", total["value"])
	}
}

func TestValidation_InvalidIndexName(t *testing.T) {
	base := startTestNode(t)

	// Uppercase index name should be rejected
	req, _ := http.NewRequest("PUT", base+"/INVALID",
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid index name: expected 400, got %d; body: %s", resp.StatusCode, body)
	}
}

func TestBulkEndpoint(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/items",
		strings.NewReader(`{"mappings":{"properties":{"name":{"type":"text"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	resp.Body.Close()

	// Bulk index
	bulkBody := `{"index":{"_index":"items","_id":"1"}}
{"name":"alpha"}
{"index":{"_index":"items","_id":"2"}}
{"name":"beta"}
{"index":{"_index":"items","_id":"3"}}
{"name":"gamma"}
`
	req, _ = http.NewRequest("POST", base+"/_bulk",
		strings.NewReader(bulkBody))
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp = mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bulk: expected 200, got %d", resp.StatusCode)
	}
	if result["errors"] != false {
		t.Fatalf("bulk: expected errors=false, got %v", result["errors"])
	}

	// Refresh
	req, _ = http.NewRequest("POST", base+"/items/_refresh", nil)
	resp = mustDo(t, req)
	resp.Body.Close()

	// Search to verify all docs are indexed
	req, _ = http.NewRequest("POST", base+"/items/_search",
		strings.NewReader(`{"query":{"match_all":{}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp = mustDo(t, req)
	result = decodeJSON(t, resp)

	hits := result["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	if total["value"] != float64(3) {
		t.Errorf("bulk: expected 3 docs after bulk index, got %v", total["value"])
	}
}

func TestDeleteIndex(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/todelete",
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	resp.Body.Close()

	// Verify it exists
	req, _ = http.NewRequest("GET", base+"/todelete", nil)
	resp = mustDo(t, req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get index before delete: expected 200, got %d", resp.StatusCode)
	}

	// Delete it
	req, _ = http.NewRequest("DELETE", base+"/todelete", nil)
	resp = mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete index: expected 200, got %d", resp.StatusCode)
	}
	if result["acknowledged"] != true {
		t.Errorf("delete index: expected acknowledged=true, got %v", result["acknowledged"])
	}

	// GET should return 404
	req, _ = http.NewRequest("GET", base+"/todelete", nil)
	resp = mustDo(t, req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("get after delete: expected 404, got %d", resp.StatusCode)
	}
}

func TestDeleteDocument(t *testing.T) {
	base := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", base+"/mydocs",
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustDo(t, req)
	resp.Body.Close()

	// Index a document
	req, _ = http.NewRequest("PUT", base+"/mydocs/_doc/doc1",
		strings.NewReader(`{"title":"to be deleted"}`))
	req.Header.Set("Content-Type", "application/json")
	resp = mustDo(t, req)
	resp.Body.Close()

	// Refresh
	req, _ = http.NewRequest("POST", base+"/mydocs/_refresh", nil)
	resp = mustDo(t, req)
	resp.Body.Close()

	// Delete the document
	req, _ = http.NewRequest("DELETE", base+"/mydocs/_doc/doc1", nil)
	resp = mustDo(t, req)
	result := decodeJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete doc: expected 200, got %d", resp.StatusCode)
	}
	if result["result"] != "deleted" {
		t.Errorf("delete doc: expected result='deleted', got %v", result["result"])
	}
}
