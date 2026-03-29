// server/node/node_test.go
package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestNode_StartAndStop(t *testing.T) {
	cfg := NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer n.Stop()

	resp, err := http.Get(fmt.Sprintf("http://%s/nonexistent", addr))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %q", ct)
	}
}

func TestNode_StopIsIdempotent(t *testing.T) {
	cfg := NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if _, err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := n.Stop(); err != nil {
		t.Errorf("first Stop: %v", err)
	}
	if err := n.Stop(); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

func TestNode_ReturnsJSONError(t *testing.T) {
	cfg := NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer n.Stop()

	resp, err := http.Get(fmt.Sprintf("http://%s/anything", addr))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("expected non-empty JSON error body")
	}
}

func startTestNode(t *testing.T) (string, *Node) {
	t.Helper()
	cfg := NodeConfig{
		DataPath: t.TempDir(),
		HTTPPort: 0,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	addr, err := n.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { n.Stop() })
	return addr, n
}

func TestNode_CreateIndex(t *testing.T) {
	addr, _ := startTestNode(t)

	body := `{"settings":{"number_of_shards":1},"mappings":{"properties":{"title":{"type":"text"}}}}`
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if result["acknowledged"] != true {
		t.Errorf("expected acknowledged=true, got %v", result["acknowledged"])
	}
	if result["index"] != "myindex" {
		t.Errorf("expected index=myindex, got %v", result["index"])
	}
}

func TestNode_CreateIndex_Duplicate(t *testing.T) {
	addr, _ := startTestNode(t)

	body := `{}`
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(body))
	http.DefaultClient.Do(req)

	// Second create should fail
	req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate, got %d", resp.StatusCode)
	}
}

func TestNode_GetIndex(t *testing.T) {
	addr, _ := startTestNode(t)

	// Create index first
	body := `{"settings":{"number_of_shards":1},"mappings":{"properties":{"title":{"type":"text"}}}}`
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(body))
	http.DefaultClient.Do(req)

	// GET the index
	resp, err := http.Get(fmt.Sprintf("http://%s/myindex", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	indexData, ok := result["myindex"].(map[string]any)
	if !ok {
		t.Fatalf("expected myindex key in response, got %v", result)
	}
	settings, ok := indexData["settings"].(map[string]any)
	if !ok {
		t.Fatalf("expected settings in response")
	}
	if settings["number_of_shards"] != float64(1) {
		t.Errorf("expected number_of_shards=1, got %v", settings["number_of_shards"])
	}
}

func TestNode_GetIndex_NotFound(t *testing.T) {
	addr, _ := startTestNode(t)

	resp, err := http.Get(fmt.Sprintf("http://%s/nonexistent", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestNode_DeleteIndex(t *testing.T) {
	addr, _ := startTestNode(t)

	// Create index first
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(`{}`))
	http.DefaultClient.Do(req)

	// DELETE the index
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("http://%s/myindex", addr), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if result["acknowledged"] != true {
		t.Errorf("expected acknowledged=true")
	}

	// Verify GET returns 404
	resp2, err := http.Get(fmt.Sprintf("http://%s/myindex", addr))
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 after delete, got %d", resp2.StatusCode)
	}
}

func TestNode_DeleteIndex_NotFound(t *testing.T) {
	addr, _ := startTestNode(t)

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/nonexistent", addr), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestNode_CreateIndex_InvalidName(t *testing.T) {
	addr, _ := startTestNode(t)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/INVALID", addr), strings.NewReader(`{}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid name, got %d", resp.StatusCode)
	}
}

func TestNode_CreateIndex_InvalidBody(t *testing.T) {
	addr, _ := startTestNode(t)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(`{not json`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid body, got %d", resp.StatusCode)
	}
}

func TestNode_IndexAndGetDocument(t *testing.T) {
	addr, _ := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr),
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	http.DefaultClient.Do(req)

	// Index a document
	req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex/_doc/1", addr),
		strings.NewReader(`{"title":"hello world"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT _doc: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, body)
	}

	var indexResult map[string]any
	json.NewDecoder(resp.Body).Decode(&indexResult)
	if indexResult["result"] != "created" {
		t.Errorf("expected result=created, got %v", indexResult["result"])
	}

	// Refresh
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_refresh", addr), nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST _refresh: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("refresh expected 200, got %d", resp2.StatusCode)
	}

	// Get the document
	resp3, err := http.Get(fmt.Sprintf("http://%s/myindex/_doc/1", addr))
	if err != nil {
		t.Fatalf("GET _doc: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp3.StatusCode)
	}

	var getResult map[string]any
	json.NewDecoder(resp3.Body).Decode(&getResult)
	if getResult["found"] != true {
		t.Errorf("expected found=true, got %v", getResult["found"])
	}
	if getResult["_id"] != "1" {
		t.Errorf("expected _id=1, got %v", getResult["_id"])
	}
}

func TestNode_DeleteDocument(t *testing.T) {
	addr, _ := startTestNode(t)

	// Create index + doc
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr),
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex/_doc/1", addr),
		strings.NewReader(`{"title":"hello"}`))
	http.DefaultClient.Do(req)

	// Delete document
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("http://%s/myindex/_doc/1", addr), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE _doc: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	if result["result"] != "deleted" {
		t.Errorf("expected result=deleted, got %v", result["result"])
	}
}

func TestNode_GetDocument_NotFound(t *testing.T) {
	addr, _ := startTestNode(t)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr), strings.NewReader(`{}`))
	http.DefaultClient.Do(req)

	// Refresh so searcher is not nil
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_refresh", addr), nil)
	http.DefaultClient.Do(req)

	resp, err := http.Get(fmt.Sprintf("http://%s/myindex/_doc/999", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestNode_Search(t *testing.T) {
	addr, _ := startTestNode(t)

	// Create index
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr),
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"},"status":{"type":"keyword"}}}}`))
	http.DefaultClient.Do(req)

	// Index documents
	for i, doc := range []string{
		`{"title":"hello world","status":"active"}`,
		`{"title":"hello go","status":"active"}`,
		`{"title":"goodbye world","status":"archived"}`,
	} {
		req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex/_doc/%d", addr, i+1),
			strings.NewReader(doc))
		http.DefaultClient.Do(req)
	}

	// Refresh
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_refresh", addr), nil)
	http.DefaultClient.Do(req)

	// Search with match query
	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_search", addr),
		strings.NewReader(`{"query":{"match":{"title":"hello"}},"size":10}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST _search: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)

	hits, ok := result["hits"].(map[string]any)
	if !ok {
		t.Fatalf("expected hits in response, got %v", result)
	}
	total := hits["total"].(map[string]any)
	if total["value"] != float64(2) {
		t.Errorf("expected 2 hits, got %v", total["value"])
	}
	hitList := hits["hits"].([]any)
	if len(hitList) != 2 {
		t.Errorf("expected 2 hit entries, got %d", len(hitList))
	}
}

func TestNode_SearchMatchAll(t *testing.T) {
	addr, _ := startTestNode(t)

	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex", addr),
		strings.NewReader(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("PUT", fmt.Sprintf("http://%s/myindex/_doc/1", addr),
		strings.NewReader(`{"title":"test"}`))
	http.DefaultClient.Do(req)

	req, _ = http.NewRequest("POST", fmt.Sprintf("http://%s/myindex/_refresh", addr), nil)
	http.DefaultClient.Do(req)

	// Search with GET (no body = match_all)
	resp, err := http.Get(fmt.Sprintf("http://%s/myindex/_search", addr))
	if err != nil {
		t.Fatalf("GET _search: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)

	hits := result["hits"].(map[string]any)
	total := hits["total"].(map[string]any)
	if total["value"] != float64(1) {
		t.Errorf("expected 1 hit for match_all, got %v", total["value"])
	}
}
