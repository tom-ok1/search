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
