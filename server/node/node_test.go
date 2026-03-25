// server/node/node_test.go
package node

import (
	"fmt"
	"io"
	"net/http"
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
