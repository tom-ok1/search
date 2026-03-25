// server/e2e_test.go
package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"gosearch/server/node"
)

func TestNodeStartsAndReturns404(t *testing.T) {
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
	defer n.Stop()

	for _, path := range []string{"/", "/myindex", "/myindex/_search", "/myindex/_doc/1"} {
		resp, err := http.Get(fmt.Sprintf("http://%s%s", addr, path))
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: expected 404, got %d", path, resp.StatusCode)
		}

		var errResp map[string]any
		if err := json.Unmarshal(body, &errResp); err != nil {
			t.Errorf("GET %s: response is not valid JSON: %v", path, err)
		}
		if _, ok := errResp["error"]; !ok {
			t.Errorf("GET %s: expected 'error' key in response", path)
		}
	}
}
