# Step 1: Core Infrastructure — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the `server/` package structure, Node lifecycle, ClusterState, transport action registry, and REST controller — producing a running HTTP server that returns 404 for all routes.

**Architecture:** Layered design mirroring Elasticsearch: REST layer → Transport Action layer → Index Service layer → GoSearch Lucene layer. This step builds the skeleton of the first three layers (REST, Transport, Node) with no real business logic yet.

**Tech Stack:** Go 1.23, `net/http` stdlib, existing `gosearch` module

---

### Task 1: Cluster State & Metadata

**Files:**
- Create: `server/cluster/state.go`
- Create: `server/cluster/metadata.go`
- Test: `server/cluster/state_test.go`

- [ ] **Step 1: Write failing test for ClusterState**

```go
// server/cluster/state_test.go
package cluster

import (
	"fmt"
	"testing"
)

func TestClusterState_EmptyMetadata(t *testing.T) {
	cs := NewClusterState()
	meta := cs.Metadata()
	if meta == nil {
		t.Fatal("expected non-nil metadata")
	}
	if len(meta.Indices) != 0 {
		t.Errorf("expected 0 indices, got %d", len(meta.Indices))
	}
}

func TestClusterState_UpdateMetadata(t *testing.T) {
	cs := NewClusterState()
	cs.UpdateMetadata(func(m *Metadata) *Metadata {
		m.Indices["test-index"] = &IndexMetadata{
			Name: "test-index",
			Settings: IndexSettings{
				NumberOfShards:   1,
				NumberOfReplicas: 0,
			},
			NumShards: 1,
			State:     IndexStateOpen,
		}
		return m
	})

	meta := cs.Metadata()
	if len(meta.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(meta.Indices))
	}
	idx := meta.Indices["test-index"]
	if idx.Name != "test-index" {
		t.Errorf("expected name 'test-index', got %q", idx.Name)
	}
	if idx.State != IndexStateOpen {
		t.Errorf("expected state OPEN, got %v", idx.State)
	}
}

func TestClusterState_ConcurrentAccess(t *testing.T) {
	cs := NewClusterState()
	done := make(chan struct{})
	for i := range 10 {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			cs.UpdateMetadata(func(m *Metadata) *Metadata {
				m.Indices[fmt.Sprintf("index-%d", n)] = &IndexMetadata{
					Name: fmt.Sprintf("index-%d", n),
				}
				return m
			})
			_ = cs.Metadata()
		}(i)
	}
	for range 10 {
		<-done
	}
	meta := cs.Metadata()
	if len(meta.Indices) != 10 {
		t.Errorf("expected 10 indices, got %d", len(meta.Indices))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/cluster/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement Metadata types**

```go
// server/cluster/metadata.go
package cluster

// IndexState represents the state of an index.
type IndexState int

const (
	IndexStateOpen IndexState = iota
	IndexStateClosed
)

// Metadata holds the cluster-level metadata about all indices.
type Metadata struct {
	Indices map[string]*IndexMetadata
}

// IndexMetadata describes a single index's configuration.
type IndexMetadata struct {
	Name      string
	Settings  IndexSettings
	NumShards int
	State     IndexState
}

// IndexSettings holds index-level settings.
type IndexSettings struct {
	NumberOfShards   int
	NumberOfReplicas int
}

// NewMetadata creates empty metadata.
func NewMetadata() *Metadata {
	return &Metadata{
		Indices: make(map[string]*IndexMetadata),
	}
}
```

- [ ] **Step 4: Implement ClusterState**

```go
// server/cluster/state.go
package cluster

import "sync"

// ClusterState is the authoritative registry of all indices and their configurations.
type ClusterState struct {
	mu       sync.RWMutex
	metadata *Metadata
}

// NewClusterState creates a ClusterState with empty metadata.
func NewClusterState() *ClusterState {
	return &ClusterState{
		metadata: NewMetadata(),
	}
}

// Metadata returns the current metadata snapshot.
func (cs *ClusterState) Metadata() *Metadata {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.metadata
}

// UpdateMetadata applies a mutation function to the metadata.
func (cs *ClusterState) UpdateMetadata(fn func(*Metadata) *Metadata) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.metadata = fn(cs.metadata)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./server/cluster/ -v`
Expected: PASS — all 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add server/cluster/
git commit -m "feat(server): add ClusterState and Metadata types"
```

---

### Task 2: Transport Action Registry

**Files:**
- Create: `server/transport/action.go`
- Test: `server/transport/action_test.go`

- [ ] **Step 1: Write failing test for ActionRegistry**

```go
// server/transport/action_test.go
package transport

import "testing"

type mockAction struct {
	name string
}

func (a *mockAction) Name() string { return a.name }

func TestActionRegistry_RegisterAndGet(t *testing.T) {
	reg := NewActionRegistry()
	action := &mockAction{name: "indices:data/read/search"}
	reg.Register(action)

	got := reg.Get("indices:data/read/search")
	if got == nil {
		t.Fatal("expected action, got nil")
	}
	if got.Name() != "indices:data/read/search" {
		t.Errorf("expected name 'indices:data/read/search', got %q", got.Name())
	}
}

func TestActionRegistry_GetMissing(t *testing.T) {
	reg := NewActionRegistry()
	got := reg.Get("nonexistent")
	if got != nil {
		t.Errorf("expected nil for missing action, got %v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/transport/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement ActionRegistry**

```go
// server/transport/action.go
package transport

// ActionHandler is the interface that all transport actions implement.
type ActionHandler interface {
	Name() string
}

// ActionRegistry stores transport actions by name.
type ActionRegistry struct {
	handlers map[string]ActionHandler
}

// NewActionRegistry creates an empty registry.
func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{
		handlers: make(map[string]ActionHandler),
	}
}

// Register adds a handler to the registry.
func (r *ActionRegistry) Register(handler ActionHandler) {
	r.handlers[handler.Name()] = handler
}

// Get returns the handler for the given action name, or nil if not found.
func (r *ActionRegistry) Get(name string) ActionHandler {
	return r.handlers[name]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./server/transport/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/transport/
git commit -m "feat(server): add transport ActionRegistry"
```

---

### Task 3: REST Controller

**Files:**
- Create: `server/rest/controller.go`
- Create: `server/rest/request.go`
- Create: `server/rest/response.go`
- Test: `server/rest/controller_test.go`

- [ ] **Step 1: Write failing test for RestController**

```go
// server/rest/controller_test.go
package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubHandler struct {
	routes  []Route
	called  bool
}

func (h *stubHandler) Routes() []Route { return h.routes }
func (h *stubHandler) HandleRequest(req *RestRequest, resp *RestResponseWriter) {
	h.called = true
	resp.WriteJSON(http.StatusOK, map[string]string{"status": "ok"})
}

func TestRestController_UnregisteredRouteReturns404(t *testing.T) {
	rc := NewRestController()
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	w := httptest.NewRecorder()

	rc.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestRestController_RegisterAndDispatch(t *testing.T) {
	rc := NewRestController()
	handler := &stubHandler{
		routes: []Route{{Method: "GET", Path: "/test"}},
	}
	rc.RegisterHandler(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if !handler.called {
		t.Error("expected handler to be called")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestRestController_MethodMismatchReturns405(t *testing.T) {
	rc := NewRestController()
	handler := &stubHandler{
		routes: []Route{{Method: "PUT", Path: "/test"}},
	}
	rc.RegisterHandler(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestRestController_PathParamsExtracted(t *testing.T) {
	rc := NewRestController()
	var captured map[string]string
	rc.RegisterHandlerFunc(Route{Method: "GET", Path: "/{index}/_search"}, func(req *RestRequest, resp *RestResponseWriter) {
		captured = req.Params
		resp.WriteJSON(http.StatusOK, nil)
	})

	req := httptest.NewRequest("GET", "/my-index/_search", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if captured["index"] != "my-index" {
		t.Errorf("expected param index='my-index', got %q", captured["index"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/rest/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement RestRequest and RestResponseWriter**

```go
// server/rest/request.go
package rest

// RestRequest wraps an incoming HTTP request with parsed parameters.
type RestRequest struct {
	Method string
	Params map[string]string // URL path params + query params
	Body   []byte
}
```

```go
// server/rest/response.go
package rest

import (
	"encoding/json"
	"net/http"
)

// RestResponseWriter wraps http.ResponseWriter with convenience methods.
type RestResponseWriter struct {
	http.ResponseWriter
}

// WriteJSON writes a JSON response with the given status code.
func (w *RestResponseWriter) WriteJSON(status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		json.NewEncoder(w).Encode(body)
	}
}

// WriteError writes a JSON error response matching Elasticsearch's format.
func (w *RestResponseWriter) WriteError(status int, errType string, reason string) {
	w.WriteJSON(status, map[string]interface{}{
		"error": map[string]interface{}{
			"type":   errType,
			"reason": reason,
		},
		"status": status,
	})
}
```

- [ ] **Step 4: Implement RestController**

```go
// server/rest/controller.go
package rest

import (
	"io"
	"net/http"
	"strings"
)

// Route defines an HTTP method and path pattern.
type Route struct {
	Method string
	Path   string // e.g., "/{index}/_search"
}

// RestHandler handles REST requests.
type RestHandler interface {
	Routes() []Route
	HandleRequest(req *RestRequest, resp *RestResponseWriter)
}

// HandlerFunc is a function that handles REST requests.
type HandlerFunc func(req *RestRequest, resp *RestResponseWriter)

type routeEntry struct {
	route   Route
	handler HandlerFunc
}

// RestController dispatches HTTP requests to registered REST handlers.
type RestController struct {
	routes []routeEntry
}

// NewRestController creates a new RestController.
func NewRestController() *RestController {
	return &RestController{}
}

// RegisterHandler registers all routes from a RestHandler.
func (rc *RestController) RegisterHandler(handler RestHandler) {
	for _, route := range handler.Routes() {
		rc.routes = append(rc.routes, routeEntry{
			route:   route,
			handler: handler.HandleRequest,
		})
	}
}

// RegisterHandlerFunc registers a single route with a handler function.
func (rc *RestController) RegisterHandlerFunc(route Route, fn HandlerFunc) {
	rc.routes = append(rc.routes, routeEntry{
		route:   route,
		handler: fn,
	})
}

// ServeHTTP implements http.Handler.
func (rc *RestController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := &RestResponseWriter{ResponseWriter: w}

	pathMatched := false
	for _, entry := range rc.routes {
		params, ok := matchPath(entry.route.Path, r.URL.Path)
		if !ok {
			continue
		}
		pathMatched = true
		if entry.route.Method != r.Method {
			continue
		}

		// Merge query params
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}

		body, _ := io.ReadAll(r.Body)
		req := &RestRequest{
			Method: r.Method,
			Params: params,
			Body:   body,
		}
		entry.handler(req, resp)
		return
	}

	if pathMatched {
		resp.WriteError(http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	resp.WriteError(http.StatusNotFound, "not_found", "no handler found for "+r.URL.Path)
}

// matchPath matches a URL path against a pattern with {param} placeholders.
// Returns extracted params and whether the match succeeded.
func matchPath(pattern, path string) (map[string]string, bool) {
	patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	if len(patternParts) != len(pathParts) {
		return nil, false
	}

	params := make(map[string]string)
	for i, pp := range patternParts {
		if strings.HasPrefix(pp, "{") && strings.HasSuffix(pp, "}") {
			paramName := pp[1 : len(pp)-1]
			params[paramName] = pathParts[i]
		} else if pp != pathParts[i] {
			return nil, false
		}
	}
	return params, true
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./server/rest/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add server/rest/
git commit -m "feat(server): add REST controller with route matching"
```

---

### Task 4: Node Lifecycle

**Files:**
- Create: `server/node/node.go`
- Test: `server/node/node_test.go`

- [ ] **Step 1: Write failing test for Node**

```go
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
		HTTPPort: 0, // OS assigns a free port
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

	// Verify the server is responding
	resp, err := http.Get(fmt.Sprintf("http://%s/nonexistent", addr))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should return 404 JSON for unknown routes
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

	// Calling Stop twice should not panic or error
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
	// Should contain JSON error structure
	if len(body) == 0 {
		t.Error("expected non-empty JSON error body")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/node/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement Node**

```go
// server/node/node.go
package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/rest"
	"gosearch/server/transport"
)

// NodeConfig holds configuration for a Node.
type NodeConfig struct {
	DataPath string
	HTTPPort int
}

// Node is the entry point that creates and wires all services.
type Node struct {
	config         NodeConfig
	clusterState   *cluster.ClusterState
	restController *rest.RestController
	actionRegistry *transport.ActionRegistry
	httpServer     *http.Server
	listener       net.Listener
	stopped        bool
}

// NewNode creates a new Node with the given configuration.
func NewNode(config NodeConfig) (*Node, error) {
	cs := cluster.NewClusterState()
	rc := rest.NewRestController()
	ar := transport.NewActionRegistry()

	return &Node{
		config:         config,
		clusterState:   cs,
		restController: rc,
		actionRegistry: ar,
	}, nil
}

// Start starts the HTTP server and returns the listen address.
func (n *Node) Start() (string, error) {
	addr := fmt.Sprintf(":%d", n.config.HTTPPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	n.listener = listener

	n.httpServer = &http.Server{
		Handler: n.restController,
	}

	go n.httpServer.Serve(listener)

	return listener.Addr().String(), nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() error {
	if n.stopped {
		return nil
	}
	n.stopped = true

	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return n.httpServer.Shutdown(ctx)
	}
	return nil
}

// ClusterState returns the node's cluster state.
func (n *Node) ClusterState() *cluster.ClusterState {
	return n.clusterState
}

// ActionRegistry returns the node's action registry.
func (n *Node) ActionRegistry() *transport.ActionRegistry {
	return n.actionRegistry
}

// RestController returns the node's REST controller.
func (n *Node) RestController() *rest.RestController {
	return n.restController
}
```

- [ ] **Step 4: Create go.mod for server package**

The server package lives inside the `gosearch` module. No new `go.mod` is needed — all imports use `gosearch/server/...` paths.

Verify the module path in the existing `go.mod` is `gosearch` (it is).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./server/node/ -v`
Expected: PASS — all 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add server/node/
git commit -m "feat(server): add Node with HTTP lifecycle"
```

---

### Task 5: Verify end-to-end — Node starts and returns 404

**Files:**
- Create: `server/e2e_test.go`

- [ ] **Step 1: Write integration test**

```go
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

	// Test multiple routes all return 404
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

		var errResp map[string]interface{}
		if err := json.Unmarshal(body, &errResp); err != nil {
			t.Errorf("GET %s: response is not valid JSON: %v", path, err)
		}
		if _, ok := errResp["error"]; !ok {
			t.Errorf("GET %s: expected 'error' key in response", path)
		}
	}
}
```

- [ ] **Step 2: Run integration test**

Run: `go test ./server/ -v -run TestNodeStartsAndReturns404`
Expected: PASS

- [ ] **Step 3: Run all server tests**

Run: `go test ./server/... -v`
Expected: PASS — all tests across cluster, transport, rest, node, and e2e pass

- [ ] **Step 4: Commit**

```bash
git add server/e2e_test.go
git commit -m "test(server): add e2e test for node startup"
```

---

### Summary of files created

| File | Responsibility |
|---|---|
| `server/cluster/state.go` | ClusterState with thread-safe metadata access |
| `server/cluster/metadata.go` | Metadata, IndexMetadata, IndexSettings, IndexState types |
| `server/cluster/state_test.go` | Unit tests for ClusterState |
| `server/transport/action.go` | ActionHandler interface and ActionRegistry |
| `server/transport/action_test.go` | Unit tests for ActionRegistry |
| `server/rest/controller.go` | RestController, Route, path matching, HTTP dispatch |
| `server/rest/request.go` | RestRequest type |
| `server/rest/response.go` | RestResponseWriter with WriteJSON/WriteError |
| `server/rest/controller_test.go` | Unit tests for RestController |
| `server/node/node.go` | Node lifecycle — wires ClusterState, REST, transport |
| `server/node/node_test.go` | Unit tests for Node start/stop |
| `server/e2e_test.go` | Integration test — node starts, returns 404 JSON |
