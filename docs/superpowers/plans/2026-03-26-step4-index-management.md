# Step 4: Index Management Actions + REST — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up `PUT /myindex`, `GET /myindex`, `DELETE /myindex` end-to-end through REST → Transport Action → Index Service layers.

**Architecture:** Three transport actions (`TransportCreateIndexAction`, `TransportGetIndexAction`, `TransportDeleteIndexAction`) contain all business logic. Three REST handlers parse HTTP and delegate to the transport actions. Node wires everything together at startup.

**Tech Stack:** Go 1.23, standard library `net/http`, existing `server/` packages

---

## File Structure

```
server/
├── cluster/
│   └── metadata.go          # MODIFY — add Mapping field to IndexMetadata
├── action/
│   ├── create_index.go       # CREATE — TransportCreateIndexAction
│   ├── delete_index.go       # CREATE — TransportDeleteIndexAction
│   ├── get_index.go          # CREATE — TransportGetIndexAction
│   └── action_test.go        # CREATE — unit tests for all three actions
├── rest/
│   └── action/
│       ├── create_index.go   # CREATE — RestCreateIndexAction
│       ├── delete_index.go   # CREATE — RestDeleteIndexAction
│       └── get_index.go      # CREATE — RestGetIndexAction
└── node/
    ├── node.go               # MODIFY — add indexServices, analyzer, wiring, close
    └── node_test.go          # MODIFY — add E2E tests for index management
```

---

### Task 1: Add Mapping field to IndexMetadata

**Files:**
- Modify: `server/cluster/metadata.go`

This is needed so `GetIndexAction` can return the mapping. No circular dependency: `cluster` → `mapping` is fine since `mapping` does not import `cluster`.

- [ ] **Step 1: Add the Mapping field**

In `server/cluster/metadata.go`, add the import and the field:

```go
import "gosearch/server/mapping"

type IndexMetadata struct {
	Name      string
	Settings  IndexSettings
	Mapping   *mapping.MappingDefinition
	NumShards int
	State     IndexState
}
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/...`
Expected: All existing tests pass (the field is additive, no existing code sets it).

---

### Task 2: Transport Actions

**Files:**
- Create: `server/action/create_index.go`
- Create: `server/action/delete_index.go`
- Create: `server/action/get_index.go`
- Create: `server/action/action_test.go`

All three actions implement `transport.ActionHandler` (the `Name() string` method). Each also has an `Execute` method with typed request/response.

The actions need access to shared node state. Rather than importing `node` (which would be circular), each action receives the dependencies it needs via constructor injection:
- `ClusterState` — to read/update metadata
- `indexServices map[string]*index.IndexService` — the live index map (pointer to node's map)
- `dataPath string` — base path for index data directories
- `analyzer *analysis.Analyzer` — default analyzer for creating new index services

- [ ] **Step 1: Write the test file with all transport action tests**

Create `server/action/action_test.go`:

```go
package action

import (
	"os"
	"path/filepath"
	"testing"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

func newTestDeps(t *testing.T) (*cluster.ClusterState, map[string]*index.IndexService, string, *analysis.Analyzer) {
	t.Helper()
	cs := cluster.NewClusterState()
	services := make(map[string]*index.IndexService)
	dataPath := t.TempDir()
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter())
	return cs, services, dataPath, analyzer
}

func TestTransportCreateIndexAction_Name(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)
	if a.Name() != "indices:admin/create" {
		t.Errorf("unexpected name: %s", a.Name())
	}
}

func TestTransportCreateIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)

	req := CreateIndexRequest{
		Name: "testindex",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	}

	resp, err := a.Execute(req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}
	if resp.Index != "testindex" {
		t.Errorf("expected index=testindex, got %s", resp.Index)
	}

	// Verify cluster state updated
	meta := cs.Metadata()
	if meta.Indices["testindex"] == nil {
		t.Fatal("index not in cluster state")
	}
	if meta.Indices["testindex"].NumShards != 1 {
		t.Errorf("expected 1 shard, got %d", meta.Indices["testindex"].NumShards)
	}

	// Verify index service created
	if services["testindex"] == nil {
		t.Fatal("index service not registered")
	}

	// Verify data directory created
	indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", "testindex")
	if _, err := os.Stat(indexDataPath); os.IsNotExist(err) {
		t.Error("index data directory not created")
	}

	// Cleanup
	services["testindex"].Close()
}

func TestTransportCreateIndexAction_DefaultShards(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)

	req := CreateIndexRequest{
		Name: "defaultshards",
	}

	resp, err := a.Execute(req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	meta := cs.Metadata()
	if meta.Indices["defaultshards"].NumShards != 1 {
		t.Errorf("expected default 1 shard, got %d", meta.Indices["defaultshards"].NumShards)
	}

	services["defaultshards"].Close()
}

func TestTransportCreateIndexAction_EmptyName(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)

	_, err := a.Execute(CreateIndexRequest{Name: ""})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestTransportCreateIndexAction_DuplicateName(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)

	req := CreateIndexRequest{Name: "dup"}
	if _, err := a.Execute(req); err != nil {
		t.Fatalf("first create: %v", err)
	}

	_, err := a.Execute(req)
	if err == nil {
		t.Fatal("expected error for duplicate index")
	}

	services["dup"].Close()
}

func TestTransportCreateIndexAction_InvalidName(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)

	invalidNames := []string{"UPPER", "has space", "has/slash", "has*star", ".dotstart"}
	for _, name := range invalidNames {
		_, err := a.Execute(CreateIndexRequest{Name: name})
		if err == nil {
			t.Errorf("expected error for invalid name %q", name)
		}
	}
}

func TestTransportDeleteIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)

	// First create an index
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)
	if _, err := createAction.Execute(CreateIndexRequest{Name: "todelete"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Now delete it
	deleteAction := NewTransportDeleteIndexAction(cs, services, dataPath)
	resp, err := deleteAction.Execute(DeleteIndexRequest{Name: "todelete"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}

	// Verify removed from cluster state
	if cs.Metadata().Indices["todelete"] != nil {
		t.Error("index still in cluster state")
	}

	// Verify index service removed
	if services["todelete"] != nil {
		t.Error("index service still registered")
	}

	// Verify data directory cleaned up
	indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", "todelete")
	if _, err := os.Stat(indexDataPath); !os.IsNotExist(err) {
		t.Error("index data directory not cleaned up")
	}
}

func TestTransportDeleteIndexAction_NotFound(t *testing.T) {
	cs, services, dataPath, _ := newTestDeps(t)
	a := NewTransportDeleteIndexAction(cs, services, dataPath)

	_, err := a.Execute(DeleteIndexRequest{Name: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent index")
	}
}

func TestTransportGetIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, analyzer := newTestDeps(t)

	// Create an index with mappings
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, analyzer)
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}
	if _, err := createAction.Execute(CreateIndexRequest{
		Name:     "getme",
		Settings: cluster.IndexSettings{NumberOfShards: 1},
		Mappings: m,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	getAction := NewTransportGetIndexAction(cs)
	resp, err := getAction.Execute(GetIndexRequest{Name: "getme"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if resp.Name != "getme" {
		t.Errorf("expected name=getme, got %s", resp.Name)
	}
	if resp.Settings.NumberOfShards != 1 {
		t.Errorf("expected 1 shard, got %d", resp.Settings.NumberOfShards)
	}
	if resp.Mapping == nil || resp.Mapping.Properties["title"].Type != mapping.FieldTypeText {
		t.Error("mapping not returned correctly")
	}

	services["getme"].Close()
}

func TestTransportGetIndexAction_NotFound(t *testing.T) {
	cs, _, _, _ := newTestDeps(t)
	a := NewTransportGetIndexAction(cs)

	_, err := a.Execute(GetIndexRequest{Name: "nope"})
	if err == nil {
		t.Fatal("expected error for nonexistent index")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/action/...`
Expected: Compilation errors (package doesn't exist yet).

- [ ] **Step 3: Implement TransportCreateIndexAction**

Create `server/action/create_index.go`:

```go
package action

import (
	"fmt"
	"path/filepath"
	"regexp"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

var validIndexName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

type CreateIndexRequest struct {
	Name     string
	Settings cluster.IndexSettings
	Mappings *mapping.MappingDefinition
}

type CreateIndexResponse struct {
	Acknowledged bool
	Index        string
}

type TransportCreateIndexAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	dataPath      string
	analyzer      *analysis.Analyzer
}

func NewTransportCreateIndexAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
	dataPath string,
	analyzer *analysis.Analyzer,
) *TransportCreateIndexAction {
	return &TransportCreateIndexAction{
		clusterState:  cs,
		indexServices: services,
		dataPath:      dataPath,
		analyzer:      analyzer,
	}
}

func (a *TransportCreateIndexAction) Name() string {
	return "indices:admin/create"
}

func (a *TransportCreateIndexAction) Execute(req CreateIndexRequest) (CreateIndexResponse, error) {
	if req.Name == "" {
		return CreateIndexResponse{}, fmt.Errorf("index name must not be empty")
	}
	if !validIndexName.MatchString(req.Name) {
		return CreateIndexResponse{}, fmt.Errorf("invalid index name [%s]: must be lowercase, start with a letter or digit, and contain only [a-z0-9._-]", req.Name)
	}

	// Check for duplicate
	if a.clusterState.Metadata().Indices[req.Name] != nil {
		return CreateIndexResponse{}, fmt.Errorf("index [%s] already exists", req.Name)
	}

	// Default to 1 shard if not specified
	numShards := req.Settings.NumberOfShards
	if numShards <= 0 {
		numShards = 1
	}

	// Default to empty mapping if not provided
	m := req.Mappings
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}

	// Build IndexMetadata
	meta := &cluster.IndexMetadata{
		Name: req.Name,
		Settings: cluster.IndexSettings{
			NumberOfShards:   numShards,
			NumberOfReplicas: req.Settings.NumberOfReplicas,
		},
		Mapping:   m,
		NumShards: numShards,
		State:     cluster.IndexStateOpen,
	}

	// Create IndexService
	indexDataPath := filepath.Join(a.dataPath, "nodes", "0", "indices", req.Name)
	svc, err := index.NewIndexService(meta, m, indexDataPath, a.analyzer)
	if err != nil {
		return CreateIndexResponse{}, fmt.Errorf("create index service: %w", err)
	}

	// Update cluster state
	a.clusterState.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		md.Indices[req.Name] = meta
		return md
	})

	// Register index service
	a.indexServices[req.Name] = svc

	return CreateIndexResponse{
		Acknowledged: true,
		Index:        req.Name,
	}, nil
}
```

- [ ] **Step 4: Implement TransportDeleteIndexAction**

Create `server/action/delete_index.go`:

```go
package action

import (
	"fmt"
	"os"
	"path/filepath"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type DeleteIndexRequest struct {
	Name string
}

type DeleteIndexResponse struct {
	Acknowledged bool
}

type TransportDeleteIndexAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	dataPath      string
}

func NewTransportDeleteIndexAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
	dataPath string,
) *TransportDeleteIndexAction {
	return &TransportDeleteIndexAction{
		clusterState:  cs,
		indexServices: services,
		dataPath:      dataPath,
	}
}

func (a *TransportDeleteIndexAction) Name() string {
	return "indices:admin/delete"
}

func (a *TransportDeleteIndexAction) Execute(req DeleteIndexRequest) (DeleteIndexResponse, error) {
	// Verify index exists
	if a.clusterState.Metadata().Indices[req.Name] == nil {
		return DeleteIndexResponse{}, fmt.Errorf("no such index [%s]", req.Name)
	}

	// Close IndexService
	svc := a.indexServices[req.Name]
	if svc != nil {
		if err := svc.Close(); err != nil {
			return DeleteIndexResponse{}, fmt.Errorf("close index [%s]: %w", req.Name, err)
		}
	}

	// Remove from cluster state
	a.clusterState.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		delete(md.Indices, req.Name)
		return md
	})

	// Remove from index services map
	delete(a.indexServices, req.Name)

	// Clean up data directory
	indexDataPath := filepath.Join(a.dataPath, "nodes", "0", "indices", req.Name)
	os.RemoveAll(indexDataPath)

	return DeleteIndexResponse{Acknowledged: true}, nil
}
```

- [ ] **Step 5: Implement TransportGetIndexAction**

Create `server/action/get_index.go`:

```go
package action

import (
	"fmt"

	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

type GetIndexRequest struct {
	Name string
}

type GetIndexResponse struct {
	Name     string
	Settings cluster.IndexSettings
	Mapping  *mapping.MappingDefinition
}

type TransportGetIndexAction struct {
	clusterState *cluster.ClusterState
}

func NewTransportGetIndexAction(cs *cluster.ClusterState) *TransportGetIndexAction {
	return &TransportGetIndexAction{clusterState: cs}
}

func (a *TransportGetIndexAction) Name() string {
	return "indices:admin/get"
}

func (a *TransportGetIndexAction) Execute(req GetIndexRequest) (GetIndexResponse, error) {
	meta := a.clusterState.Metadata().Indices[req.Name]
	if meta == nil {
		return GetIndexResponse{}, fmt.Errorf("no such index [%s]", req.Name)
	}

	return GetIndexResponse{
		Name:     meta.Name,
		Settings: meta.Settings,
		Mapping:  meta.Mapping,
	}, nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/action/... -v`
Expected: All tests pass.

---

### Task 3: REST Handlers

**Files:**
- Create: `server/rest/action/create_index.go`
- Create: `server/rest/action/delete_index.go`
- Create: `server/rest/action/get_index.go`

Each REST handler implements `rest.RestHandler` (`Routes() []Route`, `HandleRequest(req, resp)`). They parse the HTTP request and delegate to the corresponding transport action.

- [ ] **Step 1: Implement RestCreateIndexAction**

Create `server/rest/action/create_index.go`:

```go
package action

import (
	"encoding/json"
	"net/http"

	serveraction "gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
	"gosearch/server/rest"
)

type RestCreateIndexAction struct {
	action *serveraction.TransportCreateIndexAction
}

func NewRestCreateIndexAction(action *serveraction.TransportCreateIndexAction) *RestCreateIndexAction {
	return &RestCreateIndexAction{action: action}
}

func (h *RestCreateIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "PUT", Path: "/{index}"},
	}
}

func (h *RestCreateIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	var body struct {
		Settings *cluster.IndexSettings    `json:"settings"`
		Mappings *mappingsBody             `json:"mappings"`
	}

	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			resp.WriteError(http.StatusBadRequest, "parse_exception", "failed to parse request body: "+err.Error())
			return
		}
	}

	createReq := serveraction.CreateIndexRequest{
		Name: indexName,
	}

	if body.Settings != nil {
		createReq.Settings = *body.Settings
	}

	if body.Mappings != nil {
		createReq.Mappings = &mapping.MappingDefinition{
			Properties: body.Mappings.Properties,
		}
	}

	result, err := h.action.Execute(createReq)
	if err != nil {
		// Determine error type based on message
		errMsg := err.Error()
		if contains(errMsg, "already exists") {
			resp.WriteError(http.StatusBadRequest, "resource_already_exists_exception", errMsg)
		} else if contains(errMsg, "invalid index name") || contains(errMsg, "must not be empty") {
			resp.WriteError(http.StatusBadRequest, "invalid_index_name_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "index_creation_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"acknowledged": result.Acknowledged,
		"index":        result.Index,
	})
}

// mappingsBody is used for JSON parsing of the mappings section of the create index request.
type mappingsBody struct {
	Properties map[string]mapping.FieldMapping `json:"properties"`
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Implement RestDeleteIndexAction**

Create `server/rest/action/delete_index.go`:

```go
package action

import (
	"net/http"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestDeleteIndexAction struct {
	action *serveraction.TransportDeleteIndexAction
}

func NewRestDeleteIndexAction(action *serveraction.TransportDeleteIndexAction) *RestDeleteIndexAction {
	return &RestDeleteIndexAction{action: action}
}

func (h *RestDeleteIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "DELETE", Path: "/{index}"},
	}
}

func (h *RestDeleteIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	result, err := h.action.Execute(serveraction.DeleteIndexRequest{Name: indexName})
	if err != nil {
		errMsg := err.Error()
		if contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "index_deletion_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"acknowledged": result.Acknowledged,
	})
}
```

- [ ] **Step 3: Implement RestGetIndexAction**

Create `server/rest/action/get_index.go`:

```go
package action

import (
	"net/http"

	serveraction "gosearch/server/action"
	"gosearch/server/mapping"
	"gosearch/server/rest"
)

type RestGetIndexAction struct {
	action *serveraction.TransportGetIndexAction
}

func NewRestGetIndexAction(action *serveraction.TransportGetIndexAction) *RestGetIndexAction {
	return &RestGetIndexAction{action: action}
}

func (h *RestGetIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "GET", Path: "/{index}"},
	}
}

func (h *RestGetIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	result, err := h.action.Execute(serveraction.GetIndexRequest{Name: indexName})
	if err != nil {
		errMsg := err.Error()
		if contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "internal_error", errMsg)
		}
		return
	}

	// Build ES-compatible response: { "indexname": { "settings": {...}, "mappings": {...} } }
	mappingsResp := map[string]any{}
	if result.Mapping != nil && len(result.Mapping.Properties) > 0 {
		props := make(map[string]any, len(result.Mapping.Properties))
		for name, fm := range result.Mapping.Properties {
			prop := map[string]any{"type": string(fm.Type)}
			if fm.Analyzer != "" {
				prop["analyzer"] = fm.Analyzer
			}
			props[name] = prop
		}
		mappingsResp["properties"] = props
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		indexName: map[string]any{
			"settings": map[string]any{
				"number_of_shards":   result.Settings.NumberOfShards,
				"number_of_replicas": result.Settings.NumberOfReplicas,
			},
			"mappings": mappingsResp,
		},
	})
}
```

- [ ] **Step 4: Verify compilation**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go build ./server/...`
Expected: Compiles successfully.

---

### Task 4: Node Wiring

**Files:**
- Modify: `server/node/node.go`

Add `indexServices` map and a default analyzer to Node. Create and register all transport actions + REST handlers in `NewNode`. Close all IndexServices in `Stop`.

- [ ] **Step 1: Modify Node to hold index services, create actions, and register handlers**

Update `server/node/node.go` to:

```go
package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"gosearch/analysis"
	"gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/rest"
	restaction "gosearch/server/rest/action"
	"gosearch/server/transport"
)

type NodeConfig struct {
	DataPath string
	HTTPPort int
}

type Node struct {
	config         NodeConfig
	clusterState   *cluster.ClusterState
	indexServices  map[string]*index.IndexService
	restController *rest.RestController
	actionRegistry *transport.ActionRegistry
	analyzer       *analysis.Analyzer
	httpServer     *http.Server
	listener       net.Listener
	stopped        bool
}

func NewNode(config NodeConfig) (*Node, error) {
	cs := cluster.NewClusterState()
	rc := rest.NewRestController()
	ar := transport.NewActionRegistry()
	indexServices := make(map[string]*index.IndexService)
	analyzer := analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), analysis.NewLowerCaseFilter())

	n := &Node{
		config:         config,
		clusterState:   cs,
		indexServices:  indexServices,
		restController: rc,
		actionRegistry: ar,
		analyzer:       analyzer,
	}

	// Create transport actions
	createAction := action.NewTransportCreateIndexAction(cs, indexServices, config.DataPath, analyzer)
	deleteAction := action.NewTransportDeleteIndexAction(cs, indexServices, config.DataPath)
	getAction := action.NewTransportGetIndexAction(cs)

	// Register transport actions
	ar.Register(createAction)
	ar.Register(deleteAction)
	ar.Register(getAction)

	// Create and register REST handlers
	rc.RegisterHandler(restaction.NewRestCreateIndexAction(createAction))
	rc.RegisterHandler(restaction.NewRestDeleteIndexAction(deleteAction))
	rc.RegisterHandler(restaction.NewRestGetIndexAction(getAction))

	return n, nil
}

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

func (n *Node) Stop() error {
	if n.stopped {
		return nil
	}
	n.stopped = true

	// Close all index services
	for name, svc := range n.indexServices {
		svc.Close()
		delete(n.indexServices, name)
	}

	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return n.httpServer.Shutdown(ctx)
	}
	return nil
}

func (n *Node) ClusterState() *cluster.ClusterState {
	return n.clusterState
}

func (n *Node) ActionRegistry() *transport.ActionRegistry {
	return n.actionRegistry
}

func (n *Node) RestController() *rest.RestController {
	return n.restController
}

func (n *Node) IndexService(name string) *index.IndexService {
	return n.indexServices[name]
}
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/...`
Expected: All existing tests pass.

---

### Task 5: E2E Tests

**Files:**
- Modify: `server/node/node_test.go`

Add end-to-end tests that start a Node, use `net/http` to PUT/GET/DELETE indices, and verify responses and error cases.

- [ ] **Step 1: Add E2E tests for index management**

Append the following tests to `server/node/node_test.go`:

```go
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
```

Also add the new imports to the test file: `"encoding/json"` and `"strings"`.

- [ ] **Step 2: Run all tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/... -v`
Expected: All tests pass, including existing tests and new E2E tests.

- [ ] **Step 3: Run the full test suite**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./...`
Expected: All tests across the entire project pass.

---

### Task 6: Route Conflict Resolution

There is a potential route conflict: `GET /{index}` (RestGetIndexAction) and `PUT /{index}` (RestCreateIndexAction) and `DELETE /{index}` (RestDeleteIndexAction) all match the same path pattern. The RestController's `matchPath` function matches by path first, then checks method. Since all three handlers register different methods on the same path, the routing works correctly — `ServeHTTP` iterates routes, matches path first, then checks method.

However, there may be a conflict with the existing 404 handler behavior for paths like `GET /nonexistent` — the GET handler will fire even though no index exists. This is correct: the handler should return a 404 JSON error for nonexistent indices, which is exactly what `TransportGetIndexAction` does.

No code changes needed — this is a verification step.

- [ ] **Step 1: Verify route handling with curl**

This is optional manual verification. Start a node and test:
```bash
# Create
curl -X PUT http://localhost:9200/myindex -H 'Content-Type: application/json' -d '{"settings":{"number_of_shards":1},"mappings":{"properties":{"title":{"type":"text"}}}}'

# Get
curl http://localhost:9200/myindex

# Delete
curl -X DELETE http://localhost:9200/myindex
```

The E2E tests in Task 5 already cover this, so this is informational.
