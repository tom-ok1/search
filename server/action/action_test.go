package action

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

func newTestDeps(t *testing.T) (*cluster.ClusterState, map[string]*index.IndexService, string, *analysis.AnalyzerRegistry) {
	t.Helper()
	cs := cluster.NewClusterState()
	services := make(map[string]*index.IndexService)
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()
	return cs, services, dataPath, registry
}

func TestTransportCreateIndexAction_Name(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	if a.Name() != "indices:admin/create" {
		t.Errorf("unexpected name: %s", a.Name())
	}
}

func TestTransportCreateIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

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
	if meta.Indices["testindex"].Settings.NumberOfShards != 1 {
		t.Errorf("expected 1 shard, got %d", meta.Indices["testindex"].Settings.NumberOfShards)
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
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

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
	if meta.Indices["defaultshards"].Settings.NumberOfShards != 1 {
		t.Errorf("expected default 1 shard, got %d", meta.Indices["defaultshards"].Settings.NumberOfShards)
	}

	services["defaultshards"].Close()
}

func TestTransportCreateIndexAction_EmptyName(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

	_, err := a.Execute(CreateIndexRequest{Name: ""})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestTransportCreateIndexAction_DuplicateName(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

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
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

	invalidNames := []string{"UPPER", "has space", "has/slash", "has*star", ".dotstart"}
	for _, name := range invalidNames {
		_, err := a.Execute(CreateIndexRequest{Name: name})
		if err == nil {
			t.Errorf("expected error for invalid name %q", name)
		}
	}
}

func TestTransportDeleteIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	// First create an index
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
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
	cs, services, dataPath, registry := newTestDeps(t)

	// Create an index with mappings
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
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

func TestTransportIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	// Create an index first
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["docs"].Close()

	indexAction := NewTransportIndexAction(cs, services)

	resp, err := indexAction.Execute(IndexDocumentRequest{
		Index:  "docs",
		ID:     "1",
		Source: json.RawMessage(`{"title":"hello world"}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.ID != "1" {
		t.Errorf("expected id=1, got %s", resp.ID)
	}
	if resp.Index != "docs" {
		t.Errorf("expected index=docs, got %s", resp.Index)
	}
	if resp.Result != "created" {
		t.Errorf("expected result=created, got %s", resp.Result)
	}
}

func TestTransportIndexAction_IndexNotFound(t *testing.T) {
	cs, services, _, _ := newTestDeps(t)
	indexAction := NewTransportIndexAction(cs, services)

	_, err := indexAction.Execute(IndexDocumentRequest{
		Index:  "nonexistent",
		ID:     "1",
		Source: json.RawMessage(`{"title":"hello"}`),
	})
	if err == nil {
		t.Fatal("expected error for nonexistent index")
	}
}

func TestTransportGetAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["docs"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	_, err = indexAction.Execute(IndexDocumentRequest{
		Index:  "docs",
		ID:     "1",
		Source: json.RawMessage(`{"title":"hello world"}`),
	})
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	_, err = refreshAction.Execute(RefreshRequest{Index: "docs"})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}

	getAction := NewTransportGetAction(cs, services)
	resp, err := getAction.Execute(GetDocumentRequest{Index: "docs", ID: "1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected Found=true")
	}
	if resp.ID != "1" {
		t.Errorf("expected id=1, got %s", resp.ID)
	}
	if string(resp.Source) != `{"title":"hello world"}` {
		t.Errorf("unexpected source: %s", resp.Source)
	}
}

func TestTransportGetAction_NotFound(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{Name: "docs"})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["docs"].Close()

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "docs"})

	getAction := NewTransportGetAction(cs, services)
	resp, err := getAction.Execute(GetDocumentRequest{Index: "docs", ID: "999"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Found {
		t.Error("expected Found=false for missing doc")
	}
}

func TestTransportDeleteDocumentAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["docs"].Close()

	indexAction := NewTransportIndexAction(cs, services)
	indexAction.Execute(IndexDocumentRequest{
		Index: "docs", ID: "1",
		Source: json.RawMessage(`{"title":"hello"}`),
	})

	deleteDocAction := NewTransportDeleteDocumentAction(cs, services)
	resp, err := deleteDocAction.Execute(DeleteDocumentRequest{Index: "docs", ID: "1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Result != "deleted" {
		t.Errorf("expected result=deleted, got %s", resp.Result)
	}

	refreshAction := NewTransportRefreshAction(cs, services)
	refreshAction.Execute(RefreshRequest{Index: "docs"})

	getAction := NewTransportGetAction(cs, services)
	getResp, _ := getAction.Execute(GetDocumentRequest{Index: "docs", ID: "1"})
	if getResp.Found {
		t.Error("document should be deleted")
	}
}

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
