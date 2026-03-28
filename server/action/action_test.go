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
