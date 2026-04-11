package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

func TestGatewayMetaStateFreshStart(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	gw := NewGatewayMetaState()
	cs, services, err := gw.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if len(cs.Metadata().Indices) != 0 {
		t.Errorf("indices = %d, want 0", len(cs.Metadata().Indices))
	}
	if len(services) != 0 {
		t.Errorf("services = %d, want 0", len(services))
	}
}

func TestGatewayMetaStateRecoverIndex(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	// Phase 1: create an index via normal flow
	gw1 := NewGatewayMetaState()
	cs1, services1, err := gw1.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start 1: %v", err)
	}

	meta := &cluster.IndexMetadata{
		Name: "test_index",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
			RefreshInterval:  1 * time.Second,
		},
		Mapping: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
			},
		},
		State: cluster.IndexStateOpen,
	}

	// Create index data directory and shard (simulating create index action)
	indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", "test_index")
	svc, err := createIndexService(meta, indexDataPath, registry)
	if err != nil {
		t.Fatalf("create index service: %v", err)
	}
	services1["test_index"] = svc

	// Update cluster state (this persists to disk via FilePersistedState)
	cs1.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		md.Indices["test_index"] = meta
		return md
	})

	// Close all services (simulating node shutdown)
	for _, s := range services1 {
		s.Close()
	}

	// Phase 2: start fresh and verify recovery
	gw2 := NewGatewayMetaState()
	cs2, services2, err := gw2.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start 2: %v", err)
	}
	defer func() {
		for _, s := range services2 {
			s.Close()
		}
	}()

	if len(cs2.Metadata().Indices) != 1 {
		t.Fatalf("recovered indices = %d, want 1", len(cs2.Metadata().Indices))
	}
	idx := cs2.Metadata().Indices["test_index"]
	if idx == nil {
		t.Fatal("index 'test_index' not recovered")
	}
	if idx.Name != "test_index" {
		t.Errorf("name = %q, want %q", idx.Name, "test_index")
	}
	if _, ok := services2["test_index"]; !ok {
		t.Error("index service for 'test_index' not recovered")
	}
}

func TestGatewayMetaStateMissingIndexDataDir(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	// Write a state file referencing an index whose data dir doesn't exist
	stateDir := filepath.Join(dataPath, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	state := struct {
		Version  int64             `json:"version"`
		Metadata *cluster.Metadata `json:"metadata"`
	}{
		Version: 1,
		Metadata: &cluster.Metadata{
			Indices: map[string]*cluster.IndexMetadata{
				"ghost_index": {
					Name: "ghost_index",
					Settings: cluster.IndexSettings{
						NumberOfShards:   1,
						NumberOfReplicas: 0,
						RefreshInterval:  1 * time.Second,
					},
					Mapping: &mapping.MappingDefinition{
						Properties: map[string]mapping.FieldMapping{},
					},
					State: cluster.IndexStateOpen,
				},
			},
		},
	}
	data, _ := json.MarshalIndent(state, "", "  ")
	os.WriteFile(filepath.Join(stateDir, "cluster_state.json"), data, 0o644)

	gw := NewGatewayMetaState()
	cs, services, err := gw.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		for _, s := range services {
			s.Close()
		}
	}()

	// Ghost index should be removed from metadata
	if len(cs.Metadata().Indices) != 0 {
		t.Errorf("indices after cleanup = %d, want 0", len(cs.Metadata().Indices))
	}
	if len(services) != 0 {
		t.Errorf("services after cleanup = %d, want 0", len(services))
	}
}

func TestGatewayMetaStateCorruptStateFile(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	stateDir := filepath.Join(dataPath, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(stateDir, "cluster_state.json"), []byte("{corrupt"), 0o644)

	gw := NewGatewayMetaState()
	_, _, err := gw.Start(dataPath, registry)
	if err == nil {
		t.Fatal("expected error for corrupt state file, got nil")
	}
}
