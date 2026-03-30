package action

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

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

func TestTransportCreateIndexAction_DefaultRefreshInterval(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

	req := CreateIndexRequest{Name: "defaultrefresh"}
	_, err := a.Execute(req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	meta := cs.Metadata()
	if meta.Indices["defaultrefresh"].Settings.RefreshInterval != cluster.DefaultRefreshInterval {
		t.Errorf("expected default refresh interval %v, got %v",
			cluster.DefaultRefreshInterval, meta.Indices["defaultrefresh"].Settings.RefreshInterval)
	}

	services["defaultrefresh"].Close()
}

func TestTransportCreateIndexAction_CustomRefreshInterval(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

	req := CreateIndexRequest{
		Name: "customrefresh",
		Settings: cluster.IndexSettings{
			RefreshInterval: 5 * time.Second,
		},
	}
	_, err := a.Execute(req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	meta := cs.Metadata()
	if meta.Indices["customrefresh"].Settings.RefreshInterval != 5*time.Second {
		t.Errorf("expected 5s refresh interval, got %v",
			meta.Indices["customrefresh"].Settings.RefreshInterval)
	}

	services["customrefresh"].Close()
}

func TestTransportCreateIndexAction_DisableRefreshInterval(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)
	a := NewTransportCreateIndexAction(cs, services, dataPath, registry)

	req := CreateIndexRequest{
		Name: "norefresh",
		Settings: cluster.IndexSettings{
			RefreshInterval: -1,
		},
	}
	_, err := a.Execute(req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	meta := cs.Metadata()
	if meta.Indices["norefresh"].Settings.RefreshInterval != -1 {
		t.Errorf("expected -1 refresh interval, got %v",
			meta.Indices["norefresh"].Settings.RefreshInterval)
	}

	services["norefresh"].Close()
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
