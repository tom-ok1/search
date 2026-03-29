package action

import (
	"testing"

	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

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
