package action

import (
	"encoding/json"
	"testing"

	"gosearch/server/mapping"
)

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

func TestIndexDocument_VersionIncrement(t *testing.T) {
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

	// Index doc "1" first time
	resp1, err := indexAction.Execute(IndexDocumentRequest{
		Index:  "docs",
		ID:     "1",
		Source: json.RawMessage(`{"title":"first"}`),
	})
	if err != nil {
		t.Fatalf("first index: %v", err)
	}
	if resp1.Result != "created" {
		t.Errorf("expected result=created on first index, got %s", resp1.Result)
	}
	v1 := resp1.Version

	// Index doc "1" second time (update)
	resp2, err := indexAction.Execute(IndexDocumentRequest{
		Index:  "docs",
		ID:     "1",
		Source: json.RawMessage(`{"title":"second"}`),
	})
	if err != nil {
		t.Fatalf("second index: %v", err)
	}
	if resp2.Result != "updated" {
		t.Errorf("expected result=updated on second index, got %s", resp2.Result)
	}
	v2 := resp2.Version
	if v2 <= v1 {
		t.Errorf("expected version to increment: v1=%d, v2=%d", v1, v2)
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
