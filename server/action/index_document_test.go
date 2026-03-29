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
