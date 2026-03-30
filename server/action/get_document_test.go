package action

import (
	"encoding/json"
	"testing"

	"gosearch/server/mapping"
)

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

func TestGetDocument_RealtimeGet(t *testing.T) {
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

	// GET document WITHOUT calling refresh — should still be found via version map
	getAction := NewTransportGetAction(cs, services)
	resp, err := getAction.Execute(GetDocumentRequest{Index: "docs", ID: "1"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected Found=true without refresh (real-time GET)")
	}
	if resp.Version < 1 {
		t.Errorf("expected Version >= 1, got %d", resp.Version)
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
