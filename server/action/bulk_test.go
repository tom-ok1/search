package action

import (
	"encoding/json"
	"testing"

	"gosearch/server/mapping"
)

func TestTransportBulkAction_Execute(t *testing.T) {
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

	bulkAction := NewTransportBulkAction(cs, services)
	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"first"}`)},
			{Action: "index", Index: "docs", ID: "2", Source: json.RawMessage(`{"title":"second"}`)},
			{Action: "delete", Index: "docs", ID: "1"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Errors {
		t.Error("expected no errors")
	}
	if len(resp.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(resp.Items))
	}
	if resp.Items[0].Status != 201 {
		t.Errorf("expected status 201 for index, got %d", resp.Items[0].Status)
	}
	if resp.Items[2].Action != "delete" {
		t.Errorf("expected action=delete, got %s", resp.Items[2].Action)
	}
}

func TestTransportBulkAction_PartialErrors(t *testing.T) {
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

	bulkAction := NewTransportBulkAction(cs, services)
	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"ok"}`)},
			{Action: "index", Index: "nonexistent", ID: "1", Source: json.RawMessage(`{"title":"fail"}`)},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Errors {
		t.Error("expected Errors=true for partial failure")
	}
	if resp.Items[0].Error != nil {
		t.Errorf("first item should succeed, got error: %v", resp.Items[0].Error)
	}
	if resp.Items[1].Error == nil {
		t.Error("second item should fail for nonexistent index")
	}
}

func TestTransportBulkAction_CreateAction(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createIdxAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	createIdxAction.Execute(CreateIndexRequest{
		Name: "docs",
		Mappings: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText},
			},
		},
	})
	defer services["docs"].Close()

	bulkAction := NewTransportBulkAction(cs, services)
	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{
				Action: "create",
				Index:  "docs",
				ID:     "1",
				Source: json.RawMessage(`{"title":"hello"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Errors {
		t.Errorf("expected no errors, got errors: %+v", resp.Items)
	}
	if resp.Items[0].Status != 201 {
		t.Errorf("expected status 201, got %d", resp.Items[0].Status)
	}
}
