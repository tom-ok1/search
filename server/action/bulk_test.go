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
	if resp.Items[1].Status != 404 {
		t.Errorf("expected status 404 for missing index, got %d", resp.Items[1].Status)
	}
	if resp.Items[1].Error.Type != "index_not_found_exception" {
		t.Errorf("expected error type index_not_found_exception, got %s", resp.Items[1].Error.Type)
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

func TestTransportBulkAction_CreateConflict(t *testing.T) {
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

	// Index a document first, then refresh so it becomes visible.
	bulkAction := NewTransportBulkAction(cs, services)
	bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"original"}`)},
		},
	})
	svc := services["docs"]
	svc.Shard(0).Refresh()

	// Attempt to "create" a document with the same ID should fail.
	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "create", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"duplicate"}`)},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !resp.Errors {
		t.Fatal("expected Errors=true for create conflict")
	}
	if resp.Items[0].Status != 409 {
		t.Errorf("expected status 409, got %d", resp.Items[0].Status)
	}
	if resp.Items[0].Error.Type != "version_conflict_engine_exception" {
		t.Errorf("expected version_conflict_engine_exception, got %s", resp.Items[0].Error.Type)
	}
}

func TestTransportBulkAction_IndexOverwrites(t *testing.T) {
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

	// Index, refresh, then index again with same ID — should succeed (overwrite).
	bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"original"}`)},
		},
	})
	services["docs"].Shard(0).Refresh()

	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "1", Source: json.RawMessage(`{"title":"updated"}`)},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Errors {
		t.Errorf("expected no errors for index overwrite, got: %+v", resp.Items)
	}
	if resp.Items[0].Status != 201 {
		t.Errorf("expected status 201, got %d", resp.Items[0].Status)
	}
}

func TestTransportBulkAction_ResponseOrderPreserved(t *testing.T) {
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

	// Mix of valid and invalid items — response order must match request order.
	resp, err := bulkAction.Execute(BulkRequest{
		Items: []BulkItem{
			{Action: "index", Index: "docs", ID: "a", Source: json.RawMessage(`{"title":"a"}`)},
			{Action: "index", Index: "missing", ID: "b", Source: json.RawMessage(`{"title":"b"}`)},
			{Action: "index", Index: "docs", ID: "c", Source: json.RawMessage(`{"title":"c"}`)},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(resp.Items) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != "a" || resp.Items[0].Error != nil {
		t.Errorf("item 0: expected success for ID=a, got %+v", resp.Items[0])
	}
	if resp.Items[1].ID != "b" || resp.Items[1].Error == nil {
		t.Errorf("item 1: expected error for ID=b, got %+v", resp.Items[1])
	}
	if resp.Items[2].ID != "c" || resp.Items[2].Error != nil {
		t.Errorf("item 2: expected success for ID=c, got %+v", resp.Items[2])
	}
}
