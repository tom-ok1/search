package action

import (
	"encoding/json"
	"testing"

	"gosearch/server/mapping"
)

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
