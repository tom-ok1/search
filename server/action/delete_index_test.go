package action

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTransportDeleteIndexAction_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	// First create an index
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
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
