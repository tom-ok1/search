package action

import (
	"strings"
	"testing"

	"gosearch/server/cluster"
)

func TestCatIndices_Execute(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	// Create an index first
	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "testindex",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["testindex"].Close()

	// Execute cat indices
	catAction := NewTransportCatIndicesAction(cs, services)
	resp, err := catAction.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(resp.Indices) != 1 {
		t.Fatalf("expected 1 index, got %d", len(resp.Indices))
	}

	idx := resp.Indices[0]
	if idx.Index != "testindex" {
		t.Errorf("expected index=testindex, got %s", idx.Index)
	}
	if idx.Health != "green" {
		t.Errorf("expected health=green, got %s", idx.Health)
	}
	if idx.Status != "open" {
		t.Errorf("expected status=open, got %s", idx.Status)
	}
	if idx.Pri != 1 {
		t.Errorf("expected pri=1, got %d", idx.Pri)
	}
	if idx.Rep != 0 {
		t.Errorf("expected rep=0, got %d", idx.Rep)
	}

	// Verify FormatText contains index name
	text := resp.FormatText()
	if !strings.Contains(text, "testindex") {
		t.Errorf("FormatText should contain index name, got:\n%s", text)
	}
	if !strings.Contains(text, "health") {
		t.Errorf("FormatText should contain header, got:\n%s", text)
	}
}

func TestCatIndices_MultipleIndices(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	for _, name := range []string{"beta", "alpha"} {
		_, err := createAction.Execute(CreateIndexRequest{
			Name: name,
			Settings: cluster.IndexSettings{
				NumberOfShards:   1,
				NumberOfReplicas: 0,
			},
		})
		if err != nil {
			t.Fatalf("create index %s: %v", name, err)
		}
		defer services[name].Close()
	}

	catAction := NewTransportCatIndicesAction(cs, services)
	resp, err := catAction.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(resp.Indices) != 2 {
		t.Fatalf("expected 2 indices, got %d", len(resp.Indices))
	}

	// Should be sorted alphabetically
	if resp.Indices[0].Index != "alpha" {
		t.Errorf("expected first index=alpha, got %s", resp.Indices[0].Index)
	}
	if resp.Indices[1].Index != "beta" {
		t.Errorf("expected second index=beta, got %s", resp.Indices[1].Index)
	}
}

func TestCatIndices_EmptyCluster(t *testing.T) {
	cs, services, _, _ := newTestDeps(t)

	catAction := NewTransportCatIndicesAction(cs, services)
	resp, err := catAction.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(resp.Indices) != 0 {
		t.Errorf("expected 0 indices, got %d", len(resp.Indices))
	}
}

func TestCatIndices_YellowHealth(t *testing.T) {
	cs, services, dataPath, registry := newTestDeps(t)

	createAction := NewTransportCreateIndexAction(cs, services, dataPath, registry)
	_, err := createAction.Execute(CreateIndexRequest{
		Name: "withreplicas",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 1,
		},
	})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer services["withreplicas"].Close()

	catAction := NewTransportCatIndicesAction(cs, services)
	resp, err := catAction.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if resp.Indices[0].Health != "yellow" {
		t.Errorf("expected health=yellow for index with replicas, got %s", resp.Indices[0].Health)
	}
}
