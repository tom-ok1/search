package action

import (
	"strings"
	"testing"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

func TestCatHealth(t *testing.T) {
	cs := cluster.NewClusterState()
	indexServices := make(map[string]*index.IndexService)

	action := NewTransportCatHealthAction(cs, indexServices)

	resp, err := action.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != "green" {
		t.Errorf("expected status green, got %s", resp.Status)
	}
	if resp.ClusterName != "gosearch" {
		t.Errorf("expected cluster name gosearch, got %s", resp.ClusterName)
	}
	if resp.NodeTotal != 1 {
		t.Errorf("expected node total 1, got %d", resp.NodeTotal)
	}
	if resp.NodeData != 1 {
		t.Errorf("expected node data 1, got %d", resp.NodeData)
	}
	if resp.Shards != 0 {
		t.Errorf("expected 0 shards, got %d", resp.Shards)
	}

	text := resp.FormatText()
	if !strings.Contains(text, "green") {
		t.Errorf("expected FormatText to contain 'green', got: %s", text)
	}
	if !strings.Contains(text, "gosearch") {
		t.Errorf("expected FormatText to contain 'gosearch', got: %s", text)
	}
}

func TestCatHealthWithIndices(t *testing.T) {
	cs := cluster.NewClusterState()
	indexServices := make(map[string]*index.IndexService)

	// Add an index with replicas to get yellow status
	cs.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		md.Indices["test-index"] = &cluster.IndexMetadata{
			Name: "test-index",
			Settings: cluster.IndexSettings{
				NumberOfShards:   3,
				NumberOfReplicas: 1,
			},
			State: cluster.IndexStateOpen,
		}
		return md
	})

	action := NewTransportCatHealthAction(cs, indexServices)

	resp, err := action.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != "yellow" {
		t.Errorf("expected status yellow, got %s", resp.Status)
	}
	if resp.Shards != 3 {
		t.Errorf("expected 3 shards, got %d", resp.Shards)
	}
	if resp.Pri != 3 {
		t.Errorf("expected 3 primary shards, got %d", resp.Pri)
	}
}
