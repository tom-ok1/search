package index_test

import (
	"testing"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

func TestIndexService_AutoRefresh(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "test-auto-refresh",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
			RefreshInterval:  100 * time.Millisecond,
		},
		State: cluster.IndexStateOpen,
	}
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	svc, err := index.NewIndexService(meta, m, dataPath, newTestRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	shard := svc.Shard(0)

	// Index a document
	if _, err := shard.Index("1", []byte(`{"title": "hello world"}`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Before auto-refresh, searcher should be nil
	if shard.Searcher() != nil {
		t.Fatal("expected nil searcher before auto-refresh")
	}

	// Wait for auto-refresh to trigger
	time.Sleep(250 * time.Millisecond)

	// After auto-refresh, searcher should be non-nil
	searcher := shard.Searcher()
	if searcher == nil {
		t.Fatal("expected non-nil searcher after auto-refresh")
	}
	if searcher.Reader().LiveDocCount() != 1 {
		t.Fatalf("expected 1 live doc, got %d", searcher.Reader().LiveDocCount())
	}
}

func TestIndexService_AutoRefreshDisabled(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "test-no-refresh",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
			RefreshInterval:  -1,
		},
		State: cluster.IndexStateOpen,
	}
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	svc, err := index.NewIndexService(meta, m, dataPath, newTestRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	shard := svc.Shard(0)

	// Index a document
	if _, err := shard.Index("1", []byte(`{"title": "hello world"}`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Wait to ensure no auto-refresh occurs
	time.Sleep(200 * time.Millisecond)

	// Searcher should still be nil since auto-refresh is disabled
	if shard.Searcher() != nil {
		t.Fatal("expected nil searcher when auto-refresh is disabled")
	}
}
