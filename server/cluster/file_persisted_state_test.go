package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gosearch/server/mapping"
)

func TestFilePersistedStateRoundtrip(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "_state")

	fps, err := NewFilePersistedState(stateDir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Initially empty
	md := fps.GetMetadata()
	if len(md.Indices) != 0 {
		t.Fatalf("initial indices = %d, want 0", len(md.Indices))
	}

	// Set metadata with an index
	meta := NewMetadata()
	meta.Indices["test"] = &IndexMetadata{
		Name: "test",
		Settings: IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
			RefreshInterval:  1 * time.Second,
		},
		Mapping: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
			},
		},
		State: IndexStateOpen,
	}
	fps.SetMetadata(meta)

	// Load from a fresh instance pointing to same directory
	fps2, err := NewFilePersistedState(stateDir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	md2 := fps2.GetMetadata()
	if len(md2.Indices) != 1 {
		t.Fatalf("reloaded indices = %d, want 1", len(md2.Indices))
	}
	idx := md2.Indices["test"]
	if idx == nil {
		t.Fatal("index 'test' not found after reload")
	}
	if idx.Name != "test" {
		t.Errorf("name = %q, want %q", idx.Name, "test")
	}
	if idx.Settings.NumberOfShards != 1 {
		t.Errorf("shards = %d, want 1", idx.Settings.NumberOfShards)
	}
	if idx.Mapping.Properties["title"].Type != mapping.FieldTypeText {
		t.Errorf("title type = %q, want %q", idx.Mapping.Properties["title"].Type, mapping.FieldTypeText)
	}
}

func TestFilePersistedStateVersionIncrement(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "_state")

	fps, err := NewFilePersistedState(stateDir)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if fps.Version() != 0 {
		t.Errorf("initial version = %d, want 0", fps.Version())
	}

	fps.SetMetadata(NewMetadata())
	if fps.Version() != 1 {
		t.Errorf("after first set version = %d, want 1", fps.Version())
	}

	fps.SetMetadata(NewMetadata())
	if fps.Version() != 2 {
		t.Errorf("after second set version = %d, want 2", fps.Version())
	}
}

func TestFilePersistedStateCleansStaleTmpFile(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a stale tmp file (simulates crash during previous write)
	tmpFile := filepath.Join(stateDir, "cluster_state.tmp")
	if err := os.WriteFile(tmpFile, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewFilePersistedState(stateDir)
	if err != nil {
		t.Fatalf("new with stale tmp: %v", err)
	}

	// Tmp file should be cleaned up
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("stale tmp file was not cleaned up")
	}
}

func TestFilePersistedStateCorruptFileReturnsError(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write corrupt JSON
	stateFile := filepath.Join(stateDir, "cluster_state.json")
	if err := os.WriteFile(stateFile, []byte("{corrupt"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewFilePersistedState(stateDir)
	if err == nil {
		t.Fatal("expected error for corrupt state file, got nil")
	}
}
