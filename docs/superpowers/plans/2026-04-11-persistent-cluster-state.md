# Persistent Cluster State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist cluster state (index metadata) to disk so nodes automatically recover all indices on restart.

**Architecture:** A new `FilePersistedState` implements the existing `PersistedState` interface, writing metadata as JSON to `{dataPath}/_state/cluster_state.json` with atomic tmp+rename. A new `GatewayMetaState` (in `server/gateway/`) handles startup recovery — loading persisted state and reopening all index services. `Node.NewNode()` delegates to `GatewayMetaState` instead of creating empty in-memory state.

**Tech Stack:** Go standard library (`encoding/json`, `os`, `path/filepath`), existing `PersistedState` interface, existing `index.NewIndexService`.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `server/cluster/file_persisted_state.go` | Create | `FilePersistedState` struct — JSON read/write with atomic commits, version tracking |
| `server/cluster/file_persisted_state_test.go` | Create | Unit tests for `FilePersistedState` |
| `server/cluster/metadata.go` | Modify | Add JSON struct tags and custom marshal/unmarshal for `IndexState` and `RefreshInterval` |
| `server/cluster/metadata_test.go` | Create | JSON serialization roundtrip tests for `Metadata` |
| `server/mapping/mapping.go` | Modify | Add JSON struct tags to `MappingDefinition` and `FieldMapping` |
| `server/gateway/gateway_meta_state.go` | Create | `GatewayMetaState` — load persisted state, recover index services on startup |
| `server/gateway/gateway_meta_state_test.go` | Create | Unit tests for `GatewayMetaState` recovery logic |
| `server/node/node.go` | Modify | Use `GatewayMetaState` in `NewNode()` instead of `NewClusterState()` |
| `server/node/node_test.go` | Modify | Add integration test: create index → stop → restart → index recovered |

---

### Task 1: JSON Serialization for Metadata Types

Add JSON struct tags to `Metadata`, `IndexMetadata`, `IndexSettings`, `MappingDefinition`, and `FieldMapping`. Add custom marshal/unmarshal for `IndexState` (string) and `RefreshInterval` (duration string).

**Files:**
- Modify: `server/cluster/metadata.go`
- Modify: `server/mapping/mapping.go`
- Create: `server/cluster/metadata_test.go`

- [ ] **Step 1: Write the failing test for Metadata JSON roundtrip**

Create `server/cluster/metadata_test.go`:

```go
package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"gosearch/server/mapping"
)

func TestMetadataJSONRoundtrip(t *testing.T) {
	original := &Metadata{
		Indices: map[string]*IndexMetadata{
			"test_index": {
				Name: "test_index",
				Settings: IndexSettings{
					NumberOfShards:   2,
					NumberOfReplicas: 1,
					RefreshInterval:  5 * time.Second,
				},
				Mapping: &mapping.MappingDefinition{
					Properties: map[string]mapping.FieldMapping{
						"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
						"count": {Type: mapping.FieldTypeLong},
					},
				},
				State: IndexStateOpen,
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored Metadata
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	idx := restored.Indices["test_index"]
	if idx == nil {
		t.Fatal("index 'test_index' not found after roundtrip")
	}
	if idx.Name != "test_index" {
		t.Errorf("name = %q, want %q", idx.Name, "test_index")
	}
	if idx.Settings.NumberOfShards != 2 {
		t.Errorf("shards = %d, want 2", idx.Settings.NumberOfShards)
	}
	if idx.Settings.NumberOfReplicas != 1 {
		t.Errorf("replicas = %d, want 1", idx.Settings.NumberOfReplicas)
	}
	if idx.Settings.RefreshInterval != 5*time.Second {
		t.Errorf("refresh_interval = %v, want 5s", idx.Settings.RefreshInterval)
	}
	if idx.State != IndexStateOpen {
		t.Errorf("state = %v, want IndexStateOpen", idx.State)
	}
	if idx.Mapping == nil || len(idx.Mapping.Properties) != 2 {
		t.Fatalf("mapping properties count = %d, want 2", len(idx.Mapping.Properties))
	}
	if idx.Mapping.Properties["title"].Type != mapping.FieldTypeText {
		t.Errorf("title type = %q, want %q", idx.Mapping.Properties["title"].Type, mapping.FieldTypeText)
	}
	if idx.Mapping.Properties["title"].Analyzer != "standard" {
		t.Errorf("title analyzer = %q, want %q", idx.Mapping.Properties["title"].Analyzer, "standard")
	}
}

func TestIndexStateJSON(t *testing.T) {
	tests := []struct {
		state IndexState
		want  string
	}{
		{IndexStateOpen, `"open"`},
		{IndexStateClosed, `"closed"`},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.state)
		if err != nil {
			t.Fatalf("marshal %v: %v", tt.state, err)
		}
		if string(data) != tt.want {
			t.Errorf("marshal %v = %s, want %s", tt.state, data, tt.want)
		}

		var got IndexState
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		if got != tt.state {
			t.Errorf("unmarshal %s = %v, want %v", data, got, tt.state)
		}
	}
}

func TestRefreshIntervalJSON(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     string
	}{
		{1 * time.Second, `"1s"`},
		{5 * time.Second, `"5s"`},
		{-1, `"-1"`},
		{0, `"0s"`},
		{500 * time.Millisecond, `"500ms"`},
	}
	for _, tt := range tests {
		settings := IndexSettings{RefreshInterval: tt.interval}
		data, err := json.Marshal(settings)
		if err != nil {
			t.Fatalf("marshal %v: %v", tt.interval, err)
		}

		var got IndexSettings
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		if got.RefreshInterval != tt.interval {
			t.Errorf("roundtrip %v: got %v", tt.interval, got.RefreshInterval)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/cluster/ -run 'TestMetadataJSON|TestIndexStateJSON|TestRefreshIntervalJSON' -v`
Expected: FAIL — `IndexState` has no `MarshalJSON`, `IndexSettings` needs custom marshal for `RefreshInterval`.

- [ ] **Step 3: Add JSON tags to mapping types**

Edit `server/mapping/mapping.go` — add JSON struct tags:

```go
type MappingDefinition struct {
	Properties map[string]FieldMapping `json:"properties"`
}

type FieldMapping struct {
	Type     FieldType `json:"type"`
	Analyzer string    `json:"analyzer,omitempty"`
}
```

- [ ] **Step 4: Add JSON tags and custom marshal/unmarshal to metadata types**

Edit `server/cluster/metadata.go`:

1. Add JSON tags to `Metadata` and `IndexMetadata`:

```go
type Metadata struct {
	Indices map[string]*IndexMetadata `json:"indices"`
}

type IndexMetadata struct {
	Name     string                   `json:"name"`
	Settings IndexSettings            `json:"settings"`
	Mapping  *mapping.MappingDefinition `json:"mapping,omitempty"`
	State    IndexState               `json:"state"`
}
```

2. Add `MarshalJSON`/`UnmarshalJSON` for `IndexState`:

```go
func (s IndexState) MarshalJSON() ([]byte, error) {
	switch s {
	case IndexStateOpen:
		return json.Marshal("open")
	case IndexStateClosed:
		return json.Marshal("closed")
	default:
		return nil, fmt.Errorf("unknown index state: %d", s)
	}
}

func (s *IndexState) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "open":
		*s = IndexStateOpen
	case "closed":
		*s = IndexStateClosed
	default:
		return fmt.Errorf("unknown index state: %q", str)
	}
	return nil
}
```

3. Add custom `MarshalJSON`/`UnmarshalJSON` for `IndexSettings` to handle `RefreshInterval` as a duration string:

```go
type indexSettingsJSON struct {
	NumberOfShards   int    `json:"number_of_shards"`
	NumberOfReplicas int    `json:"number_of_replicas"`
	RefreshInterval  string `json:"refresh_interval"`
}

func (s IndexSettings) MarshalJSON() ([]byte, error) {
	var ri string
	switch {
	case s.RefreshInterval == -1:
		ri = "-1"
	case s.RefreshInterval%time.Second == 0:
		ri = fmt.Sprintf("%ds", int(s.RefreshInterval.Seconds()))
	case s.RefreshInterval%time.Millisecond == 0:
		ri = fmt.Sprintf("%dms", s.RefreshInterval.Milliseconds())
	default:
		ri = s.RefreshInterval.String()
	}
	return json.Marshal(indexSettingsJSON{
		NumberOfShards:   s.NumberOfShards,
		NumberOfReplicas: s.NumberOfReplicas,
		RefreshInterval:  ri,
	})
}

func (s *IndexSettings) UnmarshalJSON(data []byte) error {
	var raw indexSettingsJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.NumberOfShards = raw.NumberOfShards
	s.NumberOfReplicas = raw.NumberOfReplicas

	if raw.RefreshInterval == "-1" {
		s.RefreshInterval = -1
		return nil
	}
	d, err := time.ParseDuration(raw.RefreshInterval)
	if err != nil {
		return fmt.Errorf("parse refresh_interval %q: %w", raw.RefreshInterval, err)
	}
	s.RefreshInterval = d
	return nil
}
```

Add required imports to `metadata.go`: `"encoding/json"`, `"fmt"`, `"time"` (time already imported).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./server/cluster/ -run 'TestMetadataJSON|TestIndexStateJSON|TestRefreshIntervalJSON' -v`
Expected: PASS

- [ ] **Step 6: Run full test suite to check for regressions**

Run: `go test ./server/...`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add server/cluster/metadata.go server/cluster/metadata_test.go server/mapping/mapping.go
git commit -m "feat: add JSON serialization for cluster metadata types"
```

---

### Task 2: FilePersistedState

Implement `FilePersistedState` — a `PersistedState` that persists metadata to a JSON file with atomic writes and version tracking.

**Files:**
- Create: `server/cluster/file_persisted_state.go`
- Create: `server/cluster/file_persisted_state_test.go`

- [ ] **Step 1: Write the failing test for FilePersistedState roundtrip**

Create `server/cluster/file_persisted_state_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/cluster/ -run 'TestFilePersistedState' -v`
Expected: FAIL — `NewFilePersistedState` not defined.

- [ ] **Step 3: Implement FilePersistedState**

Create `server/cluster/file_persisted_state.go`:

```go
package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	stateFileName = "cluster_state.json"
	stateTmpName  = "cluster_state.tmp"
)

// persistedStateFile is the on-disk JSON format for cluster state.
type persistedStateFile struct {
	Version  int64     `json:"version"`
	Metadata *Metadata `json:"metadata"`
}

// FilePersistedState implements PersistedState by writing metadata
// as JSON to {stateDir}/cluster_state.json with atomic tmp+rename.
type FilePersistedState struct {
	stateDir string
	metadata *Metadata
	version  int64
}

// NewFilePersistedState creates a FilePersistedState backed by the given directory.
// If a state file exists, it is loaded. If not, an empty metadata is used.
// Stale tmp files from interrupted writes are cleaned up.
func NewFilePersistedState(stateDir string) (*FilePersistedState, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	// Clean up stale tmp file from a crashed write
	tmpPath := filepath.Join(stateDir, stateTmpName)
	os.Remove(tmpPath) // ignore error — file may not exist

	statePath := filepath.Join(stateDir, stateFileName)
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		return &FilePersistedState{
			stateDir: stateDir,
			metadata: NewMetadata(),
			version:  0,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}

	var sf persistedStateFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("corrupt cluster state file %s: %w", statePath, err)
	}
	if sf.Metadata == nil {
		sf.Metadata = NewMetadata()
	}

	return &FilePersistedState{
		stateDir: stateDir,
		metadata: sf.Metadata,
		version:  sf.Version,
	}, nil
}

func (f *FilePersistedState) GetMetadata() *Metadata {
	return f.metadata
}

func (f *FilePersistedState) SetMetadata(metadata *Metadata) {
	f.version++
	f.metadata = metadata
	if err := f.writeToDisk(); err != nil {
		panic(fmt.Sprintf("failed to persist cluster state: %v", err))
	}
}

// Version returns the current version counter.
func (f *FilePersistedState) Version() int64 {
	return f.version
}

// writeToDisk atomically writes the current state to disk using tmp+rename.
func (f *FilePersistedState) writeToDisk() error {
	sf := persistedStateFile{
		Version:  f.version,
		Metadata: f.metadata,
	}
	data, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	tmpPath := filepath.Join(f.stateDir, stateTmpName)
	statePath := filepath.Join(f.stateDir, stateFileName)

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, statePath); err != nil {
		return fmt.Errorf("rename tmp to state: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./server/cluster/ -run 'TestFilePersistedState' -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `go test ./server/...`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add server/cluster/file_persisted_state.go server/cluster/file_persisted_state_test.go
git commit -m "feat: add FilePersistedState for disk-backed cluster state"
```

---

### Task 3: GatewayMetaState

Implement `GatewayMetaState` — loads persisted state on startup and recovers index services by reopening shards.

**Files:**
- Create: `server/gateway/gateway_meta_state.go`
- Create: `server/gateway/gateway_meta_state_test.go`

- [ ] **Step 1: Write the failing test for GatewayMetaState fresh start**

Create `server/gateway/gateway_meta_state_test.go`:

```go
package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
)

func TestGatewayMetaStateFreshStart(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	gw := NewGatewayMetaState()
	cs, services, err := gw.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if len(cs.Metadata().Indices) != 0 {
		t.Errorf("indices = %d, want 0", len(cs.Metadata().Indices))
	}
	if len(services) != 0 {
		t.Errorf("services = %d, want 0", len(services))
	}
}

func TestGatewayMetaStateRecoverIndex(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	// Phase 1: create an index via normal flow
	gw1 := NewGatewayMetaState()
	cs1, services1, err := gw1.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start 1: %v", err)
	}

	meta := &cluster.IndexMetadata{
		Name: "test_index",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
			RefreshInterval:  1 * time.Second,
		},
		Mapping: &mapping.MappingDefinition{
			Properties: map[string]mapping.FieldMapping{
				"title": {Type: mapping.FieldTypeText, Analyzer: "standard"},
			},
		},
		State: cluster.IndexStateOpen,
	}

	// Create index data directory and shard (simulating create index action)
	indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", "test_index")
	svc, err := createIndexService(meta, indexDataPath, registry)
	if err != nil {
		t.Fatalf("create index service: %v", err)
	}
	services1["test_index"] = svc

	// Update cluster state (this persists to disk via FilePersistedState)
	cs1.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
		md.Indices["test_index"] = meta
		return md
	})

	// Close all services (simulating node shutdown)
	for _, s := range services1 {
		s.Close()
	}

	// Phase 2: start fresh and verify recovery
	gw2 := NewGatewayMetaState()
	cs2, services2, err := gw2.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start 2: %v", err)
	}
	defer func() {
		for _, s := range services2 {
			s.Close()
		}
	}()

	if len(cs2.Metadata().Indices) != 1 {
		t.Fatalf("recovered indices = %d, want 1", len(cs2.Metadata().Indices))
	}
	idx := cs2.Metadata().Indices["test_index"]
	if idx == nil {
		t.Fatal("index 'test_index' not recovered")
	}
	if idx.Name != "test_index" {
		t.Errorf("name = %q, want %q", idx.Name, "test_index")
	}
	if _, ok := services2["test_index"]; !ok {
		t.Error("index service for 'test_index' not recovered")
	}
}

func TestGatewayMetaStateMissingIndexDataDir(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	// Write a state file referencing an index whose data dir doesn't exist
	stateDir := filepath.Join(dataPath, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	state := struct {
		Version  int64             `json:"version"`
		Metadata *cluster.Metadata `json:"metadata"`
	}{
		Version: 1,
		Metadata: &cluster.Metadata{
			Indices: map[string]*cluster.IndexMetadata{
				"ghost_index": {
					Name: "ghost_index",
					Settings: cluster.IndexSettings{
						NumberOfShards:   1,
						NumberOfReplicas: 0,
						RefreshInterval:  1 * time.Second,
					},
					Mapping: &mapping.MappingDefinition{
						Properties: map[string]mapping.FieldMapping{},
					},
					State: cluster.IndexStateOpen,
				},
			},
		},
	}
	data, _ := json.MarshalIndent(state, "", "  ")
	os.WriteFile(filepath.Join(stateDir, "cluster_state.json"), data, 0o644)

	gw := NewGatewayMetaState()
	cs, services, err := gw.Start(dataPath, registry)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		for _, s := range services {
			s.Close()
		}
	}()

	// Ghost index should be removed from metadata
	if len(cs.Metadata().Indices) != 0 {
		t.Errorf("indices after cleanup = %d, want 0", len(cs.Metadata().Indices))
	}
	if len(services) != 0 {
		t.Errorf("services after cleanup = %d, want 0", len(services))
	}
}

func TestGatewayMetaStateCorruptStateFile(t *testing.T) {
	dataPath := t.TempDir()
	registry := analysis.DefaultRegistry()

	stateDir := filepath.Join(dataPath, "_state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(stateDir, "cluster_state.json"), []byte("{corrupt"), 0o644)

	gw := NewGatewayMetaState()
	_, _, err := gw.Start(dataPath, registry)
	if err == nil {
		t.Fatal("expected error for corrupt state file, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/gateway/ -run 'TestGatewayMetaState' -v`
Expected: FAIL — package `server/gateway` does not exist.

- [ ] **Step 3: Implement GatewayMetaState**

Create `server/gateway/gateway_meta_state.go`:

```go
package gateway

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
)

// GatewayMetaState handles loading persisted cluster state and recovering
// index services on node startup. This mirrors Elasticsearch's GatewayMetaState.
type GatewayMetaState struct{}

func NewGatewayMetaState() *GatewayMetaState {
	return &GatewayMetaState{}
}

// Start loads persisted cluster state from disk, recovers all index services,
// and returns the reconstructed ClusterState and index service map.
// If no state file exists, a fresh empty state is returned.
func (g *GatewayMetaState) Start(dataPath string, registry *analysis.AnalyzerRegistry) (*cluster.ClusterState, map[string]*index.IndexService, error) {
	stateDir := filepath.Join(dataPath, "_state")
	ps, err := cluster.NewFilePersistedState(stateDir)
	if err != nil {
		return nil, nil, fmt.Errorf("load persisted state: %w", err)
	}

	cs := cluster.NewClusterStateWith(ps)
	services := make(map[string]*index.IndexService)

	md := ps.GetMetadata()
	var removed []string

	for name, meta := range md.Indices {
		indexDataPath := filepath.Join(dataPath, "nodes", "0", "indices", name)

		svc, err := recoverIndexService(meta, indexDataPath, registry)
		if err != nil {
			log.Printf("WARNING: skipping index %q during recovery: %v", name, err)
			removed = append(removed, name)
			continue
		}
		services[name] = svc
	}

	// Clean up indices that failed to recover
	if len(removed) > 0 {
		cs.UpdateMetadata(func(md *cluster.Metadata) *cluster.Metadata {
			for _, name := range removed {
				delete(md.Indices, name)
			}
			return md
		})
	}

	return cs, services, nil
}

// recoverIndexService reopens an IndexService from existing shard data on disk.
func recoverIndexService(meta *cluster.IndexMetadata, indexDataPath string, registry *analysis.AnalyzerRegistry) (*index.IndexService, error) {
	// Verify the index data directory exists
	if _, err := os.Stat(indexDataPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("index data directory missing: %s", indexDataPath)
	}

	m := meta.Mapping
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}

	return index.NewIndexService(meta, m, indexDataPath, registry)
}

// createIndexService is a convenience for creating a new IndexService
// (used by tests and actions that need to create indices).
func createIndexService(meta *cluster.IndexMetadata, indexDataPath string, registry *analysis.AnalyzerRegistry) (*index.IndexService, error) {
	m := meta.Mapping
	if m == nil {
		m = &mapping.MappingDefinition{
			Properties: make(map[string]mapping.FieldMapping),
		}
	}
	return index.NewIndexService(meta, m, indexDataPath, registry)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./server/gateway/ -run 'TestGatewayMetaState' -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `go test ./server/...`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add server/gateway/gateway_meta_state.go server/gateway/gateway_meta_state_test.go
git commit -m "feat: add GatewayMetaState for cluster state recovery on startup"
```

---

### Task 4: Wire into Node Startup

Replace `NewClusterState()` with `GatewayMetaState.Start()` in `node.go` and add a node-level integration test.

**Files:**
- Modify: `server/node/node.go`
- Modify: `server/node/node_test.go`

- [ ] **Step 1: Write the failing integration test**

Add to `server/node/node_test.go`:

```go
func TestNodeRecoveryAfterRestart(t *testing.T) {
	dataPath := t.TempDir()

	// Start node, create an index, index a document
	n1, err := node.NewNode(node.NodeConfig{DataPath: dataPath, HTTPPort: 0})
	if err != nil {
		t.Fatalf("new node 1: %v", err)
	}
	addr1, err := n1.Start()
	if err != nil {
		t.Fatalf("start node 1: %v", err)
	}

	baseURL := "http://" + addr1

	// Create index
	createBody := `{"settings":{"number_of_shards":1},"mappings":{"properties":{"title":{"type":"text"}}}}`
	resp, err := http.Put(baseURL+"/test_index", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create index status = %d", resp.StatusCode)
	}

	// Index a document
	docBody := `{"title":"hello world"}`
	resp, err = http.Post(baseURL+"/test_index/_doc/1", "application/json", strings.NewReader(docBody))
	if err != nil {
		t.Fatalf("index doc: %v", err)
	}
	resp.Body.Close()

	// Refresh
	resp, err = http.Post(baseURL+"/test_index/_refresh", "application/json", nil)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	resp.Body.Close()

	// Stop node 1
	n1.Stop()

	// Start node 2 with same data path
	n2, err := node.NewNode(node.NodeConfig{DataPath: dataPath, HTTPPort: 0})
	if err != nil {
		t.Fatalf("new node 2: %v", err)
	}
	addr2, err := n2.Start()
	if err != nil {
		t.Fatalf("start node 2: %v", err)
	}
	defer n2.Stop()

	baseURL2 := "http://" + addr2

	// Verify index exists via _cat/indices
	resp, err = http.Get(baseURL2 + "/_cat/indices")
	if err != nil {
		t.Fatalf("cat indices: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "test_index") {
		t.Errorf("_cat/indices does not contain test_index: %s", body)
	}

	// Verify document is searchable
	searchBody := `{"query":{"match":{"title":"hello"}}}`
	resp, err = http.Post(baseURL2+"/test_index/_search", "application/json", strings.NewReader(searchBody))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	searchResult, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(searchResult), "hello world") {
		t.Errorf("search result does not contain document: %s", searchResult)
	}
}
```

Note: check existing test imports in `node_test.go` and add `"io"`, `"net/http"`, `"strings"` if not already present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./server/node/ -run 'TestNodeRecoveryAfterRestart' -v`
Expected: FAIL — node starts with empty state, index not recovered.

- [ ] **Step 3: Modify node.go to use GatewayMetaState**

Edit `server/node/node.go`:

1. Add import `"gosearch/server/gateway"`

2. Replace in `NewNode()`:

```go
// Before:
cs := cluster.NewClusterState()
indexServices := make(map[string]*index.IndexService)

// After:
gw := gateway.NewGatewayMetaState()
cs, indexServices, err := gw.Start(config.DataPath, registry)
if err != nil {
    return nil, fmt.Errorf("recover cluster state: %w", err)
}
```

3. Remove the now-unused `"gosearch/server/cluster"` import if no other references exist.

- [ ] **Step 4: Run the integration test to verify it passes**

Run: `go test ./server/node/ -run 'TestNodeRecoveryAfterRestart' -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `go test ./server/...`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add server/node/node.go server/node/node_test.go
git commit -m "feat: wire GatewayMetaState into node startup for automatic recovery"
```
