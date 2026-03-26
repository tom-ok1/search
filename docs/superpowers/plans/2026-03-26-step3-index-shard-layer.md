# Step 3: Index & Shard Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Engine, IndexShard, and IndexService that wrap GoSearch's existing Lucene layer, providing the index/shard abstraction that transport actions will use in Step 4+.

**Architecture:** Three-layer hierarchy mirroring Elasticsearch: `IndexService` manages per-index state and shards → `IndexShard` owns a single shard's Engine and delegates operations → `Engine` wraps `IndexWriter`/`IndexReader`/`IndexSearcher` and handles refresh (NRT reader swap). A `routeShard` function provides consistent hashing for shard routing.

**Tech Stack:** Go 1.23.6, existing `gosearch` packages (`index/`, `search/`, `store/`, `analysis/`, `document/`), `server/mapping/`, `server/cluster/`

---

### Task 1: Engine — Core Wrapper

**Files:**
- Create: `server/index/engine.go`
- Create: `server/index/engine_test.go`

The Engine wraps an `IndexWriter` and manages the `IndexReader`/`IndexSearcher` lifecycle. It exposes `Index`, `Delete`, `Refresh`, `Searcher`, and `Close`.

- [ ] **Step 1: Write the failing test for Engine creation and basic indexing**

```go
// server/index/engine_test.go
package index_test

import (
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/server/index"
	"gosearch/store"
)

func newTestAnalyzer() *analysis.Analyzer {
	return analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		analysis.NewLowerCaseFilter(),
	)
}

func TestEngine_IndexAndRefreshAndSearch(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Index a document
	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello world", document.FieldTypeText)

	if err := eng.Index(doc); err != nil {
		t.Fatal(err)
	}

	// Before refresh, searcher should find nothing
	searcher := eng.Searcher()
	if searcher == nil {
		t.Fatal("expected non-nil searcher even before refresh")
	}

	// Refresh to make documents visible
	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	// After refresh, searcher should find the document
	searcher = eng.Searcher()
	if searcher == nil {
		t.Fatal("expected non-nil searcher after refresh")
	}
	if searcher.Reader().LiveDocCount() != 1 {
		t.Fatalf("expected 1 live doc, got %d", searcher.Reader().LiveDocCount())
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestEngine_IndexAndRefreshAndSearch -v`
Expected: compilation error — package `server/index` does not exist.

- [ ] **Step 3: Implement Engine**

```go
// server/index/engine.go
package index

import (
	"sync"

	"gosearch/analysis"
	goindex "gosearch/index"
	"gosearch/search"
	"gosearch/store"
	"gosearch/document"
)

const defaultBufferSize = 1000

// Engine wraps an IndexWriter and manages the IndexReader/IndexSearcher lifecycle.
// It mirrors Elasticsearch's InternalEngine.
type Engine struct {
	writer   *goindex.IndexWriter
	reader   *goindex.IndexReader
	searcher *search.IndexSearcher
	dir      store.Directory
	mu       sync.RWMutex // protects reader/searcher swap on refresh
}

// NewEngine creates a new Engine backed by the given directory and analyzer.
func NewEngine(dir store.Directory, analyzer *analysis.Analyzer) (*Engine, error) {
	writer := goindex.NewIndexWriter(dir, analyzer, defaultBufferSize)
	return &Engine{
		writer: writer,
		dir:    dir,
	}, nil
}

// Index adds a document to the engine's writer.
func (e *Engine) Index(doc *document.Document) error {
	return e.writer.AddDocument(doc)
}

// Delete removes all documents matching the given field/value term.
func (e *Engine) Delete(field, value string) error {
	return e.writer.DeleteDocuments(field, value)
}

// Refresh opens a new NRT reader from the writer, making recently indexed
// documents visible to search. This mirrors Elasticsearch's refresh semantics.
func (e *Engine) Refresh() error {
	reader, err := goindex.OpenNRTReader(e.writer)
	if err != nil {
		return err
	}

	newSearcher := search.NewIndexSearcher(reader)

	e.mu.Lock()
	oldReader := e.reader
	e.reader = reader
	e.searcher = newSearcher
	e.mu.Unlock()

	if oldReader != nil {
		oldReader.Close()
	}
	return nil
}

// Searcher returns the current IndexSearcher. Returns nil if Refresh has
// never been called. The caller must not close the returned searcher.
func (e *Engine) Searcher() *search.IndexSearcher {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.searcher
}

// Close shuts down the engine, closing the reader and writer.
func (e *Engine) Close() error {
	e.mu.Lock()
	reader := e.reader
	e.reader = nil
	e.searcher = nil
	e.mu.Unlock()

	if reader != nil {
		reader.Close()
	}
	return e.writer.Close()
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestEngine_IndexAndRefreshAndSearch -v`
Expected: PASS

- [ ] **Step 5: Write test for Engine.Delete**

```go
// Add to server/index/engine_test.go
func TestEngine_Delete(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello world", document.FieldTypeText)
	if err := eng.Index(doc); err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}
	if eng.Searcher().Reader().LiveDocCount() != 1 {
		t.Fatal("expected 1 doc before delete")
	}

	// Delete and refresh
	if err := eng.Delete("_id", "1"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}
	if eng.Searcher().Reader().LiveDocCount() != 0 {
		t.Fatalf("expected 0 live docs after delete, got %d", eng.Searcher().Reader().LiveDocCount())
	}
}
```

- [ ] **Step 6: Run the delete test**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestEngine_Delete -v`
Expected: PASS (implementation already handles this)

---

### Task 2: IndexShard

**Files:**
- Create: `server/index/shard.go`
- Modify: `server/index/engine_test.go` (add shard tests)

IndexShard owns an Engine for a single shard and provides the document-level API (Index by ID + source, Delete by ID, Refresh, Searcher).

- [ ] **Step 1: Write the failing test for IndexShard**

```go
// Add to server/index/engine_test.go

import (
	"gosearch/server/mapping"
)

func TestIndexShard_IndexAndSearch(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	// Index a document
	source := []byte(`{"title": "hello world"}`)
	if err := shard.Index("doc1", source); err != nil {
		t.Fatal(err)
	}

	// Refresh
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	// Verify via searcher
	searcher := shard.Searcher()
	if searcher == nil {
		t.Fatal("expected non-nil searcher")
	}
	if searcher.Reader().LiveDocCount() != 1 {
		t.Fatalf("expected 1 live doc, got %d", searcher.Reader().LiveDocCount())
	}

	if shard.ShardID() != 0 {
		t.Fatalf("expected shard ID 0, got %d", shard.ShardID())
	}
	if shard.IndexName() != "test-index" {
		t.Fatalf("expected index name test-index, got %s", shard.IndexName())
	}
}

func TestIndexShard_Delete(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	source := []byte(`{"title": "hello world"}`)
	if err := shard.Index("doc1", source); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	if err := shard.Delete("doc1"); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	if shard.Searcher().Reader().LiveDocCount() != 0 {
		t.Fatalf("expected 0 docs after delete, got %d", shard.Searcher().Reader().LiveDocCount())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestIndexShard -v`
Expected: compilation error — `index.NewIndexShard` undefined.

- [ ] **Step 3: Implement IndexShard**

```go
// server/index/shard.go
package index

import (
	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/mapping"
	"gosearch/store"
)

// IndexShard represents a single shard of an index. It owns an Engine
// and provides document-level operations. This mirrors Elasticsearch's IndexShard.
type IndexShard struct {
	shardID   int
	indexName string
	engine    *Engine
	mapping   *mapping.MappingDefinition
}

// NewIndexShard creates a new IndexShard backed by the given directory.
func NewIndexShard(shardID int, indexName string, dir store.Directory, m *mapping.MappingDefinition, analyzer *analysis.Analyzer) (*IndexShard, error) {
	engine, err := NewEngine(dir, analyzer)
	if err != nil {
		return nil, err
	}

	return &IndexShard{
		shardID:   shardID,
		indexName: indexName,
		engine:    engine,
		mapping:   m,
	}, nil
}

// Index parses the JSON source according to the mapping and indexes the document.
func (s *IndexShard) Index(id string, source []byte) error {
	doc, err := mapping.ParseDocument(id, source, s.mapping)
	if err != nil {
		return err
	}

	// Delete existing document with same ID first (update = delete + re-add)
	if err := s.engine.Delete("_id", id); err != nil {
		return err
	}

	return s.engine.Index(doc)
}

// Delete removes a document by its _id.
func (s *IndexShard) Delete(id string) error {
	return s.engine.Delete("_id", id)
}

// Refresh makes recently indexed documents visible to search.
func (s *IndexShard) Refresh() error {
	return s.engine.Refresh()
}

// Searcher returns the current IndexSearcher for this shard.
func (s *IndexShard) Searcher() *search.IndexSearcher {
	return s.engine.Searcher()
}

// ShardID returns this shard's numeric ID.
func (s *IndexShard) ShardID() int {
	return s.shardID
}

// IndexName returns the name of the index this shard belongs to.
func (s *IndexShard) IndexName() string {
	return s.indexName
}

// Close shuts down the shard's engine.
func (s *IndexShard) Close() error {
	return s.engine.Close()
}
```

- [ ] **Step 4: Run the shard tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestIndexShard -v`
Expected: PASS

---

### Task 3: IndexService & Shard Routing

**Files:**
- Create: `server/index/service.go`
- Modify: `server/index/engine_test.go` (add service and routing tests)

IndexService manages all shards for a single index. It creates shard directories and provides shard lookup. The `RouteShard` function provides consistent hash-based routing.

- [ ] **Step 1: Write the failing test for IndexService**

```go
// Add to server/index/engine_test.go

import (
	"gosearch/server/cluster"
	"path/filepath"
)

func TestIndexService_CreateAndAccess(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "test-index",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
		NumShards: 1,
		State:     cluster.IndexStateOpen,
	}
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	svc, err := index.NewIndexService(meta, m, dataPath, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	// Should have 1 shard
	shard := svc.Shard(0)
	if shard == nil {
		t.Fatal("expected shard 0 to exist")
	}
	if svc.Shard(1) != nil {
		t.Fatal("expected shard 1 to not exist")
	}

	// Index a doc through the shard
	if err := shard.Index("1", []byte(`{"title": "hello"}`)); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}
	if shard.Searcher().Reader().LiveDocCount() != 1 {
		t.Fatal("expected 1 doc")
	}

	// Verify shard directory was created
	shardDir := filepath.Join(dataPath, "0", "index")
	entries, err := filepath.Glob(filepath.Join(shardDir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("expected segment files in shard directory")
	}
}

func TestRouteShard(t *testing.T) {
	// Deterministic: same ID always routes to same shard
	shard1 := index.RouteShard("doc1", 5)
	shard2 := index.RouteShard("doc1", 5)
	if shard1 != shard2 {
		t.Fatal("expected deterministic routing")
	}

	// Within range
	for i := 0; i < 100; i++ {
		id := "doc" + string(rune('A'+i))
		s := index.RouteShard(id, 5)
		if s < 0 || s >= 5 {
			t.Fatalf("shard %d out of range [0, 5)", s)
		}
	}

	// Single shard always returns 0
	if index.RouteShard("anything", 1) != 0 {
		t.Fatal("single shard must return 0")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run "TestIndexService|TestRouteShard" -v`
Expected: compilation error — `index.NewIndexService` and `index.RouteShard` undefined.

- [ ] **Step 3: Implement IndexService and RouteShard**

```go
// server/index/service.go
package index

import (
	"fmt"
	"hash/fnv"
	"path/filepath"

	"gosearch/analysis"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
	"gosearch/store"
)

// IndexService manages all shards for a single index.
// This mirrors Elasticsearch's IndexService.
type IndexService struct {
	metadata *cluster.IndexMetadata
	mapping  *mapping.MappingDefinition
	shards   map[int]*IndexShard
}

// NewIndexService creates a new IndexService, initializing all shards.
// dataPath is the base directory for this index (e.g., data/nodes/0/indices/{index_name}).
func NewIndexService(meta *cluster.IndexMetadata, m *mapping.MappingDefinition, dataPath string, analyzer *analysis.Analyzer) (*IndexService, error) {
	shards := make(map[int]*IndexShard, meta.NumShards)

	for i := 0; i < meta.NumShards; i++ {
		shardPath := filepath.Join(dataPath, fmt.Sprintf("%d", i), "index")
		dir, err := store.NewFSDirectory(shardPath)
		if err != nil {
			// Close already-created shards on error
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d directory: %w", i, err)
		}

		shard, err := NewIndexShard(i, meta.Name, dir, m, analyzer)
		if err != nil {
			for _, s := range shards {
				s.Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards[i] = shard
	}

	return &IndexService{
		metadata: meta,
		mapping:  m,
		shards:   shards,
	}, nil
}

// Shard returns the IndexShard with the given ID, or nil if not found.
func (is *IndexService) Shard(id int) *IndexShard {
	return is.shards[id]
}

// Mapping returns the mapping definition for this index.
func (is *IndexService) Mapping() *mapping.MappingDefinition {
	return is.mapping
}

// NumShards returns the number of shards in this index.
func (is *IndexService) NumShards() int {
	return len(is.shards)
}

// Close shuts down all shards in this index.
func (is *IndexService) Close() error {
	var firstErr error
	for _, shard := range is.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RouteShard returns the shard ID for a given document ID using consistent hashing.
func RouteShard(id string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(numShards))
}
```

- [ ] **Step 4: Run all tests**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -v`
Expected: All tests PASS

---

### Task 4: Integration Test — Full Index Lifecycle

**Files:**
- Modify: `server/index/engine_test.go` (add integration test)

An integration test that exercises the full lifecycle: create IndexService → index multiple docs → refresh → search → delete → verify.

- [ ] **Step 1: Write the integration test**

```go
// Add to server/index/engine_test.go

import (
	"gosearch/search"
)

func TestIntegration_IndexLifecycle(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "products",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
		NumShards: 1,
		State:     cluster.IndexStateOpen,
	}
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"name":     {Type: mapping.FieldTypeText},
			"category": {Type: mapping.FieldTypeKeyword},
		},
	}

	svc, err := index.NewIndexService(meta, m, dataPath, newTestAnalyzer())
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	shard := svc.Shard(0)

	// Index multiple documents
	docs := []struct {
		id     string
		source string
	}{
		{"1", `{"name": "Go Programming", "category": "books"}`},
		{"2", `{"name": "Rust Programming", "category": "books"}`},
		{"3", `{"name": "Blue T-Shirt", "category": "clothing"}`},
	}
	for _, d := range docs {
		if err := shard.Index(d.id, []byte(d.source)); err != nil {
			t.Fatalf("index doc %s: %v", d.id, err)
		}
	}

	// Refresh
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	// Verify doc count
	searcher := shard.Searcher()
	if searcher.Reader().LiveDocCount() != 3 {
		t.Fatalf("expected 3 docs, got %d", searcher.Reader().LiveDocCount())
	}

	// Search for "programming" in name field
	q := search.NewTermQuery("name", "programming")
	collector := search.NewTopKCollector(10)
	results := searcher.Search(q, collector)
	if len(results) != 2 {
		t.Fatalf("expected 2 results for 'programming', got %d", len(results))
	}

	// Search for exact category
	q2 := search.NewTermQuery("category", "books")
	collector2 := search.NewTopKCollector(10)
	results2 := searcher.Search(q2, collector2)
	if len(results2) != 2 {
		t.Fatalf("expected 2 results for category 'books', got %d", len(results2))
	}

	// Delete one doc and verify
	if err := shard.Delete("2"); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	searcher = shard.Searcher()
	if searcher.Reader().LiveDocCount() != 2 {
		t.Fatalf("expected 2 docs after delete, got %d", searcher.Reader().LiveDocCount())
	}

	// Re-index (update) existing doc
	if err := shard.Index("1", []byte(`{"name": "Advanced Go", "category": "books"}`)); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	searcher = shard.Searcher()
	if searcher.Reader().LiveDocCount() != 2 {
		t.Fatalf("expected 2 docs after update, got %d", searcher.Reader().LiveDocCount())
	}

	// Verify updated doc is searchable by new term
	q3 := search.NewTermQuery("name", "advanced")
	collector3 := search.NewTopKCollector(10)
	results3 := searcher.Search(q3, collector3)
	if len(results3) != 1 {
		t.Fatalf("expected 1 result for 'advanced', got %d", len(results3))
	}
}
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./server/index/ -run TestIntegration_IndexLifecycle -v`
Expected: PASS

- [ ] **Step 3: Run all tests to confirm nothing is broken**

Run: `cd /Users/tomoyaoki/Documents/playground/gosearch && go test ./... -count=1`
Expected: All tests PASS across entire project.
