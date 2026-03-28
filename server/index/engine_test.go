package index_test

import (
	"path/filepath"
	"testing"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/search"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/mapping"
	"gosearch/store"
)

func newTestAnalyzer() *analysis.Analyzer {
	return analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
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

	// Before refresh, searcher should be nil
	searcher := eng.Searcher()
	if searcher != nil {
		t.Fatal("expected nil searcher before first refresh")
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

	source := []byte(`{"title": "hello world"}`)
	if err := shard.Index("doc1", source); err != nil {
		t.Fatal(err)
	}

	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

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

func TestIndexService_CreateAndAccess(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "test-index",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
		State: cluster.IndexStateOpen,
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
	for i := range 100 {
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

func TestIntegration_IndexLifecycle(t *testing.T) {
	dataPath := t.TempDir()
	meta := &cluster.IndexMetadata{
		Name: "products",
		Settings: cluster.IndexSettings{
			NumberOfShards:   1,
			NumberOfReplicas: 0,
		},
		State: cluster.IndexStateOpen,
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
