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

func newTestFieldAnalyzers() *analysis.FieldAnalyzers {
	return analysis.NewFieldAnalyzers(
		analysis.NewAnalyzer(analysis.NewWhitespaceTokenizer(), &analysis.LowerCaseFilter{}),
	)
}

func newTestRegistry() *analysis.AnalyzerRegistry {
	return analysis.DefaultRegistry()
}

func TestEngine_IndexAndRefreshAndSearch(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Index a document
	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello world", document.FieldTypeText)

	if _, err := eng.Index("1", doc, nil, nil, nil); err != nil {
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

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello world", document.FieldTypeText)
	if _, err := eng.Index("1", doc, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}
	if eng.Searcher().Reader().LiveDocCount() != 1 {
		t.Fatal("expected 1 doc before delete")
	}

	// Delete and refresh
	if _, err := eng.Delete("1", nil, nil); err != nil {
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

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestRegistry(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	source := []byte(`{"title": "hello world"}`)
	if _, err := shard.Index("doc1", source, nil, nil); err != nil {
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

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestRegistry(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	source := []byte(`{"title": "hello world"}`)
	if _, err := shard.Index("doc1", source, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := shard.Refresh(); err != nil {
		t.Fatal(err)
	}

	if _, err := shard.Delete("doc1", nil, nil); err != nil {
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

	svc, err := index.NewIndexService(meta, m, dataPath, newTestRegistry())
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
	if _, err := shard.Index("1", []byte(`{"title": "hello"}`), nil, nil); err != nil {
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

func TestEngine_IndexAndSearchJapanese(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc0 := document.NewDocument()
	doc0.AddField("_id", "1", document.FieldTypeKeyword)
	doc0.AddField("title", "東京タワー スカイツリー", document.FieldTypeText)
	eng.Index("1", doc0, nil, nil, nil)

	doc1 := document.NewDocument()
	doc1.AddField("_id", "2", document.FieldTypeKeyword)
	doc1.AddField("title", "大阪城 通天閣", document.FieldTypeText)
	eng.Index("2", doc1, nil, nil, nil)

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	searcher := eng.Searcher()
	results := searcher.Search(search.NewTermQuery("title", "東京タワー"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for '東京タワー', got %d", len(results))
	}

	results = searcher.Search(search.NewTermQuery("title", "大阪城"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for '大阪城', got %d", len(results))
	}
}

func TestEngine_DeleteJapanese(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "東京", document.FieldTypeKeyword)
	doc.AddField("title", "東京タワー", document.FieldTypeText)
	eng.Index("東京", doc, nil, nil, nil)
	eng.Refresh()

	if _, err := eng.Delete("東京", nil, nil); err != nil {
		t.Fatal(err)
	}
	eng.Refresh()

	if eng.Searcher().Reader().LiveDocCount() != 0 {
		t.Fatalf("expected 0 live docs after delete, got %d", eng.Searcher().Reader().LiveDocCount())
	}
}

func TestIndexShard_IndexAndSearchJapanese(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title":    {Type: mapping.FieldTypeText},
			"category": {Type: mapping.FieldTypeKeyword},
		},
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestRegistry(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	shard.Index("1", []byte(`{"title": "東京 大阪", "category": "都市"}`), nil, nil)
	shard.Index("2", []byte(`{"title": "名古屋 京都", "category": "都市"}`), nil, nil)
	shard.Refresh()

	searcher := shard.Searcher()
	results := searcher.Search(search.NewTermQuery("title", "東京"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for '東京', got %d", len(results))
	}

	results = searcher.Search(search.NewTermQuery("category", "都市"), search.NewTopKCollector(10))
	if len(results) != 2 {
		t.Errorf("expected 2 results for category '都市', got %d", len(results))
	}
}

func TestEngine_PerDocumentSeqNo(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// First index of doc "a" → version 1
	docA := document.NewDocument()
	docA.AddField("_id", "a", document.FieldTypeKeyword)
	docA.AddField("title", "hello", document.FieldTypeText)
	r1, err := eng.Index("a", docA, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r1.SeqNo < 0 {
		t.Fatalf("expected SeqNo >= 0 for new doc a, got %d", r1.SeqNo)
	}

	// First index of doc "b" gets its own SeqNo
	docB := document.NewDocument()
	docB.AddField("_id", "b", document.FieldTypeKeyword)
	docB.AddField("title", "world", document.FieldTypeText)
	r2, err := eng.Index("b", docB, []byte(`{"title":"world"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r2.SeqNo <= r1.SeqNo {
		t.Fatalf("expected SeqNo > %d for new doc b, got %d", r1.SeqNo, r2.SeqNo)
	}

	// Update doc "a" gets next SeqNo
	docA2 := document.NewDocument()
	docA2.AddField("_id", "a", document.FieldTypeKeyword)
	docA2.AddField("title", "hello updated", document.FieldTypeText)
	r3, err := eng.Index("a", docA2, []byte(`{"title":"hello updated"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r3.SeqNo <= r2.SeqNo {
		t.Fatalf("expected SeqNo > %d for updated doc a, got %d", r2.SeqNo, r3.SeqNo)
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

	// ES-compatible shard assignments (Murmur3 + floorMod)
	esTests := []struct {
		id        string
		numShards int
		want      int
	}{
		{"doc1", 5, 2},
		{"doc2", 5, 4},
		{"test", 5, 1},
		{"hello", 5, 1},
		{"elasticsearch", 5, 0},
		{"0", 5, 1},
		{"1", 5, 3},
		{"12345", 5, 3},
	}
	for _, tt := range esTests {
		got := index.RouteShard(tt.id, tt.numShards)
		if got != tt.want {
			t.Errorf("RouteShard(%q, %d) = %d, want %d (ES-compatible)", tt.id, tt.numShards, got, tt.want)
		}
	}
}

func TestEngine_IndexAndSearchSpecialChars(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc0 := document.NewDocument()
	doc0.AddField("_id", "1", document.FieldTypeKeyword)
	doc0.AddField("title", "café résumé", document.FieldTypeText)
	eng.Index("1", doc0, nil, nil, nil)

	doc1 := document.NewDocument()
	doc1.AddField("_id", "2", document.FieldTypeKeyword)
	doc1.AddField("title", "hello 🔍 world", document.FieldTypeText)
	eng.Index("2", doc1, nil, nil, nil)

	doc2 := document.NewDocument()
	doc2.AddField("_id", "3", document.FieldTypeKeyword)
	doc2.AddField("title", "𠮷野家 テスト", document.FieldTypeText)
	eng.Index("3", doc2, nil, nil, nil)

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	searcher := eng.Searcher()

	tests := []struct {
		term     string
		expected int
	}{
		{"café", 1},
		{"résumé", 1},
		{"🔍", 1},
		{"𠮷野家", 1},
		{"テスト", 1},
	}
	for _, tt := range tests {
		results := searcher.Search(search.NewTermQuery("title", tt.term), search.NewTopKCollector(10))
		if len(results) != tt.expected {
			t.Errorf("term %q: expected %d results, got %d", tt.term, tt.expected, len(results))
		}
	}
}

func TestEngine_DeleteSpecialCharID(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "user@example.com", document.FieldTypeKeyword)
	doc.AddField("title", "test doc", document.FieldTypeText)
	eng.Index("user@example.com", doc, nil, nil, nil)
	eng.Refresh()

	if eng.Searcher().Reader().LiveDocCount() != 1 {
		t.Fatal("expected 1 doc")
	}

	eng.Delete("user@example.com", nil, nil)
	eng.Refresh()

	if eng.Searcher().Reader().LiveDocCount() != 0 {
		t.Fatalf("expected 0 docs after delete, got %d", eng.Searcher().Reader().LiveDocCount())
	}
}

func TestIndexShard_SpecialCharsRoundtrip(t *testing.T) {
	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title":    {Type: mapping.FieldTypeText},
			"category": {Type: mapping.FieldTypeKeyword},
		},
	}

	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	shard, err := index.NewIndexShard(0, "test-index", dir, m, newTestRegistry(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer shard.Close()

	shard.Index("1", []byte(`{"title": "café résumé", "category": "New York"}`), nil, nil)
	shard.Index("2", []byte(`{"title": "🔍 search 🔎", "category": "C++"}`), nil, nil)
	shard.Index("3", []byte(`{"title": "𠮷野家 テスト", "category": "user@example.com"}`), nil, nil)
	shard.Refresh()

	searcher := shard.Searcher()

	// Text field search
	results := searcher.Search(search.NewTermQuery("title", "café"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'café', got %d", len(results))
	}

	// Keyword field exact match with spaces
	results = searcher.Search(search.NewTermQuery("category", "New York"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for keyword 'New York', got %d", len(results))
	}

	// Keyword partial should not match
	results = searcher.Search(search.NewTermQuery("category", "New"), search.NewTopKCollector(10))
	if len(results) != 0 {
		t.Errorf("expected 0 results for partial keyword 'New', got %d", len(results))
	}

	// Keyword with special chars
	results = searcher.Search(search.NewTermQuery("category", "C++"), search.NewTopKCollector(10))
	if len(results) != 1 {
		t.Errorf("expected 1 result for keyword 'C++', got %d", len(results))
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

	svc, err := index.NewIndexService(meta, m, dataPath, newTestRegistry())
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
		if _, err := shard.Index(d.id, []byte(d.source), nil, nil); err != nil {
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
	if _, err := shard.Delete("2", nil, nil); err != nil {
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
	if _, err := shard.Index("1", []byte(`{"name": "Advanced Go", "category": "books"}`), nil, nil); err != nil {
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

func TestEngine_ResultsIncludeSeqNoAndPrimaryTerm(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Index
	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello", document.FieldTypeText)
	ir, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ir.SeqNo < 0 {
		t.Fatalf("expected SeqNo >= 0, got %d", ir.SeqNo)
	}
	if ir.PrimaryTerm != 1 {
		t.Fatalf("expected PrimaryTerm 1, got %d", ir.PrimaryTerm)
	}

	// Delete
	dr, err := eng.Delete("1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dr.SeqNo <= ir.SeqNo {
		t.Fatalf("expected delete SeqNo > index SeqNo, got %d <= %d", dr.SeqNo, ir.SeqNo)
	}
	if dr.PrimaryTerm != 1 {
		t.Fatalf("expected PrimaryTerm 1, got %d", dr.PrimaryTerm)
	}

	// Get (real-time)
	doc2 := document.NewDocument()
	doc2.AddField("_id", "2", document.FieldTypeKeyword)
	doc2.AddField("title", "world", document.FieldTypeText)
	eng.Index("2", doc2, []byte(`{"title":"world"}`), nil, nil)

	gr := eng.Get("2")
	if !gr.Found {
		t.Fatal("expected doc 2 to be found")
	}
	if gr.SeqNo < 0 {
		t.Fatalf("expected SeqNo >= 0 in GetResult, got %d", gr.SeqNo)
	}
	if gr.PrimaryTerm != 1 {
		t.Fatalf("expected PrimaryTerm 1 in GetResult, got %d", gr.PrimaryTerm)
	}
}

func TestEngine_SeqNoAcrossDeleteAndReindex(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Index doc "a"
	docA := document.NewDocument()
	docA.AddField("_id", "a", document.FieldTypeKeyword)
	docA.AddField("title", "hello", document.FieldTypeText)
	r1, err := eng.Index("a", docA, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r1.SeqNo < 0 {
		t.Fatalf("expected SeqNo >= 0, got %d", r1.SeqNo)
	}

	// Delete doc "a"
	dr, err := eng.Delete("a", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dr.SeqNo <= r1.SeqNo {
		t.Fatalf("expected delete SeqNo > %d, got %d", r1.SeqNo, dr.SeqNo)
	}
	if !dr.Found {
		t.Fatal("expected Found=true")
	}

	// Re-index doc "a" after delete
	docA2 := document.NewDocument()
	docA2.AddField("_id", "a", document.FieldTypeKeyword)
	docA2.AddField("title", "hello again", document.FieldTypeText)
	r2, err := eng.Index("a", docA2, []byte(`{"title":"hello again"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r2.SeqNo <= dr.SeqNo {
		t.Fatalf("expected SeqNo > %d after re-index, got %d", dr.SeqNo, r2.SeqNo)
	}
	if !r2.Created {
		t.Fatal("expected Created=true after re-index of deleted doc")
	}
}

func TestEngine_IfSeqNoCASConflict(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello", document.FieldTypeText)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Update with correct if_seq_no → should succeed
	doc2 := document.NewDocument()
	doc2.AddField("_id", "1", document.FieldTypeKeyword)
	doc2.AddField("title", "updated", document.FieldTypeText)
	seqNo := r1.SeqNo
	term := r1.PrimaryTerm
	r2, err := eng.Index("1", doc2, []byte(`{"title":"updated"}`), &seqNo, &term)
	if err != nil {
		t.Fatalf("expected CAS success, got: %v", err)
	}

	// Update with stale if_seq_no → should fail
	doc3 := document.NewDocument()
	doc3.AddField("_id", "1", document.FieldTypeKeyword)
	doc3.AddField("title", "conflict", document.FieldTypeText)
	_, err = eng.Index("1", doc3, []byte(`{"title":"conflict"}`), &seqNo, &term)
	if err == nil {
		t.Fatal("expected CAS conflict error")
	}

	// Update with correct new seqNo → should succeed
	newSeqNo := r2.SeqNo
	newTerm := r2.PrimaryTerm
	_, err = eng.Index("1", doc3, []byte(`{"title":"conflict"}`), &newSeqNo, &newTerm)
	if err != nil {
		t.Fatalf("expected CAS success with updated seqNo, got: %v", err)
	}
}

func TestEngine_IfSeqNoCASOnNonexistentDoc(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello", document.FieldTypeText)
	seqNo := int64(0)
	term := int64(1)
	_, err = eng.Index("1", doc, []byte(`{"title":"hello"}`), &seqNo, &term)
	if err == nil {
		t.Fatal("expected CAS conflict error for nonexistent doc")
	}
}

func TestEngine_DeleteWithCAS(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	doc := document.NewDocument()
	doc.AddField("_id", "1", document.FieldTypeKeyword)
	doc.AddField("title", "hello", document.FieldTypeText)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	wrongSeqNo := int64(999)
	wrongTerm := int64(1)
	_, err = eng.Delete("1", &wrongSeqNo, &wrongTerm)
	if err == nil {
		t.Fatal("expected CAS conflict error")
	}

	seqNo := r1.SeqNo
	term := r1.PrimaryTerm
	dr, err := eng.Delete("1", &seqNo, &term)
	if err != nil {
		t.Fatalf("expected CAS delete success, got: %v", err)
	}
	if !dr.Found {
		t.Fatal("expected Found=true")
	}
}

func TestEngine_CASAfterRefresh(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	doc, _ := mapping.ParseDocument("1", []byte(`{"title":"hello"}`), m)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	doc2, _ := mapping.ParseDocument("1", []byte(`{"title":"updated"}`), m)
	seqNo := r1.SeqNo
	term := r1.PrimaryTerm
	_, err = eng.Index("1", doc2, []byte(`{"title":"updated"}`), &seqNo, &term)
	if err != nil {
		t.Fatalf("expected CAS after refresh to succeed, got: %v", err)
	}
}

func TestEngine_CASAfterRefreshWithStaleSeqNo(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	doc, _ := mapping.ParseDocument("1", []byte(`{"title":"hello"}`), m)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	doc2, _ := mapping.ParseDocument("1", []byte(`{"title":"updated"}`), m)
	_, err = eng.Index("1", doc2, []byte(`{"title":"updated"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	doc3, _ := mapping.ParseDocument("1", []byte(`{"title":"conflict"}`), m)
	staleSeqNo := r1.SeqNo
	term := r1.PrimaryTerm
	_, err = eng.Index("1", doc3, []byte(`{"title":"conflict"}`), &staleSeqNo, &term)
	if err == nil {
		t.Fatal("expected CAS conflict error for stale seqNo after refresh")
	}
	if _, ok := err.(*index.VersionConflictEngineError); !ok {
		t.Fatalf("expected VersionConflictEngineError, got %T: %v", err, err)
	}
}

func TestEngine_DeleteCASAfterRefresh(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	doc, _ := mapping.ParseDocument("1", []byte(`{"title":"hello"}`), m)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	seqNo := r1.SeqNo
	term := r1.PrimaryTerm
	dr, err := eng.Delete("1", &seqNo, &term)
	if err != nil {
		t.Fatalf("expected CAS delete after refresh to succeed, got: %v", err)
	}
	if !dr.Found {
		t.Fatal("expected Found=true")
	}
}

func TestEngine_TranslogRecoveryPreservesSeqNoDocValues(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	translogPath := filepath.Join(t.TempDir(), "translog")

	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	registry := newTestRegistry()

	// Create shard, index a document, then close without flush
	shard, err := index.NewIndexShard(0, "test", dir, m, registry, translogPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = shard.Index("1", []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := shard.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen shard — translog recovery should replay the index operation
	shard2, err := index.NewIndexShard(0, "test", dir, m, registry, translogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer shard2.Close()

	// Refresh to make docs visible in Lucene
	if err := shard2.Refresh(); err != nil {
		t.Fatal(err)
	}

	// GET should return the document with a valid seqNo
	gr := shard2.Get("1")
	if !gr.Found {
		t.Fatal("expected document to be found after translog recovery")
	}
	if gr.SeqNo < 0 {
		t.Fatalf("expected SeqNo >= 0 after recovery, got %d", gr.SeqNo)
	}
	if gr.PrimaryTerm <= 0 {
		t.Fatalf("expected PrimaryTerm > 0 after recovery, got %d", gr.PrimaryTerm)
	}

	// CAS should work after recovery + refresh
	seqNo := gr.SeqNo
	term := gr.PrimaryTerm
	_, err = shard2.Index("1", []byte(`{"title":"updated"}`), &seqNo, &term)
	if err != nil {
		t.Fatalf("expected CAS after translog recovery to succeed, got: %v", err)
	}
}

func TestEngine_GetAfterRefreshIncludesSeqNo(t *testing.T) {
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := index.NewEngine(dir, newTestFieldAnalyzers(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	m := &mapping.MappingDefinition{
		Properties: map[string]mapping.FieldMapping{
			"title": {Type: mapping.FieldTypeText},
		},
	}

	doc, _ := mapping.ParseDocument("1", []byte(`{"title":"hello"}`), m)
	r1, err := eng.Index("1", doc, []byte(`{"title":"hello"}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Refresh(); err != nil {
		t.Fatal(err)
	}

	gr := eng.Get("1")
	if !gr.Found {
		t.Fatal("expected document to be found after refresh")
	}
	if gr.SeqNo != r1.SeqNo {
		t.Fatalf("expected SeqNo=%d after refresh GET, got %d", r1.SeqNo, gr.SeqNo)
	}
	if gr.PrimaryTerm != r1.PrimaryTerm {
		t.Fatalf("expected PrimaryTerm=%d after refresh GET, got %d", r1.PrimaryTerm, gr.PrimaryTerm)
	}
}
