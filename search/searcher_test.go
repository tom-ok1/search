package search

import (
	"testing"

	"gosearch/index"
)

// --- mock implementations ---

type mockSegment struct {
	name        string
	docCount    int
	deleted     map[int]bool
	stored      map[int]map[string]string
	postings    map[string]map[string][]index.Posting
	fieldLens   map[string]map[int]int
	totalFldLen map[string]int
}

func newMockSegment(name string, docCount int) *mockSegment {
	return &mockSegment{
		name:        name,
		docCount:    docCount,
		deleted:     make(map[int]bool),
		stored:      make(map[int]map[string]string),
		postings:    make(map[string]map[string][]index.Posting),
		fieldLens:   make(map[string]map[int]int),
		totalFldLen: make(map[string]int),
	}
}

func (m *mockSegment) Name() string             { return m.name }
func (m *mockSegment) DocCount() int            { return m.docCount }
func (m *mockSegment) IsDeleted(docID int) bool { return m.deleted[docID] }

func (m *mockSegment) LiveDocCount() int {
	count := m.docCount
	for range m.deleted {
		count--
	}
	return count
}

func (m *mockSegment) DocFreq(field, term string) int {
	if terms, ok := m.postings[field]; ok {
		if postings, ok := terms[term]; ok {
			return len(postings)
		}
	}
	return 0
}

func (m *mockSegment) FieldLength(field string, docID int) int {
	if lens, ok := m.fieldLens[field]; ok {
		return lens[docID]
	}
	return 0
}

func (m *mockSegment) TotalFieldLength(field string) int {
	return m.totalFldLen[field]
}

func (m *mockSegment) StoredFields(docID int) (map[string]string, error) {
	if fields, ok := m.stored[docID]; ok {
		return fields, nil
	}
	return nil, nil
}

func (m *mockSegment) PostingsIterator(field, term string) index.PostingsIterator {
	if terms, ok := m.postings[field]; ok {
		if postings, ok := terms[term]; ok {
			return index.NewSlicePostingsIterator(postings)
		}
	}
	return index.EmptyPostingsIterator{}
}

func (m *mockSegment) NumericDocValues(field string) index.NumericDocValues { return nil }
func (m *mockSegment) SortedDocValues(field string) index.SortedDocValues   { return nil }

// mockDocEntry represents a document with its score for mockQuery.
type mockDocEntry struct {
	DocID int
	Score float64
}

// mockQuery returns predetermined results.
type mockQuery struct {
	results map[string][]mockDocEntry // segment name -> results
}

func (q *mockQuery) CreateScorer(ctx index.LeafReaderContext, _ ScoreMode) Scorer {
	entries, ok := q.results[ctx.Segment.Name()]
	if !ok || len(entries) == 0 {
		return nil
	}
	return &mockScorer{entries: entries, idx: -1, docID: -1}
}

type mockScorer struct {
	entries []mockDocEntry
	idx     int
	docID   int
}

func (s *mockScorer) Iterator() DocIdSetIterator { return s }
func (s *mockScorer) DocID() int                 { return s.docID }
func (s *mockScorer) Score() float64 {
	if s.idx >= 0 && s.idx < len(s.entries) {
		return s.entries[s.idx].Score
	}
	return 0
}

func (s *mockScorer) NextDoc() int {
	s.idx++
	if s.idx >= len(s.entries) {
		s.docID = NoMoreDocs
		return NoMoreDocs
	}
	s.docID = s.entries[s.idx].DocID
	return s.docID
}

func (s *mockScorer) Advance(target int) int {
	for {
		doc := s.NextDoc()
		if doc >= target || doc == NoMoreDocs {
			return doc
		}
	}
}

func (s *mockScorer) Cost() int64 { return int64(len(s.entries)) }

// --- tests ---

func TestSearchSingleSegment(t *testing.T) {
	seg := newMockSegment("seg0", 3)
	seg.stored[0] = map[string]string{"title": "first"}
	seg.stored[1] = map[string]string{"title": "second"}
	seg.stored[2] = map[string]string{"title": "third"}

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 3.0},
				{DocID: 1, Score: 1.0},
				{DocID: 2, Score: 2.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Score != 3.0 || results[1].Score != 2.0 || results[2].Score != 1.0 {
		t.Errorf("expected scores [3.0, 2.0, 1.0], got [%f, %f, %f]",
			results[0].Score, results[1].Score, results[2].Score)
	}

	if results[0].Fields["title"] != "first" {
		t.Errorf("expected title 'first', got %q", results[0].Fields["title"])
	}
}

func TestSearchMultipleSegments(t *testing.T) {
	seg0 := newMockSegment("seg0", 2)
	seg0.stored[0] = map[string]string{"title": "doc0"}
	seg0.stored[1] = map[string]string{"title": "doc1"}

	seg1 := newMockSegment("seg1", 3)
	seg1.stored[0] = map[string]string{"title": "doc2"}
	seg1.stored[1] = map[string]string{"title": "doc3"}
	seg1.stored[2] = map[string]string{"title": "doc4"}

	reader := index.NewIndexReader([]index.SegmentReader{seg0, seg1})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {{DocID: 0, Score: 5.0}},
			"seg1": {{DocID: 1, Score: 3.0}, {DocID: 2, Score: 7.0}},
		},
	}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Score != 7.0 {
		t.Errorf("expected top score 7.0, got %f", results[0].Score)
	}

	expectedGlobalIDs := map[float64]int{7.0: 4, 5.0: 0, 3.0: 3}
	for _, r := range results {
		if expected, ok := expectedGlobalIDs[r.Score]; ok {
			if r.DocID != expected {
				t.Errorf("for score %f, expected globalDocID %d, got %d", r.Score, expected, r.DocID)
			}
		}
	}
}

func TestSearchSkipsDeletedDocs(t *testing.T) {
	seg := newMockSegment("seg0", 3)
	seg.stored[0] = map[string]string{"title": "alive"}
	seg.stored[1] = map[string]string{"title": "deleted"}
	seg.stored[2] = map[string]string{"title": "alive2"}
	seg.deleted[1] = true

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 1.0},
				{DocID: 1, Score: 5.0},
				{DocID: 2, Score: 2.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 2 {
		t.Fatalf("expected 2 results (deleted doc skipped), got %d", len(results))
	}

	for _, r := range results {
		if r.DocID == 1 {
			t.Error("deleted doc (docID=1) should not appear in results")
		}
	}
}

func TestSearchTopK(t *testing.T) {
	seg := newMockSegment("seg0", 5)
	for i := range 5 {
		seg.stored[i] = map[string]string{}
	}

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 1.0},
				{DocID: 1, Score: 5.0},
				{DocID: 2, Score: 3.0},
				{DocID: 3, Score: 4.0},
				{DocID: 4, Score: 2.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(3))
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expectedScores := []float64{5.0, 4.0, 3.0}
	for i, expected := range expectedScores {
		if results[i].Score != expected {
			t.Errorf("results[%d].Score = %f, want %f", i, results[i].Score, expected)
		}
	}
}

func TestSearchNoResults(t *testing.T) {
	seg := newMockSegment("seg0", 2)
	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{results: map[string][]mockDocEntry{}}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchEmptyIndex(t *testing.T) {
	reader := index.NewIndexReader([]index.SegmentReader{})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{results: map[string][]mockDocEntry{}}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty index, got %d", len(results))
	}
}

func TestSearchAllDocsDeleted(t *testing.T) {
	seg := newMockSegment("seg0", 2)
	seg.deleted[0] = true
	seg.deleted[1] = true

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 1.0},
				{DocID: 1, Score: 2.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 0 {
		t.Fatalf("expected 0 results when all docs deleted, got %d", len(results))
	}
}

func TestSearchStoredFieldsPopulated(t *testing.T) {
	seg := newMockSegment("seg0", 1)
	seg.stored[0] = map[string]string{
		"title": "Hello World",
		"url":   "https://example.com",
	}

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {{DocID: 0, Score: 1.0}},
		},
	}

	results := searcher.Search(q, NewTopKCollector(10))
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Fields["title"] != "Hello World" {
		t.Errorf("expected title 'Hello World', got %q", results[0].Fields["title"])
	}
	if results[0].Fields["url"] != "https://example.com" {
		t.Errorf("expected url 'https://example.com', got %q", results[0].Fields["url"])
	}
}

func TestSearchTopKAcrossMultipleSegments(t *testing.T) {
	seg0 := newMockSegment("seg0", 3)
	seg1 := newMockSegment("seg1", 3)
	for i := range 3 {
		seg0.stored[i] = map[string]string{}
		seg1.stored[i] = map[string]string{}
	}

	reader := index.NewIndexReader([]index.SegmentReader{seg0, seg1})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 1.0},
				{DocID: 1, Score: 6.0},
				{DocID: 2, Score: 3.0},
			},
			"seg1": {
				{DocID: 0, Score: 5.0},
				{DocID: 1, Score: 2.0},
				{DocID: 2, Score: 4.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(3))
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expectedScores := []float64{6.0, 5.0, 4.0}
	for i, expected := range expectedScores {
		if results[i].Score != expected {
			t.Errorf("results[%d].Score = %f, want %f", i, results[i].Score, expected)
		}
	}
}

func TestSearchDeletedDocsNotCountedInTopK(t *testing.T) {
	seg := newMockSegment("seg0", 4)
	for i := range 4 {
		seg.stored[i] = map[string]string{}
	}
	seg.deleted[1] = true
	seg.deleted[3] = true

	reader := index.NewIndexReader([]index.SegmentReader{seg})
	searcher := NewIndexSearcher(reader)

	q := &mockQuery{
		results: map[string][]mockDocEntry{
			"seg0": {
				{DocID: 0, Score: 1.0},
				{DocID: 1, Score: 10.0},
				{DocID: 2, Score: 2.0},
				{DocID: 3, Score: 9.0},
			},
		},
	}

	results := searcher.Search(q, NewTopKCollector(2))
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Score != 2.0 || results[1].Score != 1.0 {
		t.Errorf("expected scores [2.0, 1.0], got [%f, %f]",
			results[0].Score, results[1].Score)
	}
}
