package action

import (
	"container/heap"
	"encoding/json"
	"time"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/aggregation"
	"gosearch/server/cluster"
	"gosearch/server/index"
)

type SearchRequest struct {
	Index     string
	QueryJSON map[string]any
	AggsJSON  map[string]any
	Size      int
}

type SearchResponse struct {
	Took         int64
	Hits         SearchHits
	Aggregations map[string]any
}

type SearchHits struct {
	Total    TotalHits
	MaxScore float64
	Hits     []SearchHit
}

type TotalHits struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchHit struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Score  float64         `json:"_score"`
	Source json.RawMessage `json:"_source"`
}

type TransportSearchAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	registry      *analysis.AnalyzerRegistry
}

func NewTransportSearchAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
	registry *analysis.AnalyzerRegistry,
) *TransportSearchAction {
	return &TransportSearchAction{
		clusterState:  cs,
		indexServices: services,
		registry:      registry,
	}
}

func (a *TransportSearchAction) Name() string {
	return "indices:data/read/search"
}

func (a *TransportSearchAction) Execute(req SearchRequest) (SearchResponse, error) {
	start := time.Now()

	meta := a.clusterState.Metadata().Indices[req.Index]
	if meta == nil {
		return SearchResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return SearchResponse{}, &IndexNotFoundError{Index: req.Index}
	}

	// Parse query
	parser := NewQueryParser(svc.Mapping(), a.registry)
	query, err := parser.ParseQuery(req.QueryJSON)
	if err != nil {
		return SearchResponse{}, &QueryParsingError{Reason: err.Error()}
	}

	// Parse aggregations
	var aggs []aggregation.Aggregator
	if req.AggsJSON != nil {
		aggs, err = aggregation.ParseAggregations(req.AggsJSON, svc.Mapping())
		if err != nil {
			return SearchResponse{}, &QueryParsingError{Reason: err.Error()}
		}
	}

	size := req.Size
	if size <= 0 {
		size = 10
	}

	// Query phase: collect top `size` from each shard, then merge
	numShards := svc.NumShards()

	// Collect top-K from each shard
	shardResults := make([][]search.SearchResult, 0, numShards)
	totalHits := 0
	for i := range numShards {
		shard := svc.Shard(i)
		searcher := shard.Searcher()
		if searcher == nil {
			continue
		}

		collector := search.NewTopKCollector(size)
		results := searcher.Search(query, collector)
		totalHits += collector.TotalHits()
		if len(results) > 0 {
			shardResults = append(shardResults, results)
		}
	}

	// Aggregation phase: run aggregators on matched docs
	if len(aggs) > 0 {
		for i := range numShards {
			shard := svc.Shard(i)
			searcher := shard.Searcher()
			if searcher == nil {
				continue
			}
			reader := searcher.Reader()
			for _, leaf := range reader.Leaves() {
				leafAggs := make([]aggregation.LeafAggregator, len(aggs))
				for j, agg := range aggs {
					leafAggs[j] = agg.GetLeafAggregator(leaf)
				}
				weight := query.CreateWeight(searcher, search.ScoreModeNone)
				scorer := weight.Scorer(leaf)
				if scorer == nil {
					continue
				}
				liveDocs := leaf.Segment.LiveDocs()
				iter := scorer.Iterator()
				doc := iter.NextDoc()
				for doc != search.NoMoreDocs {
					if liveDocs == nil || !liveDocs.Get(doc) {
						for _, la := range leafAggs {
							la.Collect(doc)
						}
					}
					doc = iter.NextDoc()
				}
			}
		}
	}

	// Build aggregation results
	var aggResults map[string]any
	if len(aggs) > 0 {
		aggResults = make(map[string]any, len(aggs))
		for _, agg := range aggs {
			result := agg.BuildResult()
			if result.Buckets != nil {
				buckets := make([]map[string]any, len(result.Buckets))
				for i, b := range result.Buckets {
					buckets[i] = map[string]any{"key": b.Key, "doc_count": b.DocCount}
				}
				aggResults[result.Name] = map[string]any{"buckets": buckets}
			} else {
				aggResults[result.Name] = map[string]any{"value": result.Value}
			}
		}
	}

	// Merge phase: k-way merge of sorted shard results using a max-heap
	allResults := mergeTopDocs(shardResults, size)

	// Fetch phase: build SearchHits
	maxScore := 0.0
	hits := make([]SearchHit, 0, len(allResults))
	for _, r := range allResults {
		if r.Score > maxScore {
			maxScore = r.Score
		}

		id := string(r.Fields["_id"])
		source := r.Fields["_source"]

		hits = append(hits, SearchHit{
			Index:  req.Index,
			ID:     id,
			Score:  r.Score,
			Source: json.RawMessage(source),
		})
	}

	took := time.Since(start).Milliseconds()

	return SearchResponse{
		Took: took,
		Hits: SearchHits{
			Total:    TotalHits{Value: totalHits, Relation: "eq"},
			MaxScore: maxScore,
			Hits:     hits,
		},
		Aggregations: aggResults,
	}, nil
}

// mergeTopDocs performs a k-way merge of pre-sorted (descending by score) shard
// results, returning the top n results. This mirrors Lucene's TopDocs.merge
// which uses a priority queue over shard cursors.
func mergeTopDocs(shardResults [][]search.SearchResult, n int) []search.SearchResult {
	if len(shardResults) == 0 {
		return nil
	}

	// Initialize the heap with one cursor per shard, pointing at index 0
	h := make(shardRefHeap, 0, len(shardResults))
	for i, results := range shardResults {
		h = append(h, shardRef{shardIndex: i, hitIndex: 0, score: results[0].Score})
	}
	heap.Init(&h)

	merged := make([]search.SearchResult, 0, n)
	for len(merged) < n && h.Len() > 0 {
		top := &h[0]
		merged = append(merged, shardResults[top.shardIndex][top.hitIndex])

		top.hitIndex++
		if top.hitIndex < len(shardResults[top.shardIndex]) {
			top.score = shardResults[top.shardIndex][top.hitIndex].Score
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	return merged
}

// shardRef tracks the current position within a single shard's results.
type shardRef struct {
	shardIndex int
	hitIndex   int
	score      float64
}

// shardRefHeap is a max-heap ordered by score (highest first).
type shardRefHeap []shardRef

func (h shardRefHeap) Len() int { return len(h) }
func (h shardRefHeap) Less(i, j int) bool {
	if h[i].score != h[j].score {
		return h[i].score > h[j].score
	}
	return h[i].shardIndex < h[j].shardIndex
}
func (h shardRefHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *shardRefHeap) Push(x any) {
	*h = append(*h, x.(shardRef))
}

func (h *shardRefHeap) Pop() any {
	old := *h
	n := len(old)
	ref := old[n-1]
	*h = old[:n-1]
	return ref
}
