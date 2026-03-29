package action

import (
	"encoding/json"
	"fmt"
	"time"

	"gosearch/analysis"
	"gosearch/search"
	"gosearch/server/cluster"
	"gosearch/server/index"
)

type SearchRequest struct {
	Index     string
	QueryJSON map[string]any
	Size      int
}

type SearchResponse struct {
	Took int64
	Hits SearchHits
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
		return SearchResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	svc := a.indexServices[req.Index]
	if svc == nil {
		return SearchResponse{}, fmt.Errorf("no such index [%s]", req.Index)
	}

	// Parse query
	parser := NewQueryParser(svc.Mapping(), a.registry)
	query, err := parser.ParseQuery(req.QueryJSON)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("parse query: %w", err)
	}

	size := req.Size
	if size <= 0 {
		size = 10
	}

	// Query phase: collect top `size` from each shard, then merge
	numShards := svc.NumShards()

	var allResults []search.SearchResult
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
		allResults = append(allResults, results...)
	}

	// Merge phase: sort by score descending, take top `size`
	sortByScoreDesc(allResults)
	if len(allResults) > size {
		allResults = allResults[:size]
	}

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
	}, nil
}

func sortByScoreDesc(results []search.SearchResult) {
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].Score > results[j-1].Score; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}
}
