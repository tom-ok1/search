package action

import (
	"encoding/json"
	"fmt"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type BulkItem struct {
	Action string // "index", "create", or "delete"
	Index  string
	ID     string
	Source json.RawMessage // nil for delete
}

type BulkRequest struct {
	Items []BulkItem
}

type BulkItemResponse struct {
	Action  string       `json:"action"`
	Index   string       `json:"_index"`
	ID      string       `json:"_id"`
	Version int64        `json:"_version"`
	Status  int          `json:"status"`
	Error   *ErrorDetail `json:"error,omitempty"`
}

type ErrorDetail struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type BulkResponse struct {
	Took   int64
	Errors bool
	Items  []BulkItemResponse
}

type TransportBulkAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

func NewTransportBulkAction(
	cs *cluster.ClusterState,
	services map[string]*index.IndexService,
) *TransportBulkAction {
	return &TransportBulkAction{
		clusterState:  cs,
		indexServices: services,
	}
}

func (a *TransportBulkAction) Name() string {
	return "indices:data/write/bulk"
}

// shardKey identifies a target shard for grouping bulk items.
type shardKey struct {
	index   string
	shardID int
}

// bulkItemWithIndex pairs a bulk item with its original position in the request.
type bulkItemWithIndex struct {
	item    BulkItem
	origIdx int
}

func (a *TransportBulkAction) Execute(req BulkRequest) (BulkResponse, error) {
	start := time.Now()

	responses := make([]BulkItemResponse, len(req.Items))
	hasErrors := false

	// Phase 1: Resolve shards and group items.
	// Items that fail validation (e.g. missing index) are recorded immediately.
	groups := make(map[shardKey][]bulkItemWithIndex)
	var order []shardKey

	for i, item := range req.Items {
		svc := a.indexServices[item.Index]
		if a.clusterState.Metadata().Indices[item.Index] == nil || svc == nil {
			hasErrors = true
			responses[i] = BulkItemResponse{
				Action: item.Action,
				Index:  item.Index,
				ID:     item.ID,
				Status: 404,
				Error: &ErrorDetail{
					Type:   "index_not_found_exception",
					Reason: fmt.Sprintf("no such index [%s]", item.Index),
				},
			}
			continue
		}

		sid := index.RouteShard(item.ID, svc.NumShards())
		key := shardKey{index: item.Index, shardID: sid}

		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], bulkItemWithIndex{
			item:    item,
			origIdx: i,
		})
	}

	// Phase 2: Execute items grouped by shard.
	for _, key := range order {
		items := groups[key]
		svc := a.indexServices[key.index]
		shard := svc.Shard(key.shardID)

		for _, bi := range items {
			resp := BulkItemResponse{
				Action: bi.item.Action,
				Index:  bi.item.Index,
				ID:     bi.item.ID,
			}

			version, err := a.executeItemOnShard(shard, bi.item)
			if err != nil {
				hasErrors = true
				resp.Status = errorStatusCode(err)
				resp.Error = &ErrorDetail{
					Type:   errorType(err),
					Reason: err.Error(),
				}
			} else {
				resp.Version = version
				switch bi.item.Action {
				case "index", "create":
					resp.Status = 201
				case "delete":
					resp.Status = 200
				}
			}

			responses[bi.origIdx] = resp
		}
	}

	return BulkResponse{
		Took:   time.Since(start).Milliseconds(),
		Errors: hasErrors,
		Items:  responses,
	}, nil
}

// executeItemOnShard runs a single bulk item against the given shard.
func (a *TransportBulkAction) executeItemOnShard(shard *index.IndexShard, item BulkItem) (int64, error) {
	switch item.Action {
	case "create":
		// For create, check if doc already exists via the shard's Get (real-time).
		result := shard.Get(item.ID)
		if result.Found {
			return 0, &VersionConflictError{ID: item.ID, Index: item.Index}
		}
		r, err := shard.Index(item.ID, item.Source)
		if err != nil {
			return 0, err
		}
		return r.Version, nil
	case "index":
		r, err := shard.Index(item.ID, item.Source)
		if err != nil {
			return 0, err
		}
		return r.Version, nil
	case "delete":
		r, err := shard.Delete(item.ID)
		if err != nil {
			return 0, err
		}
		return r.Version, nil
	default:
		return 0, fmt.Errorf("unknown bulk action [%s]", item.Action)
	}
}

// VersionConflictError is returned when a create operation targets an existing document.
type VersionConflictError struct {
	ID    string
	Index string
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf("[%s]: version conflict, document already exists (current version [1])", e.ID)
}

func errorType(err error) string {
	if _, ok := err.(*VersionConflictError); ok {
		return "version_conflict_engine_exception"
	}
	return "action_request_validation_exception"
}

func errorStatusCode(err error) int {
	if _, ok := err.(*VersionConflictError); ok {
		return 409
	}
	return 400
}
