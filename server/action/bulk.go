package action

import (
	"encoding/json"
	"fmt"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

type BulkItem struct {
	Action string          // "index" or "delete"
	Index  string
	ID     string
	Source json.RawMessage // nil for delete
}

type BulkRequest struct {
	Items []BulkItem
}

type BulkItemResponse struct {
	Action string       `json:"action"`
	Index  string       `json:"_index"`
	ID     string       `json:"_id"`
	Status int          `json:"status"`
	Error  *ErrorDetail `json:"error,omitempty"`
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

func (a *TransportBulkAction) Execute(req BulkRequest) (BulkResponse, error) {
	start := time.Now()

	items := make([]BulkItemResponse, 0, len(req.Items))
	hasErrors := false

	for _, item := range req.Items {
		resp := BulkItemResponse{
			Action: item.Action,
			Index:  item.Index,
			ID:     item.ID,
		}

		err := a.executeItem(item)
		if err != nil {
			hasErrors = true
			resp.Status = 400
			resp.Error = &ErrorDetail{
				Type:   "action_request_validation_exception",
				Reason: err.Error(),
			}
		} else {
			switch item.Action {
			case "index":
				resp.Status = 201
			case "delete":
				resp.Status = 200
			}
		}

		items = append(items, resp)
	}

	return BulkResponse{
		Took:   time.Since(start).Milliseconds(),
		Errors: hasErrors,
		Items:  items,
	}, nil
}

func (a *TransportBulkAction) executeItem(item BulkItem) error {
	if a.clusterState.Metadata().Indices[item.Index] == nil {
		return fmt.Errorf("no such index [%s]", item.Index)
	}

	svc := a.indexServices[item.Index]
	if svc == nil {
		return fmt.Errorf("no such index [%s]", item.Index)
	}

	shardID := index.RouteShard(item.ID, svc.NumShards())
	shard := svc.Shard(shardID)

	switch item.Action {
	case "index":
		return shard.Index(item.ID, item.Source)
	case "delete":
		return shard.Delete(item.ID)
	default:
		return fmt.Errorf("unknown bulk action [%s]", item.Action)
	}
}
