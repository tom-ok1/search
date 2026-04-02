package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"gosearch/api"
	"gosearch/server/action"
	"gosearch/server/mapping"
)

// Handler implements api.StrictServerInterface by delegating to transport actions.
type Handler struct {
	createIndex    *action.TransportCreateIndexAction
	deleteIndex    *action.TransportDeleteIndexAction
	getIndex       *action.TransportGetIndexAction
	indexDocument  *action.TransportIndexAction
	getDocument    *action.TransportGetAction
	deleteDocument *action.TransportDeleteDocumentAction
	search         *action.TransportSearchAction
	bulk           *action.TransportBulkAction
	refresh        *action.TransportRefreshAction
	catIndices     *action.TransportCatIndicesAction
	catHealth      *action.TransportCatHealthAction
}

// Ensure Handler implements StrictServerInterface at compile time.
var _ api.StrictServerInterface = (*Handler)(nil)

// NewHandler creates a new Handler with the given transport actions.
func NewHandler(
	createIndex *action.TransportCreateIndexAction,
	deleteIndex *action.TransportDeleteIndexAction,
	getIndex *action.TransportGetIndexAction,
	indexDocument *action.TransportIndexAction,
	getDocument *action.TransportGetAction,
	deleteDocument *action.TransportDeleteDocumentAction,
	search *action.TransportSearchAction,
	bulk *action.TransportBulkAction,
	refresh *action.TransportRefreshAction,
	catIndices *action.TransportCatIndicesAction,
	catHealth *action.TransportCatHealthAction,
) *Handler {
	return &Handler{
		createIndex:    createIndex,
		deleteIndex:    deleteIndex,
		getIndex:       getIndex,
		indexDocument:  indexDocument,
		getDocument:    getDocument,
		deleteDocument: deleteDocument,
		search:         search,
		bulk:           bulk,
		refresh:        refresh,
		catIndices:     catIndices,
		catHealth:      catHealth,
	}
}

// errorResponse builds an api.ErrorResponse with the given status, type, and reason.
func errorResponse(status int, errType, reason string) api.ErrorResponse {
	return api.ErrorResponse{
		Error:  api.ErrorDetail{Type: errType, Reason: reason},
		Status: status,
	}
}

// parseRefreshInterval parses an ES-style refresh interval string.
// "-1" disables auto-refresh; otherwise delegates to time.ParseDuration.
func parseRefreshInterval(s string) (time.Duration, error) {
	if s == "-1" {
		return -1, nil
	}
	return time.ParseDuration(s)
}

// mapErrorStatus returns (status, errType) for a given transport action error.
func mapErrorStatus(err error) (int, string) {
	var indexNotFound *action.IndexNotFoundError
	var indexExists *action.IndexAlreadyExistsError
	var invalidName *action.InvalidIndexNameError
	var queryParsing *action.QueryParsingError
	var mapperParsing *action.MapperParsingError

	switch {
	case errors.As(err, &indexNotFound):
		return 404, "index_not_found_exception"
	case errors.As(err, &indexExists):
		return 400, "resource_already_exists_exception"
	case errors.As(err, &invalidName):
		return 400, "invalid_index_name_exception"
	case errors.As(err, &queryParsing):
		return 400, "query_parsing_exception"
	case errors.As(err, &mapperParsing):
		return 400, "mapper_parsing_exception"
	default:
		return 500, "internal_error"
	}
}

// CreateIndex creates a new index.
func (h *Handler) CreateIndex(_ context.Context, request api.CreateIndexRequestObject) (api.CreateIndexResponseObject, error) {
	actionReq := action.CreateIndexRequest{
		Name: request.Index,
	}

	if request.Body != nil {
		if request.Body.Settings != nil {
			if request.Body.Settings.NumberOfShards != nil {
				actionReq.Settings.NumberOfShards = *request.Body.Settings.NumberOfShards
			}
			if request.Body.Settings.NumberOfReplicas != nil {
				actionReq.Settings.NumberOfReplicas = *request.Body.Settings.NumberOfReplicas
			}
			if request.Body.Settings.RefreshInterval != nil {
				d, err := parseRefreshInterval(*request.Body.Settings.RefreshInterval)
				if err != nil {
					return api.CreateIndex400JSONResponse(errorResponse(400, "illegal_argument_exception", fmt.Sprintf("invalid refresh_interval: %v", err))), nil
				}
				actionReq.Settings.RefreshInterval = d
			}
		}
		if request.Body.Mappings != nil && request.Body.Mappings.Properties != nil {
			props := make(map[string]mapping.FieldMapping)
			for name, prop := range *request.Body.Mappings.Properties {
				fm := mapping.FieldMapping{
					Type: mapping.FieldType(prop.Type),
				}
				if prop.Analyzer != nil {
					fm.Analyzer = *prop.Analyzer
				}
				props[name] = fm
			}
			actionReq.Mappings = &mapping.MappingDefinition{Properties: props}
		}
	}

	resp, err := h.createIndex.Execute(actionReq)
	if err != nil {
		status, errType := mapErrorStatus(err)
		return api.CreateIndex400JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.CreateIndex200JSONResponse{
		Acknowledged: resp.Acknowledged,
		Index:        resp.Index,
	}, nil
}

// DeleteIndex deletes an index.
func (h *Handler) DeleteIndex(_ context.Context, request api.DeleteIndexRequestObject) (api.DeleteIndexResponseObject, error) {
	resp, err := h.deleteIndex.Execute(action.DeleteIndexRequest{Name: request.Index})
	if err != nil {
		status, errType := mapErrorStatus(err)
		if status == 404 {
			return api.DeleteIndex404JSONResponse(errorResponse(status, errType, err.Error())), nil
		}
		return api.DeleteIndex404JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.DeleteIndex200JSONResponse{
		Acknowledged: resp.Acknowledged,
	}, nil
}

// GetIndex returns index metadata.
func (h *Handler) GetIndex(_ context.Context, request api.GetIndexRequestObject) (api.GetIndexResponseObject, error) {
	resp, err := h.getIndex.Execute(action.GetIndexRequest{Name: request.Index})
	if err != nil {
		status, errType := mapErrorStatus(err)
		return api.GetIndex404JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	meta := api.IndexMetadata{}

	// Convert settings
	refreshStr := resp.Settings.RefreshInterval.String()
	if resp.Settings.RefreshInterval == -1 {
		refreshStr = "-1"
	}
	settings := &api.Settings{
		NumberOfShards:   &resp.Settings.NumberOfShards,
		NumberOfReplicas: &resp.Settings.NumberOfReplicas,
		RefreshInterval:  &refreshStr,
	}
	meta.Settings = settings

	// Convert mappings
	if resp.Mapping != nil && len(resp.Mapping.Properties) > 0 {
		props := make(api.Properties)
		for name, fm := range resp.Mapping.Properties {
			pd := api.PropertyDefinition{
				Type: api.FieldType(fm.Type),
			}
			if fm.Analyzer != "" {
				analyzer := fm.Analyzer
				pd.Analyzer = &analyzer
			}
			props[name] = pd
		}
		meta.Mappings = &api.Mappings{Properties: &props}
	}

	return api.GetIndex200JSONResponse(api.GetIndexResponse{
		request.Index: meta,
	}), nil
}

// IndexDocumentPut indexes a document via PUT.
func (h *Handler) IndexDocumentPut(_ context.Context, request api.IndexDocumentPutRequestObject) (api.IndexDocumentPutResponseObject, error) {
	source, err := json.Marshal(request.Body)
	if err != nil {
		return api.IndexDocumentPut400JSONResponse(errorResponse(400, "mapper_parsing_exception", err.Error())), nil
	}

	resp, err := h.indexDocument.Execute(action.IndexDocumentRequest{
		Index:  request.Index,
		ID:     request.Id,
		Source: source,
	})
	if err != nil {
		status, errType := mapErrorStatus(err)
		if status == 404 {
			return api.IndexDocumentPut404JSONResponse(errorResponse(status, errType, err.Error())), nil
		}
		return api.IndexDocumentPut400JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.IndexDocumentPut201JSONResponse{
		UnderscoreId:    resp.ID,
		UnderscoreIndex: resp.Index,
		Result:          resp.Result,
	}, nil
}

// IndexDocumentPost indexes a document via POST.
func (h *Handler) IndexDocumentPost(_ context.Context, request api.IndexDocumentPostRequestObject) (api.IndexDocumentPostResponseObject, error) {
	source, err := json.Marshal(request.Body)
	if err != nil {
		return api.IndexDocumentPost400JSONResponse(errorResponse(400, "mapper_parsing_exception", err.Error())), nil
	}

	resp, err := h.indexDocument.Execute(action.IndexDocumentRequest{
		Index:  request.Index,
		ID:     request.Id,
		Source: source,
	})
	if err != nil {
		status, errType := mapErrorStatus(err)
		if status == 404 {
			return api.IndexDocumentPost404JSONResponse(errorResponse(status, errType, err.Error())), nil
		}
		return api.IndexDocumentPost400JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.IndexDocumentPost201JSONResponse{
		UnderscoreId:    resp.ID,
		UnderscoreIndex: resp.Index,
		Result:          resp.Result,
	}, nil
}

// GetDocument retrieves a document by ID.
func (h *Handler) GetDocument(_ context.Context, request api.GetDocumentRequestObject) (api.GetDocumentResponseObject, error) {
	resp, err := h.getDocument.Execute(action.GetDocumentRequest{
		Index: request.Index,
		ID:    request.Id,
	})
	if err != nil {
		// GetDocument 404 returns GetDocumentResponse, not ErrorResponse
		var indexNotFound *action.IndexNotFoundError
		if errors.As(err, &indexNotFound) {
			return api.GetDocument404JSONResponse{
				UnderscoreId:    request.Id,
				UnderscoreIndex: request.Index,
				Found:           false,
			}, nil
		}
		return api.GetDocument404JSONResponse{
			UnderscoreId:    request.Id,
			UnderscoreIndex: request.Index,
			Found:           false,
		}, nil
	}

	if !resp.Found {
		return api.GetDocument404JSONResponse{
			UnderscoreId:    resp.ID,
			UnderscoreIndex: resp.Index,
			Found:           false,
		}, nil
	}

	var source *map[string]any
	if resp.Source != nil {
		var m map[string]any
		if err := json.Unmarshal(resp.Source, &m); err == nil {
			source = &m
		}
	}

	return api.GetDocument200JSONResponse{
		UnderscoreId:     resp.ID,
		UnderscoreIndex:  resp.Index,
		Found:            true,
		UnderscoreSource: source,
	}, nil
}

// DeleteDocument deletes a document by ID.
func (h *Handler) DeleteDocument(_ context.Context, request api.DeleteDocumentRequestObject) (api.DeleteDocumentResponseObject, error) {
	resp, err := h.deleteDocument.Execute(action.DeleteDocumentRequest{
		Index: request.Index,
		ID:    request.Id,
	})
	if err != nil {
		var indexNotFound *action.IndexNotFoundError
		if errors.As(err, &indexNotFound) {
			return api.DeleteDocument404JSONResponse{
				UnderscoreId:    request.Id,
				UnderscoreIndex: request.Index,
				Result:          "not_found",
			}, nil
		}
		return api.DeleteDocument404JSONResponse{
			UnderscoreId:    request.Id,
			UnderscoreIndex: request.Index,
			Result:          "not_found",
		}, nil
	}

	if resp.Result == "not_found" {
		return api.DeleteDocument404JSONResponse{
			UnderscoreId:    resp.ID,
			UnderscoreIndex: resp.Index,
			Result:          resp.Result,
		}, nil
	}

	return api.DeleteDocument200JSONResponse{
		UnderscoreId:    resp.ID,
		UnderscoreIndex: resp.Index,
		Result:          resp.Result,
	}, nil
}

// SearchGet handles GET search requests.
func (h *Handler) SearchGet(_ context.Context, request api.SearchGetRequestObject) (api.SearchGetResponseObject, error) {
	actionReq := buildSearchRequest(request.Index, request.Body, request.Params.Size)

	resp, err := h.search.Execute(actionReq)
	if err != nil {
		status, errType := mapErrorStatus(err)
		if status == 404 {
			return api.SearchGet404JSONResponse(errorResponse(status, errType, err.Error())), nil
		}
		return api.SearchGet400JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.SearchGet200JSONResponse(convertSearchResponse(resp)), nil
}

// SearchPost handles POST search requests.
func (h *Handler) SearchPost(_ context.Context, request api.SearchPostRequestObject) (api.SearchPostResponseObject, error) {
	actionReq := buildSearchRequest(request.Index, request.Body, request.Params.Size)

	resp, err := h.search.Execute(actionReq)
	if err != nil {
		status, errType := mapErrorStatus(err)
		if status == 404 {
			return api.SearchPost404JSONResponse(errorResponse(status, errType, err.Error())), nil
		}
		return api.SearchPost400JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.SearchPost200JSONResponse(convertSearchResponse(resp)), nil
}

// buildSearchRequest converts API search parameters to a transport action request.
func buildSearchRequest(index string, body *api.SearchRequest, paramSize *int) action.SearchRequest {
	req := action.SearchRequest{
		Index: index,
	}

	// Default to match_all if no query specified
	req.QueryJSON = map[string]any{"match_all": map[string]any{}}

	if body != nil {
		if body.Query != nil {
			req.QueryJSON = *body.Query
		}
		if body.Size != nil {
			req.Size = *body.Size
		}
		if body.Aggs != nil {
			req.AggsJSON = *body.Aggs
		} else if body.Aggregations != nil {
			req.AggsJSON = *body.Aggregations
		}
	}

	// Query param Size overrides body Size
	if paramSize != nil {
		req.Size = *paramSize
	}

	return req
}

// convertSearchResponse converts a transport action SearchResponse to an API SearchResponse.
func convertSearchResponse(resp action.SearchResponse) api.SearchResponse {
	hits := make([]api.SearchHit, 0, len(resp.Hits.Hits))
	for _, h := range resp.Hits.Hits {
		hit := api.SearchHit{
			UnderscoreId:    h.ID,
			UnderscoreIndex: h.Index,
			UnderscoreScore: float32(h.Score),
		}
		if h.Source != nil {
			var m map[string]any
			if err := json.Unmarshal(h.Source, &m); err == nil {
				hit.UnderscoreSource = &m
			}
		}
		hits = append(hits, hit)
	}

	maxScore := float32(resp.Hits.MaxScore)

	result := api.SearchResponse{
		Took: int(resp.Took),
		Hits: api.SearchHits{
			Total: api.SearchTotal{
				Value:    resp.Hits.Total.Value,
				Relation: resp.Hits.Total.Relation,
			},
			MaxScore: &maxScore,
			Hits:     hits,
		},
	}

	if resp.Aggregations != nil {
		result.Aggregations = &resp.Aggregations
	}

	return result
}

// Bulk handles bulk operations without an index scope.
func (h *Handler) Bulk(_ context.Context, request api.BulkRequestObject) (api.BulkResponseObject, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return api.Bulk400JSONResponse(errorResponse(400, "parse_exception", "failed to read bulk body")), nil
	}

	items, err := parseBulkNDJSON(body, "")
	if err != nil {
		return api.Bulk400JSONResponse(errorResponse(400, "parse_exception", err.Error())), nil
	}

	resp, err := h.bulk.Execute(action.BulkRequest{Items: items})
	if err != nil {
		return api.Bulk400JSONResponse(errorResponse(400, "internal_error", err.Error())), nil
	}

	return api.Bulk200JSONResponse(convertBulkResponse(resp)), nil
}

// BulkWithIndex handles bulk operations scoped to an index.
func (h *Handler) BulkWithIndex(_ context.Context, request api.BulkWithIndexRequestObject) (api.BulkWithIndexResponseObject, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return api.BulkWithIndex400JSONResponse(errorResponse(400, "parse_exception", "failed to read bulk body")), nil
	}

	items, err := parseBulkNDJSON(body, request.Index)
	if err != nil {
		return api.BulkWithIndex400JSONResponse(errorResponse(400, "parse_exception", err.Error())), nil
	}

	resp, err := h.bulk.Execute(action.BulkRequest{Items: items})
	if err != nil {
		return api.BulkWithIndex400JSONResponse(errorResponse(400, "internal_error", err.Error())), nil
	}

	return api.BulkWithIndex200JSONResponse(convertBulkResponse(resp)), nil
}

// convertBulkResponse converts a transport action BulkResponse to an API BulkResponse.
func convertBulkResponse(resp action.BulkResponse) api.BulkResponse {
	items := make([]api.BulkItemResponse, 0, len(resp.Items))
	for _, item := range resp.Items {
		result := api.BulkItemResult{
			UnderscoreId:    item.ID,
			UnderscoreIndex: item.Index,
			Status:          item.Status,
		}
		if item.Error != nil {
			result.Error = &api.ErrorDetail{
				Type:   item.Error.Type,
				Reason: item.Error.Reason,
			}
		}
		items = append(items, api.BulkItemResponse{
			item.Action: result,
		})
	}

	return api.BulkResponse{
		Took:   int(resp.Took),
		Errors: resp.Errors,
		Items:  items,
	}
}

// Refresh refreshes an index.
func (h *Handler) Refresh(_ context.Context, request api.RefreshRequestObject) (api.RefreshResponseObject, error) {
	resp, err := h.refresh.Execute(action.RefreshRequest{Index: request.Index})
	if err != nil {
		status, errType := mapErrorStatus(err)
		return api.Refresh404JSONResponse(errorResponse(status, errType, err.Error())), nil
	}

	return api.Refresh200JSONResponse{
		UnderscoreShards: api.ShardStats{
			Total:      resp.Shards,
			Successful: resp.Shards - resp.FailedShards,
			Failed:     resp.FailedShards,
		},
	}, nil
}

// parseBulkNDJSON parses NDJSON bulk request body into transport action BulkItems.
func parseBulkNDJSON(body []byte, defaultIndex string) ([]action.BulkItem, error) {
	var items []action.BulkItem
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var actionLine map[string]json.RawMessage
		if err := json.Unmarshal(line, &actionLine); err != nil {
			return nil, err
		}
		for actionName, meta := range actionLine {
			var metaObj struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			}
			if err := json.Unmarshal(meta, &metaObj); err != nil {
				return nil, err
			}
			idx := metaObj.Index
			if idx == "" {
				idx = defaultIndex
			}
			item := action.BulkItem{Action: actionName, Index: idx, ID: metaObj.ID}
			if actionName == "index" || actionName == "create" {
				if !scanner.Scan() {
					return nil, fmt.Errorf("missing source line for %s action", actionName)
				}
				sourceLine := bytes.TrimSpace(scanner.Bytes())
				item.Source = json.RawMessage(make([]byte, len(sourceLine)))
				copy(item.Source, sourceLine)
			}
			items = append(items, item)
		}
	}
	return items, scanner.Err()
}

// CatIndices returns index listing as plain text.
func (h *Handler) CatIndices(_ context.Context, _ api.CatIndicesRequestObject) (api.CatIndicesResponseObject, error) {
	resp, err := h.catIndices.Execute()
	if err != nil {
		return nil, err
	}
	return api.CatIndices200TextResponse(resp.FormatText()), nil
}

// CatHealth returns cluster health as plain text.
func (h *Handler) CatHealth(_ context.Context, _ api.CatHealthRequestObject) (api.CatHealthResponseObject, error) {
	resp, err := h.catHealth.Execute()
	if err != nil {
		return nil, err
	}
	return api.CatHealth200TextResponse(resp.FormatText()), nil
}
