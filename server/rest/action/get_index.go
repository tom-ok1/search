package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestGetIndexAction struct {
	action *serveraction.TransportGetIndexAction
}

func NewRestGetIndexAction(action *serveraction.TransportGetIndexAction) *RestGetIndexAction {
	return &RestGetIndexAction{action: action}
}

func (h *RestGetIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "GET", Path: "/{index}"},
	}
}

func (h *RestGetIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	result, err := h.action.Execute(serveraction.GetIndexRequest{Name: indexName})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "internal_error", errMsg)
		}
		return
	}

	// Build ES-compatible response: { "indexname": { "settings": {...}, "mappings": {...} } }
	mappingsResp := map[string]any{}
	if result.Mapping != nil && len(result.Mapping.Properties) > 0 {
		props := make(map[string]any, len(result.Mapping.Properties))
		for name, fm := range result.Mapping.Properties {
			prop := map[string]any{"type": string(fm.Type)}
			if fm.Analyzer != "" {
				prop["analyzer"] = fm.Analyzer
			}
			props[name] = prop
		}
		mappingsResp["properties"] = props
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		indexName: map[string]any{
			"settings": map[string]any{
				"number_of_shards":   result.Settings.NumberOfShards,
				"number_of_replicas": result.Settings.NumberOfReplicas,
			},
			"mappings": mappingsResp,
		},
	})
}
