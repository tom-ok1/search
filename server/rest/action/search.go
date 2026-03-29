package action

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestSearchAction struct {
	action *serveraction.TransportSearchAction
}

func NewRestSearchAction(action *serveraction.TransportSearchAction) *RestSearchAction {
	return &RestSearchAction{action: action}
}

func (h *RestSearchAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "GET", Path: "/{index}/_search"},
		{Method: "POST", Path: "/{index}/_search"},
	}
}

func (h *RestSearchAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	var queryJSON map[string]any
	size := 10

	if len(req.Body) > 0 {
		var body map[string]any
		if err := json.Unmarshal(req.Body, &body); err != nil {
			resp.WriteError(http.StatusBadRequest, "parse_exception", "failed to parse request body: "+err.Error())
			return
		}
		if q, ok := body["query"].(map[string]any); ok {
			queryJSON = q
		}
		if s, ok := body["size"].(float64); ok {
			size = int(s)
		}
	}

	// Query param size overrides body
	if sizeStr, ok := req.Params["size"]; ok {
		if s, err := strconv.Atoi(sizeStr); err == nil {
			size = s
		}
	}

	// Default to match_all if no query specified
	if queryJSON == nil {
		queryJSON = map[string]any{"match_all": map[string]any{}}
	}

	result, err := h.action.Execute(serveraction.SearchRequest{
		Index:     indexName,
		QueryJSON: queryJSON,
		Size:      size,
	})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else if strings.Contains(errMsg, "parse query") {
			resp.WriteError(http.StatusBadRequest, "query_parsing_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "search_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"took": result.Took,
		"hits": map[string]any{
			"total": map[string]any{
				"value":    result.Hits.Total.Value,
				"relation": result.Hits.Total.Relation,
			},
			"max_score": result.Hits.MaxScore,
			"hits":      result.Hits.Hits,
		},
	})
}
