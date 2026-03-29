package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestRefreshAction struct {
	action *serveraction.TransportRefreshAction
}

func NewRestRefreshAction(action *serveraction.TransportRefreshAction) *RestRefreshAction {
	return &RestRefreshAction{action: action}
}

func (h *RestRefreshAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "POST", Path: "/{index}/_refresh"},
	}
}

func (h *RestRefreshAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	result, err := h.action.Execute(serveraction.RefreshRequest{Index: indexName})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "refresh_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"_shards": map[string]any{
			"total":      result.Shards,
			"successful": result.Shards,
			"failed":     0,
		},
	})
}
