package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestGetAction struct {
	action *serveraction.TransportGetAction
}

func NewRestGetAction(action *serveraction.TransportGetAction) *RestGetAction {
	return &RestGetAction{action: action}
}

func (h *RestGetAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "GET", Path: "/{index}/_doc/{id}"},
	}
}

func (h *RestGetAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]
	docID := req.Params["id"]

	result, err := h.action.Execute(serveraction.GetDocumentRequest{
		Index: indexName,
		ID:    docID,
	})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "get_exception", errMsg)
		}
		return
	}

	if !result.Found {
		resp.WriteJSON(http.StatusNotFound, map[string]any{
			"_index": result.Index,
			"_id":    result.ID,
			"found":  false,
		})
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"_index":  result.Index,
		"_id":     result.ID,
		"found":   true,
		"_source": result.Source,
	})
}
