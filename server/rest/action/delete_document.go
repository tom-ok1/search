package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestDeleteDocumentAction struct {
	action *serveraction.TransportDeleteDocumentAction
}

func NewRestDeleteDocumentAction(action *serveraction.TransportDeleteDocumentAction) *RestDeleteDocumentAction {
	return &RestDeleteDocumentAction{action: action}
}

func (h *RestDeleteDocumentAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "DELETE", Path: "/{index}/_doc/{id}"},
	}
}

func (h *RestDeleteDocumentAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]
	docID := req.Params["id"]

	result, err := h.action.Execute(serveraction.DeleteDocumentRequest{
		Index: indexName,
		ID:    docID,
	})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "delete_exception", errMsg)
		}
		return
	}

	status := http.StatusOK
	if result.Result == "not_found" {
		status = http.StatusNotFound
	}

	resp.WriteJSON(status, map[string]any{
		"_index": result.Index,
		"_id":    result.ID,
		"result": result.Result,
	})
}
