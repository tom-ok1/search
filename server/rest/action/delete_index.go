package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestDeleteIndexAction struct {
	action *serveraction.TransportDeleteIndexAction
}

func NewRestDeleteIndexAction(action *serveraction.TransportDeleteIndexAction) *RestDeleteIndexAction {
	return &RestDeleteIndexAction{action: action}
}

func (h *RestDeleteIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "DELETE", Path: "/{index}"},
	}
}

func (h *RestDeleteIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	result, err := h.action.Execute(serveraction.DeleteIndexRequest{Name: indexName})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "index_deletion_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"acknowledged": result.Acknowledged,
	})
}
