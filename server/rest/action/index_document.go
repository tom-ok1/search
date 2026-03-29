package action

import (
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestIndexAction struct {
	action *serveraction.TransportIndexAction
}

func NewRestIndexAction(action *serveraction.TransportIndexAction) *RestIndexAction {
	return &RestIndexAction{action: action}
}

func (h *RestIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "PUT", Path: "/{index}/_doc/{id}"},
		{Method: "POST", Path: "/{index}/_doc/{id}"},
	}
}

func (h *RestIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]
	docID := req.Params["id"]

	result, err := h.action.Execute(serveraction.IndexDocumentRequest{
		Index:  indexName,
		ID:     docID,
		Source: req.Body,
	})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such index") {
			resp.WriteError(http.StatusNotFound, "index_not_found_exception", errMsg)
		} else {
			resp.WriteError(http.StatusBadRequest, "mapper_parsing_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusCreated, map[string]any{
		"_index": result.Index,
		"_id":    result.ID,
		"result": result.Result,
	})
}
