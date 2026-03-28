package action

import (
	"encoding/json"
	"net/http"
	"strings"

	serveraction "gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/mapping"
	"gosearch/server/rest"
)

type RestCreateIndexAction struct {
	action *serveraction.TransportCreateIndexAction
}

func NewRestCreateIndexAction(action *serveraction.TransportCreateIndexAction) *RestCreateIndexAction {
	return &RestCreateIndexAction{action: action}
}

func (h *RestCreateIndexAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "PUT", Path: "/{index}"},
	}
}

func (h *RestCreateIndexAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	indexName := req.Params["index"]

	var body struct {
		Settings *cluster.IndexSettings `json:"settings"`
		Mappings *mappingsBody          `json:"mappings"`
	}

	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			resp.WriteError(http.StatusBadRequest, "parse_exception", "failed to parse request body: "+err.Error())
			return
		}
	}

	createReq := serveraction.CreateIndexRequest{
		Name: indexName,
	}

	if body.Settings != nil {
		createReq.Settings = *body.Settings
	}

	if body.Mappings != nil {
		createReq.Mappings = &mapping.MappingDefinition{
			Properties: body.Mappings.Properties,
		}
	}

	result, err := h.action.Execute(createReq)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			resp.WriteError(http.StatusBadRequest, "resource_already_exists_exception", errMsg)
		} else if strings.Contains(errMsg, "invalid index name") || strings.Contains(errMsg, "must not be empty") {
			resp.WriteError(http.StatusBadRequest, "invalid_index_name_exception", errMsg)
		} else {
			resp.WriteError(http.StatusInternalServerError, "index_creation_exception", errMsg)
		}
		return
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"acknowledged": result.Acknowledged,
		"index":        result.Index,
	})
}

type mappingsBody struct {
	Properties map[string]mapping.FieldMapping `json:"properties"`
}
