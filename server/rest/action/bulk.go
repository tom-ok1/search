package action

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	serveraction "gosearch/server/action"
	"gosearch/server/rest"
)

type RestBulkAction struct {
	action *serveraction.TransportBulkAction
}

func NewRestBulkAction(action *serveraction.TransportBulkAction) *RestBulkAction {
	return &RestBulkAction{action: action}
}

func (h *RestBulkAction) Routes() []rest.Route {
	return []rest.Route{
		{Method: "POST", Path: "/_bulk"},
		{Method: "POST", Path: "/{index}/_bulk"},
	}
}

func (h *RestBulkAction) HandleRequest(req *rest.RestRequest, resp *rest.RestResponseWriter) {
	defaultIndex := req.Params["index"]

	items, err := parseNDJSON(req.Body, defaultIndex)
	if err != nil {
		resp.WriteError(http.StatusBadRequest, "parse_exception", "failed to parse bulk request: "+err.Error())
		return
	}

	result, err := h.action.Execute(serveraction.BulkRequest{Items: items})
	if err != nil {
		resp.WriteError(http.StatusInternalServerError, "bulk_exception", err.Error())
		return
	}

	responseItems := make([]map[string]any, 0, len(result.Items))
	for _, item := range result.Items {
		entry := map[string]any{
			"_index": item.Index,
			"_id":    item.ID,
			"status": item.Status,
		}
		if item.Error != nil {
			entry["error"] = map[string]any{
				"type":   item.Error.Type,
				"reason": item.Error.Reason,
			}
		}
		responseItems = append(responseItems, map[string]any{
			item.Action: entry,
		})
	}

	resp.WriteJSON(http.StatusOK, map[string]any{
		"took":   result.Took,
		"errors": result.Errors,
		"items":  responseItems,
	})
}

func parseNDJSON(body []byte, defaultIndex string) ([]serveraction.BulkItem, error) {
	var items []serveraction.BulkItem
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

			item := serveraction.BulkItem{
				Action: actionName,
				Index:  idx,
				ID:     metaObj.ID,
			}

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
