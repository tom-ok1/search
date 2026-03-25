// server/rest/response.go
package rest

import (
	"encoding/json"
	"net/http"
)

type RestResponseWriter struct {
	http.ResponseWriter
}

func (w *RestResponseWriter) WriteJSON(status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		json.NewEncoder(w).Encode(body)
	}
}

func (w *RestResponseWriter) WriteError(status int, errType string, reason string) {
	w.WriteJSON(status, map[string]interface{}{
		"error": map[string]interface{}{
			"type":   errType,
			"reason": reason,
		},
		"status": status,
	})
}
