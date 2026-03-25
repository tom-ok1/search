// server/rest/controller_test.go
package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubHandler struct {
	routes []Route
	called bool
}

func (h *stubHandler) Routes() []Route { return h.routes }
func (h *stubHandler) HandleRequest(req *RestRequest, resp *RestResponseWriter) {
	h.called = true
	resp.WriteJSON(http.StatusOK, map[string]string{"status": "ok"})
}

func TestRestController_UnregisteredRouteReturns404(t *testing.T) {
	rc := NewRestController()
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	w := httptest.NewRecorder()

	rc.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestRestController_RegisterAndDispatch(t *testing.T) {
	rc := NewRestController()
	handler := &stubHandler{
		routes: []Route{{Method: "GET", Path: "/test"}},
	}
	rc.RegisterHandler(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if !handler.called {
		t.Error("expected handler to be called")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestRestController_MethodMismatchReturns405(t *testing.T) {
	rc := NewRestController()
	handler := &stubHandler{
		routes: []Route{{Method: "PUT", Path: "/test"}},
	}
	rc.RegisterHandler(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestRestController_PathParamsExtracted(t *testing.T) {
	rc := NewRestController()
	var captured map[string]string
	rc.RegisterHandlerFunc(Route{Method: "GET", Path: "/{index}/_search"}, func(req *RestRequest, resp *RestResponseWriter) {
		captured = req.Params
		resp.WriteJSON(http.StatusOK, nil)
	})

	req := httptest.NewRequest("GET", "/my-index/_search", nil)
	w := httptest.NewRecorder()
	rc.ServeHTTP(w, req)

	if captured["index"] != "my-index" {
		t.Errorf("expected param index='my-index', got %q", captured["index"])
	}
}
