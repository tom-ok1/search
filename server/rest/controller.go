// server/rest/controller.go
package rest

import (
	"io"
	"net/http"
	"strings"
)

type Route struct {
	Method string
	Path   string
}

type RestHandler interface {
	Routes() []Route
	HandleRequest(req *RestRequest, resp *RestResponseWriter)
}

type HandlerFunc func(req *RestRequest, resp *RestResponseWriter)

type routeEntry struct {
	route   Route
	handler HandlerFunc
}

type RestController struct {
	routes []routeEntry
}

func NewRestController() *RestController {
	return &RestController{}
}

func (rc *RestController) RegisterHandler(handler RestHandler) {
	for _, route := range handler.Routes() {
		rc.routes = append(rc.routes, routeEntry{
			route:   route,
			handler: handler.HandleRequest,
		})
	}
}

func (rc *RestController) RegisterHandlerFunc(route Route, fn HandlerFunc) {
	rc.routes = append(rc.routes, routeEntry{
		route:   route,
		handler: fn,
	})
}

func (rc *RestController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := &RestResponseWriter{ResponseWriter: w}

	pathMatched := false
	for _, entry := range rc.routes {
		params, ok := matchPath(entry.route.Path, r.URL.Path)
		if !ok {
			continue
		}
		pathMatched = true
		if entry.route.Method != r.Method {
			continue
		}

		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}

		body, _ := io.ReadAll(r.Body)
		req := &RestRequest{
			Method: r.Method,
			Params: params,
			Body:   body,
		}
		entry.handler(req, resp)
		return
	}

	if pathMatched {
		resp.WriteError(http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	resp.WriteError(http.StatusNotFound, "not_found", "no handler found for "+r.URL.Path)
}

func matchPath(pattern, path string) (map[string]string, bool) {
	patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	if len(patternParts) != len(pathParts) {
		return nil, false
	}

	params := make(map[string]string)
	for i, pp := range patternParts {
		if strings.HasPrefix(pp, "{") && strings.HasSuffix(pp, "}") {
			paramName := pp[1 : len(pp)-1]
			params[paramName] = pathParts[i]
		} else if pp != pathParts[i] {
			return nil, false
		}
	}
	return params, true
}
