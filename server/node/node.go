package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-chi/chi/v5"
	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"

	"gosearch/analysis"
	"gosearch/api"
	"gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/gateway"
	"gosearch/server/handler"
	"gosearch/server/index"
)

type NodeConfig struct {
	DataPath string
	HTTPPort int
}

type Node struct {
	config        NodeConfig
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
	router        chi.Router
	registry      *analysis.AnalyzerRegistry
	httpServer    *http.Server
	listener      net.Listener
	stopped       bool
}

func NewNode(config NodeConfig) (*Node, error) {
	registry := analysis.DefaultRegistry()
	gw := gateway.NewGatewayMetaState()
	cs, indexServices, err := gw.Start(config.DataPath, registry)
	if err != nil {
		return nil, fmt.Errorf("recover cluster state: %w", err)
	}

	n := &Node{
		config:        config,
		clusterState:  cs,
		indexServices: indexServices,
		registry:      registry,
	}

	// Create transport actions
	createAction := action.NewTransportCreateIndexAction(cs, indexServices, config.DataPath, registry)
	deleteAction := action.NewTransportDeleteIndexAction(cs, indexServices, config.DataPath)
	getAction := action.NewTransportGetIndexAction(cs)

	indexDocAction := action.NewTransportIndexAction(cs, indexServices)
	getDocAction := action.NewTransportGetAction(cs, indexServices)
	deleteDocAction := action.NewTransportDeleteDocumentAction(cs, indexServices)
	refreshAction := action.NewTransportRefreshAction(cs, indexServices)

	searchAction := action.NewTransportSearchAction(cs, indexServices, registry)
	bulkAction := action.NewTransportBulkAction(cs, indexServices)

	// Create _cat actions
	catIndicesAction := action.NewTransportCatIndicesAction(cs, indexServices)
	catHealthAction := action.NewTransportCatHealthAction(cs, indexServices)

	// Create handler and wire up Chi router
	h := handler.NewHandler(
		createAction,
		deleteAction,
		getAction,
		indexDocAction,
		getDocAction,
		deleteDocAction,
		searchAction,
		bulkAction,
		refreshAction,
		catIndicesAction,
		catHealthAction,
	)

	strictHandler := api.NewStrictHandler(h, nil)

	spec, err := api.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("load openapi spec: %w", err)
	}
	spec.Servers = nil // Don't validate server URLs

	router := chi.NewRouter()
	router.Use(nethttpmiddleware.OapiRequestValidatorWithOptions(spec, &nethttpmiddleware.Options{
		Options: openapi3filter.Options{
			// Skip request body validation to avoid issues with NDJSON bulk
			// endpoints and custom content types. Path/query parameter
			// validation and route matching are still enforced.
			ExcludeRequestBody: true,
		},
		ErrorHandler: func(w http.ResponseWriter, message string, statusCode int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(statusCode)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"type":   "validation_exception",
					"reason": message,
				},
				"status": statusCode,
			})
		},
	}))
	api.HandlerFromMux(strictHandler, router)

	n.router = router

	return n, nil
}

func (n *Node) Start() (string, error) {
	addr := fmt.Sprintf(":%d", n.config.HTTPPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	n.listener = listener

	n.httpServer = &http.Server{
		Handler: n.router,
	}

	go n.httpServer.Serve(listener)

	return listener.Addr().String(), nil
}

func (n *Node) Stop() error {
	if n.stopped {
		return nil
	}
	n.stopped = true

	// Close all index services
	for name, svc := range n.indexServices {
		svc.Close()
		delete(n.indexServices, name)
	}

	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return n.httpServer.Shutdown(ctx)
	}
	return nil
}

func (n *Node) ClusterState() *cluster.ClusterState {
	return n.clusterState
}

func (n *Node) IndexService(name string) *index.IndexService {
	return n.indexServices[name]
}
