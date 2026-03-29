package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"gosearch/analysis"
	"gosearch/server/action"
	"gosearch/server/cluster"
	"gosearch/server/index"
	"gosearch/server/rest"
	restaction "gosearch/server/rest/action"
	"gosearch/server/transport"
)

type NodeConfig struct {
	DataPath string
	HTTPPort int
}

type Node struct {
	config         NodeConfig
	clusterState   *cluster.ClusterState
	indexServices  map[string]*index.IndexService
	restController *rest.RestController
	actionRegistry *transport.ActionRegistry
	registry       *analysis.AnalyzerRegistry
	httpServer     *http.Server
	listener       net.Listener
	stopped        bool
}

func NewNode(config NodeConfig) (*Node, error) {
	cs := cluster.NewClusterState()
	rc := rest.NewRestController()
	ar := transport.NewActionRegistry()
	indexServices := make(map[string]*index.IndexService)
	registry := analysis.DefaultRegistry()

	n := &Node{
		config:         config,
		clusterState:   cs,
		indexServices:  indexServices,
		restController: rc,
		actionRegistry: ar,
		registry:       registry,
	}

	// Create transport actions
	createAction := action.NewTransportCreateIndexAction(cs, indexServices, config.DataPath, registry)
	deleteAction := action.NewTransportDeleteIndexAction(cs, indexServices, config.DataPath)
	getAction := action.NewTransportGetIndexAction(cs)

	// Register transport actions
	ar.Register(createAction)
	ar.Register(deleteAction)
	ar.Register(getAction)

	// Create and register REST handlers
	rc.RegisterHandler(restaction.NewRestCreateIndexAction(createAction))
	rc.RegisterHandler(restaction.NewRestDeleteIndexAction(deleteAction))
	rc.RegisterHandler(restaction.NewRestGetIndexAction(getAction))

	// Document CRUD actions
	indexDocAction := action.NewTransportIndexAction(cs, indexServices)
	getDocAction := action.NewTransportGetAction(cs, indexServices)
	deleteDocAction := action.NewTransportDeleteDocumentAction(cs, indexServices)
	refreshAction := action.NewTransportRefreshAction(cs, indexServices)

	ar.Register(indexDocAction)
	ar.Register(getDocAction)
	ar.Register(deleteDocAction)
	ar.Register(refreshAction)

	rc.RegisterHandler(restaction.NewRestIndexAction(indexDocAction))
	rc.RegisterHandler(restaction.NewRestGetAction(getDocAction))
	rc.RegisterHandler(restaction.NewRestDeleteDocumentAction(deleteDocAction))
	rc.RegisterHandler(restaction.NewRestRefreshAction(refreshAction))

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
		Handler: n.restController,
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

func (n *Node) ActionRegistry() *transport.ActionRegistry {
	return n.actionRegistry
}

func (n *Node) RestController() *rest.RestController {
	return n.restController
}

func (n *Node) IndexService(name string) *index.IndexService {
	return n.indexServices[name]
}
