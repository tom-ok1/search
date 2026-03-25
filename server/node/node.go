// server/node/node.go
package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/rest"
	"gosearch/server/transport"
)

type NodeConfig struct {
	DataPath string
	HTTPPort int
}

type Node struct {
	config         NodeConfig
	clusterState   *cluster.ClusterState
	restController *rest.RestController
	actionRegistry *transport.ActionRegistry
	httpServer     *http.Server
	listener       net.Listener
	stopped        bool
}

func NewNode(config NodeConfig) (*Node, error) {
	cs := cluster.NewClusterState()
	rc := rest.NewRestController()
	ar := transport.NewActionRegistry()

	return &Node{
		config:         config,
		clusterState:   cs,
		restController: rc,
		actionRegistry: ar,
	}, nil
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
