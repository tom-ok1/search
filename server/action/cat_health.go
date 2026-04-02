package action

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

// CatHealthResponse holds the response for the _cat/health action.
type CatHealthResponse struct {
	ClusterName string
	Status      string // green, yellow, red
	NodeTotal   int
	NodeData    int
	Shards      int
	Pri         int
	Relo        int
	Init        int
	Unassign    int
}

// FormatText returns the response formatted as a plain text table.
func (r *CatHealthResponse) FormatText() string {
	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 0, 1, ' ', 0)
	fmt.Fprintln(tw, "epoch\ttimestamp\tcluster\tstatus\tnode.total\tnode.data\tshards\tpri\trelo\tinit\tunassign")
	now := time.Now()
	fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
		now.Unix(), now.Format("15:04:05"), r.ClusterName, r.Status,
		r.NodeTotal, r.NodeData, r.Shards, r.Pri, r.Relo, r.Init, r.Unassign)
	tw.Flush()
	return sb.String()
}

// TransportCatHealthAction handles the _cat/health action.
type TransportCatHealthAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

// NewTransportCatHealthAction creates a new TransportCatHealthAction.
func NewTransportCatHealthAction(cs *cluster.ClusterState, services map[string]*index.IndexService) *TransportCatHealthAction {
	return &TransportCatHealthAction{
		clusterState:  cs,
		indexServices: services,
	}
}

// Execute returns the cluster health information.
func (a *TransportCatHealthAction) Execute() (CatHealthResponse, error) {
	md := a.clusterState.Metadata()

	totalPrimaryShards := 0
	status := "green"

	for _, meta := range md.Indices {
		totalPrimaryShards += meta.Settings.NumberOfShards

		// If any index has replicas configured on a single-node cluster, status is yellow
		if meta.Settings.NumberOfReplicas > 0 {
			status = "yellow"
		}
	}

	return CatHealthResponse{
		ClusterName: "gosearch",
		Status:      status,
		NodeTotal:   1,
		NodeData:    1,
		Shards:      totalPrimaryShards,
		Pri:         totalPrimaryShards,
		Relo:        0,
		Init:        0,
		Unassign:    0,
	}, nil
}
