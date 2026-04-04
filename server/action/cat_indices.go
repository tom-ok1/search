package action

import (
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"gosearch/server/cluster"
	"gosearch/server/index"
)

// CatIndexInfo holds information about a single index for the _cat/indices response.
type CatIndexInfo struct {
	Health      string
	Status      string
	Index       string
	Pri         int
	Rep         int
	DocsCount   int
	DocsDeleted int
}

// CatIndicesResponse holds the response for the _cat/indices action.
type CatIndicesResponse struct {
	Indices []CatIndexInfo
}

// FormatText returns the response formatted as a plain text table.
func (r *CatIndicesResponse) FormatText() string {
	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 0, 1, ' ', 0)
	fmt.Fprintln(tw, "health\tstatus\tindex\tpri\trep\tdocs.count\tdocs.deleted")
	for _, idx := range r.Indices {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\n",
			idx.Health, idx.Status, idx.Index, idx.Pri, idx.Rep, idx.DocsCount, idx.DocsDeleted)
	}
	tw.Flush()
	return sb.String()
}

// TransportCatIndicesAction handles the _cat/indices action.
type TransportCatIndicesAction struct {
	clusterState  *cluster.ClusterState
	indexServices map[string]*index.IndexService
}

// NewTransportCatIndicesAction creates a new TransportCatIndicesAction.
func NewTransportCatIndicesAction(cs *cluster.ClusterState, services map[string]*index.IndexService) *TransportCatIndicesAction {
	return &TransportCatIndicesAction{
		clusterState:  cs,
		indexServices: services,
	}
}

// Execute returns index information for all indices in the cluster.
func (a *TransportCatIndicesAction) Execute() (CatIndicesResponse, error) {
	md := a.clusterState.Metadata()

	var indices []CatIndexInfo
	for _, meta := range md.Indices {
		info := CatIndexInfo{
			Index: meta.Name,
			Pri:   meta.Settings.NumberOfShards,
			Rep:   meta.Settings.NumberOfReplicas,
		}

		// Determine status
		if meta.State == cluster.IndexStateOpen {
			info.Status = "open"
		} else {
			info.Status = "close"
		}

		// Determine health: green if 0 replicas, yellow if replicas configured (single-node cluster)
		if meta.Settings.NumberOfReplicas == 0 {
			info.Health = "green"
		} else {
			info.Health = "yellow"
		}

		// Get doc counts from IndexService if available
		if svc, ok := a.indexServices[meta.Name]; ok {
			stats := svc.Stats()
			info.DocsCount = stats.DocCount
			info.DocsDeleted = stats.DeletedCount
		}

		indices = append(indices, info)
	}

	// Sort by index name
	sort.Slice(indices, func(i, j int) bool {
		return indices[i].Index < indices[j].Index
	})

	return CatIndicesResponse{Indices: indices}, nil
}
