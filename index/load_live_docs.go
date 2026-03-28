package index

import (
	"errors"
	"fmt"
	"gosearch/store"
	"os"
)

// loadLiveDocs loads the deletion bitset for a segment from its .del file.
// Returns nil if no .del file exists (all docs alive).
func loadLiveDocs(dirPath string, info *SegmentCommitInfo) (*Bitset, error) {
	delPath := fmt.Sprintf("%s/%s.del", dirPath, info.Name)
	delInput, err := store.OpenMMap(delPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("mmap del for %s: %w", info.Name, err)
	}
	defer delInput.Close()

	pd, err := NewPendingDeletesFromDisk(info, delInput)
	if err != nil {
		return nil, fmt.Errorf("load del for %s: %w", info.Name, err)
	}
	return pd.GetLiveDocs(), nil
}
