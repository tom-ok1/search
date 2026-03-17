package index

import (
	"encoding/json"
	"fmt"
	"gosearch/store"
	"io"
	"strings"
)

// SegmentInfos tracks all committed segments and the commit generation.
type SegmentInfos struct {
	Segments   []*SegmentCommitInfo `json:"segments"`
	Generation int64                `json:"generation"`
	Version    int64                `json:"version"`
}

// SegmentCommitInfo holds metadata for a single committed segment.
type SegmentCommitInfo struct {
	Name     string   `json:"name"`
	MaxDoc   int      `json:"max_doc"`
	DelCount int      `json:"del_count"`
	Fields   []string `json:"fields"`
	Files    []string `json:"files"` // all files belonging to this segment
}

// NewSegmentInfos creates an empty SegmentInfos.
func NewSegmentInfos() *SegmentInfos {
	return &SegmentInfos{}
}

// WritePending writes segment infos as pending_segments_N.
// Returns (pendingName, finalName, error). The caller is responsible for
// syncing, renaming pending → final, and syncing metadata.
func (si *SegmentInfos) WritePending(dir store.Directory) (string, string, error) {
	si.Generation++
	pendingName := fmt.Sprintf("pending_segments_%d", si.Generation)
	finalName := fmt.Sprintf("segments_%d", si.Generation)

	data, err := json.Marshal(si)
	if err != nil {
		return "", "", fmt.Errorf("marshal segment infos: %w", err)
	}

	out, err := dir.CreateOutput(pendingName)
	if err != nil {
		return "", "", fmt.Errorf("create %s: %w", pendingName, err)
	}
	defer out.Close()

	if _, err = out.Write(data); err != nil {
		return "", "", err
	}
	return pendingName, finalName, nil
}

// ReferencedFiles returns the set of all files referenced by the current commit:
// the segments_N file itself plus all files listed in each SegmentCommitInfo.
func (si *SegmentInfos) ReferencedFiles() map[string]bool {
	refs := make(map[string]bool)
	refs[fmt.Sprintf("segments_%d", si.Generation)] = true
	for _, info := range si.Segments {
		for _, f := range info.Files {
			refs[f] = true
		}
	}
	return refs
}

// AllFiles returns a flat slice of all files referenced by this SegmentInfos,
// including the segments_N file itself.
func (si *SegmentInfos) AllFiles() []string {
	files := []string{fmt.Sprintf("segments_%d", si.Generation)}
	for _, info := range si.Segments {
		files = append(files, info.Files...)
	}
	return files
}

// ReadLatestSegmentInfos reads the most recent segments_N file from the directory.
func ReadLatestSegmentInfos(dir store.Directory) (*SegmentInfos, error) {
	files, err := dir.ListAll()
	if err != nil {
		return nil, err
	}

	maxGen := int64(-1)
	var latestFile string
	for _, f := range files {
		if !strings.HasPrefix(f, "segments_") {
			continue
		}
		var gen int64
		if _, err := fmt.Sscanf(f, "segments_%d", &gen); err == nil && gen > maxGen {
			maxGen = gen
			latestFile = f
		}
	}

	if maxGen < 0 {
		return nil, fmt.Errorf("no segments file found")
	}

	in, err := dir.OpenInput(latestFile)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	data, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}

	var si SegmentInfos
	if err := json.Unmarshal(data, &si); err != nil {
		return nil, fmt.Errorf("unmarshal segment infos: %w", err)
	}

	return &si, nil
}
