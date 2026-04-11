package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	stateFileName = "cluster_state.json"
	stateTmpName  = "cluster_state.tmp"
)

// persistedStateFile is the on-disk JSON format for cluster state.
type persistedStateFile struct {
	Version  int64     `json:"version"`
	Metadata *Metadata `json:"metadata"`
}

// FilePersistedState implements PersistedState by writing metadata
// as JSON to {stateDir}/cluster_state.json with atomic tmp+rename.
type FilePersistedState struct {
	stateDir string
	metadata *Metadata
	version  int64
}

// NewFilePersistedState creates a FilePersistedState backed by the given directory.
// If a state file exists, it is loaded. If not, an empty metadata is used.
// Stale tmp files from interrupted writes are cleaned up.
func NewFilePersistedState(stateDir string) (*FilePersistedState, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	// Clean up stale tmp file from a crashed write
	tmpPath := filepath.Join(stateDir, stateTmpName)
	os.Remove(tmpPath) // ignore error — file may not exist

	statePath := filepath.Join(stateDir, stateFileName)
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		return &FilePersistedState{
			stateDir: stateDir,
			metadata: NewMetadata(),
			version:  0,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}

	var sf persistedStateFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("corrupt cluster state file %s: %w", statePath, err)
	}
	if sf.Metadata == nil {
		sf.Metadata = NewMetadata()
	}

	return &FilePersistedState{
		stateDir: stateDir,
		metadata: sf.Metadata,
		version:  sf.Version,
	}, nil
}

func (f *FilePersistedState) GetMetadata() *Metadata {
	return f.metadata
}

func (f *FilePersistedState) SetMetadata(metadata *Metadata) {
	f.version++
	f.metadata = metadata
	if err := f.writeToDisk(); err != nil {
		panic(fmt.Sprintf("failed to persist cluster state: %v", err))
	}
}

// Version returns the current version counter.
func (f *FilePersistedState) Version() int64 {
	return f.version
}

// writeToDisk atomically writes the current state to disk using tmp+rename.
func (f *FilePersistedState) writeToDisk() error {
	sf := persistedStateFile{
		Version:  f.version,
		Metadata: f.metadata,
	}
	data, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	tmpPath := filepath.Join(f.stateDir, stateTmpName)
	statePath := filepath.Join(f.stateDir, stateFileName)

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, statePath); err != nil {
		return fmt.Errorf("rename tmp to state: %w", err)
	}
	return nil
}
