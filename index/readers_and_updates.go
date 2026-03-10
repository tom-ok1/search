package index

import (
	"fmt"
	"gosearch/store"
)

// ReadersAndUpdates manages a single segment's reader and pending deletions.
// It lazily opens the DiskSegment and coordinates deletion state via PendingDeletes.
//
// Lucene equivalent: org.apache.lucene.index.ReadersAndUpdates (simplified)
type ReadersAndUpdates struct {
	info           *SegmentCommitInfo
	pendingDeletes *PendingDeletes
	reader         *DiskSegment // lazily opened
	dirPath        string
}

// NewReadersAndUpdates creates a ReadersAndUpdates for the given segment.
// The DiskSegment is not opened until getReader() is called.
func NewReadersAndUpdates(info *SegmentCommitInfo, dirPath string) *ReadersAndUpdates {
	return &ReadersAndUpdates{
		info:           info,
		pendingDeletes: NewPendingDeletes(info),
		dirPath:        dirPath,
	}
}

// Delete marks docID as deleted. Returns true if this was a new deletion.
func (rau *ReadersAndUpdates) Delete(docID int) bool {
	return rau.pendingDeletes.Delete(docID)
}

// IsDeleted reports whether the given docID has been deleted.
func (rau *ReadersAndUpdates) IsDeleted(docID int) bool {
	return rau.pendingDeletes.IsDeleted(docID)
}

// getReader lazily opens the DiskSegment and initializes PendingDeletes
// from any existing .del file on first access.
func (rau *ReadersAndUpdates) getReader() (*DiskSegment, error) {
	if rau.reader != nil {
		return rau.reader, nil
	}

	ds, err := OpenDiskSegment(rau.dirPath, rau.info.Name)
	if err != nil {
		return nil, err
	}
	rau.reader = ds

	// If the DiskSegment has an existing .del file, load it into PendingDeletes
	// as the initial liveDocs snapshot.
	if ds.deleted != nil {
		pd, err := NewPendingDeletesFromDisk(rau.info, ds.deleted)
		if err != nil {
			return nil, fmt.Errorf("load deletions for %s: %w", rau.info.Name, err)
		}
		rau.pendingDeletes = pd
	}

	return ds, nil
}

// GetSegmentReader returns a SegmentReader that reflects the current
// deletion state. If there are pending or committed deletions, it wraps
// the DiskSegment in a LiveDocsSegmentReader with a frozen liveDocs snapshot.
func (rau *ReadersAndUpdates) GetSegmentReader() (SegmentReader, error) {
	reader, err := rau.getReader()
	if err != nil {
		return nil, fmt.Errorf("get reader for %s: %w", rau.info.Name, err)
	}

	liveDocs := rau.pendingDeletes.GetLiveDocs()
	if liveDocs == nil {
		return reader, nil
	}

	return &LiveDocsSegmentReader{
		inner:    reader,
		liveDocs: liveDocs,
	}, nil
}

// WriteLiveDocs persists deletions to disk via PendingDeletes.
// Returns the name of the written file (empty if nothing was written).
func (rau *ReadersAndUpdates) WriteLiveDocs(dir store.Directory) (string, error) {
	return rau.pendingDeletes.WriteLiveDocs(dir)
}

// HasPendingDeletes reports whether there are uncommitted deletions.
func (rau *ReadersAndUpdates) HasPendingDeletes() bool {
	return rau.pendingDeletes.NumPendingDeletes() > 0
}

// Close releases the underlying DiskSegment.
func (rau *ReadersAndUpdates) Close() error {
	if rau.reader != nil {
		return rau.reader.Close()
	}
	return nil
}
