package index

import "gosearch/store"

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

	liveDocs, err := loadLiveDocs(rau.dirPath, rau.info)
	if err != nil {
		return nil, err
	}
	if liveDocs != nil {
		rau.pendingDeletes = NewPendingDeletesWithLiveDocs(rau.info, liveDocs)
	}

	return ds, nil
}

// GetLiveDocs returns a read-only snapshot of the current deletion state.
// Returns nil if all documents are alive.
func (rau *ReadersAndUpdates) GetLiveDocs() *Bitset {
	return rau.pendingDeletes.GetLiveDocs()
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
		err := rau.reader.Close()
		rau.reader = nil
		return err
	}
	return nil
}
