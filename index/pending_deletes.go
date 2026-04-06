package index

import (
	"gosearch/store"
)

// PendingDeletes tracks uncommitted deletions for a single segment.
// It uses copy-on-write semantics: liveDocs is the read-only snapshot,
// and writeableLiveDocs is created lazily on the first mutation.
//
// Lucene equivalent: org.apache.lucene.index.PendingDeletes
type PendingDeletes struct {
	info               *SegmentCommitInfo
	liveDocs           *Bitset // read-only snapshot; nil means all alive
	writeableLiveDocs  *Bitset // COW mutable copy; nil until first delete
	pendingDeleteCount int
}

// NewPendingDeletes creates a PendingDeletes for a segment with no prior deletions.
func NewPendingDeletes(info *SegmentCommitInfo) *PendingDeletes {
	return &PendingDeletes{info: info}
}

// NewPendingDeletesWithLiveDocs creates a PendingDeletes with an existing live docs bitset.
func NewPendingDeletesWithLiveDocs(info *SegmentCommitInfo, liveDocs *Bitset) *PendingDeletes {
	return &PendingDeletes{info: info, liveDocs: liveDocs}
}

// NewPendingDeletesFromDisk creates a PendingDeletes by reading an existing
// .del file. Format: [doc_count: uint32][bitmap: ceil(doc_count/8) bytes].
func NewPendingDeletesFromDisk(info *SegmentCommitInfo, delInput *store.MMapIndexInput) (*PendingDeletes, error) {
	docCount, err := delInput.ReadUint32()
	if err != nil {
		return nil, err
	}
	bitmapLen := (int(docCount) + 7) / 8
	bitmapData, err := delInput.ReadBytes(bitmapLen)
	if err != nil {
		return nil, err
	}
	return &PendingDeletes{
		info:     info,
		liveDocs: BitsetFromBytes(bitmapData, int(docCount)),
	}, nil
}

// Delete marks docID as deleted. Returns true if this was a new deletion.
// Lazily clones liveDocs into writeableLiveDocs on first call (COW).
func (pd *PendingDeletes) Delete(docID int) bool {
	mutable := pd.getMutableBits()
	if mutable.Get(docID) {
		return false // already deleted
	}
	mutable.Set(docID)
	pd.pendingDeleteCount++
	return true
}

// getMutableBits returns the mutable bitset, creating a COW clone if needed.
func (pd *PendingDeletes) getMutableBits() *Bitset {
	if pd.writeableLiveDocs != nil {
		return pd.writeableLiveDocs
	}
	if pd.liveDocs != nil {
		pd.writeableLiveDocs = pd.liveDocs.Clone()
	} else {
		pd.writeableLiveDocs = NewBitset(pd.info.MaxDoc)
	}
	return pd.writeableLiveDocs
}

// GetLiveDocs returns a read-only snapshot of the current live docs bitset.
// After this call, the writeable copy is frozen as the new snapshot.
// Returns nil if all docs are alive.
func (pd *PendingDeletes) GetLiveDocs() *Bitset {
	if pd.writeableLiveDocs != nil {
		pd.liveDocs = pd.writeableLiveDocs
		pd.writeableLiveDocs = nil
	}
	return pd.liveDocs
}

// IsDeleted reports whether docID is marked as deleted.
func (pd *PendingDeletes) IsDeleted(docID int) bool {
	if pd.writeableLiveDocs != nil {
		return pd.writeableLiveDocs.Get(docID)
	}
	if pd.liveDocs != nil {
		return pd.liveDocs.Get(docID)
	}
	return false
}

// NumPendingDeletes returns the number of deletions not yet committed.
func (pd *PendingDeletes) NumPendingDeletes() int {
	return pd.pendingDeleteCount
}

// WriteLiveDocs writes the current deletion state to a .del file.
// After writing, pendingDeleteCount is transferred to info.DelCount and reset.
// Returns the name of the written file (empty if nothing was written).
func (pd *PendingDeletes) WriteLiveDocs(dir store.Directory) (string, error) {
	liveDocs := pd.GetLiveDocs()
	if liveDocs == nil {
		return "", nil
	}

	bitmapBytes := liveDocs.Bytes()

	delFileName := pd.info.Name + ".del"
	out, err := dir.CreateOutput(delFileName)
	if err != nil {
		return "", err
	}
	defer out.Close()
	if err := out.WriteUint32(uint32(pd.info.MaxDoc)); err != nil {
		return "", err
	}
	if _, err := out.Write(bitmapBytes); err != nil {
		return "", err
	}

	pd.info.DelCount += pd.pendingDeleteCount
	pd.pendingDeleteCount = 0
	return delFileName, nil
}
