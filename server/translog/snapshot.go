package translog

// Snapshot provides a point-in-time iterator over translog operations.
type Snapshot interface {
	TotalOperations() int
	Next() (Operation, error) // Returns nil, nil at end
	Close() error
}

// TranslogSnapshot is a cursor over a single reader's operations.
type TranslogSnapshot struct {
	reader            *BaseTranslogReader
	numOps            int
	currentOp         int
	offset            int64
	trimmedAboveSeqNo int64
}

func (s *TranslogSnapshot) TotalOperations() int {
	return s.numOps
}

func (s *TranslogSnapshot) Next() (Operation, error) {
	if s.currentOp >= s.numOps {
		return nil, nil
	}

	op, err := s.reader.Read(s.offset)
	if err != nil {
		return nil, err
	}

	// Advance offset past this operation by re-seeking to measure.
	currentPos, err := s.reader.file.Seek(0, 1) // current position after read
	if err != nil {
		return nil, err
	}
	s.offset = currentPos
	s.currentOp++

	// SeqNos are monotonically increasing in the file, so once we
	// hit a trimmed operation all remaining ones are also trimmed.
	if s.trimmedAboveSeqNo != SeqNoUnassigned && op.SeqNo() > s.trimmedAboveSeqNo {
		s.currentOp = s.numOps
		return nil, nil
	}

	return op, nil
}

func (s *TranslogSnapshot) Close() error {
	return nil // reader lifecycle managed separately
}

// MultiSnapshot composes multiple snapshots with deduplication.
// It iterates newest generation first so that newer operations win.
type MultiSnapshot struct {
	snapshots  []*TranslogSnapshot
	seenSeqNos map[int64]struct{}
	current    int
	totalOps   int
}

// NewMultiSnapshot creates a multi-snapshot from the given snapshots.
// Snapshots should be ordered from oldest to newest; iteration will
// proceed from newest first for deduplication.
func NewMultiSnapshot(snapshots []*TranslogSnapshot) *MultiSnapshot {
	total := 0
	for _, s := range snapshots {
		total += s.TotalOperations()
	}
	return &MultiSnapshot{
		snapshots:  snapshots,
		seenSeqNos: make(map[int64]struct{}),
		current:    len(snapshots) - 1, // start from newest
		totalOps:   total,
	}
}

func (ms *MultiSnapshot) TotalOperations() int {
	return ms.totalOps
}

func (ms *MultiSnapshot) Next() (Operation, error) {
	for ms.current >= 0 {
		op, err := ms.snapshots[ms.current].Next()
		if err != nil {
			return nil, err
		}
		if op == nil {
			// Current snapshot exhausted, move to older one.
			ms.current--
			continue
		}

		seqNo := op.SeqNo()
		if _, seen := ms.seenSeqNos[seqNo]; seen {
			continue // dedup: already seen from a newer generation
		}
		ms.seenSeqNos[seqNo] = struct{}{}
		return op, nil
	}
	return nil, nil
}

func (ms *MultiSnapshot) Close() error {
	for _, s := range ms.snapshots {
		s.Close()
	}
	return nil
}

// SeqNoFilterSnapshot filters operations to a [fromSeqNo, toSeqNo] range.
type SeqNoFilterSnapshot struct {
	inner     Snapshot
	fromSeqNo int64
	toSeqNo   int64
	filtered  int
}

// NewSeqNoFilterSnapshot wraps a snapshot with sequence number range filtering.
func NewSeqNoFilterSnapshot(inner Snapshot, fromSeqNo, toSeqNo int64) *SeqNoFilterSnapshot {
	return &SeqNoFilterSnapshot{
		inner:     inner,
		fromSeqNo: fromSeqNo,
		toSeqNo:   toSeqNo,
	}
}

func (f *SeqNoFilterSnapshot) TotalOperations() int {
	return f.inner.TotalOperations() - f.filtered
}

func (f *SeqNoFilterSnapshot) Next() (Operation, error) {
	for {
		op, err := f.inner.Next()
		if err != nil || op == nil {
			return op, err
		}

		seqNo := op.SeqNo()
		if seqNo < f.fromSeqNo || seqNo > f.toSeqNo {
			f.filtered++
			continue
		}
		return op, nil
	}
}

func (f *SeqNoFilterSnapshot) Close() error {
	return f.inner.Close()
}
