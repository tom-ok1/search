package index

// MergeCandidate represents a set of segments that should be merged together.
type MergeCandidate struct {
	Segments []*SegmentCommitInfo
}

// MergePolicy decides which segments to merge.
type MergePolicy interface {
	FindMerges(infos []*SegmentCommitInfo) []MergeCandidate
}
