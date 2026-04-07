package index

import "sort"

// TieredMergePolicy selects merge candidates based on segment size tiers.
// It favors merging similarly-sized small segments and segments with high deletion ratios.
type TieredMergePolicy struct {
	MaxMergeAtOnce        int
	SegmentsPerTier       int
	MaxMergedSegmentDocs  int
	DeletedDocsPctAllowed float64
}

// NewTieredMergePolicy returns a TieredMergePolicy with default settings.
func NewTieredMergePolicy() *TieredMergePolicy {
	return &TieredMergePolicy{
		MaxMergeAtOnce:        10,
		SegmentsPerTier:       10,
		MaxMergedSegmentDocs:  5000000,
		DeletedDocsPctAllowed: 0.33,
	}
}

// FindMerges returns merge candidates based on segment sizes and deletion ratios.
func (p *TieredMergePolicy) FindMerges(infos []*SegmentCommitInfo) []MergeCandidate {
	// Sort segments by live doc count descending
	eligible := make([]*SegmentCommitInfo, 0, len(infos))
	for _, info := range infos {
		liveDocs := info.MaxDoc - info.DelCount
		if liveDocs < p.MaxMergedSegmentDocs {
			eligible = append(eligible, info)
		}
	}

	sort.Slice(eligible, func(i, j int) bool {
		liveI := eligible[i].MaxDoc - eligible[i].DelCount
		liveJ := eligible[j].MaxDoc - eligible[j].DelCount
		return liveI > liveJ
	})

	if len(eligible) <= p.SegmentsPerTier {
		return nil
	}

	// Slide a window of up to MaxMergeAtOnce segments and find the best scoring merge.
	windowSize := min(p.MaxMergeAtOnce, len(eligible))
	if windowSize < 2 {
		return nil
	}

	bestScore := -1.0
	bestStart := -1

	for start := 0; start+windowSize <= len(eligible); start++ {
		window := eligible[start : start+windowSize]
		score := p.scoreMerge(window)
		if score > bestScore {
			bestScore = score
			bestStart = start
		}
	}

	if bestStart < 0 {
		return nil
	}

	window := eligible[bestStart : bestStart+windowSize]
	return []MergeCandidate{{Segments: window}}
}

// scoreMerge scores a merge candidate. Higher is better.
func (p *TieredMergePolicy) scoreMerge(infos []*SegmentCommitInfo) float64 {
	if len(infos) == 0 {
		return 0
	}

	var totalDocs, totalDeleted, maxSize int
	for _, info := range infos {
		live := info.MaxDoc - info.DelCount
		totalDocs += info.MaxDoc
		totalDeleted += info.DelCount
		if live > maxSize {
			maxSize = live
		}
	}

	if totalDocs == 0 {
		return 0
	}

	avgSize := float64(totalDocs-totalDeleted) / float64(len(infos))
	if avgSize == 0 {
		avgSize = 1
	}

	skew := float64(maxSize) / avgSize
	if skew < 1 {
		skew = 1
	}

	deletedRatio := float64(totalDeleted) / float64(totalDocs)

	return deletedRatio + (1.0 / skew)
}

// Compile-time check
var _ MergePolicy = (*TieredMergePolicy)(nil)
