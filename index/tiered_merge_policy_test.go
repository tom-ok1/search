package index

import (
	"fmt"
	"testing"
)

func TestTieredMergePolicyNoMergeUnderThreshold(t *testing.T) {
	policy := NewTieredMergePolicy()
	policy.SegmentsPerTier = 5

	// Create 5 segments (at threshold) - should not merge
	infos := make([]*SegmentCommitInfo, 5)
	for i := range infos {
		infos[i] = &SegmentCommitInfo{
			Name:   fmt.Sprintf("_seg%d", i),
			MaxDoc: 100,
		}
	}

	result := policy.FindMerges(infos)
	if len(result) != 0 {
		t.Errorf("expected no merges for %d segments (threshold=%d), got %d",
			len(infos), policy.SegmentsPerTier, len(result))
	}
}

func TestTieredMergePolicyMergeOverThreshold(t *testing.T) {
	policy := NewTieredMergePolicy()
	policy.SegmentsPerTier = 5
	policy.MaxMergeAtOnce = 5

	// Create 11 segments (over threshold) - should merge
	infos := make([]*SegmentCommitInfo, 11)
	for i := range infos {
		infos[i] = &SegmentCommitInfo{
			Name:   fmt.Sprintf("_seg%d", i),
			MaxDoc: 100,
		}
	}

	result := policy.FindMerges(infos)
	if len(result) == 0 {
		t.Fatal("expected merge candidates, got none")
	}
	if len(result[0].Segments) == 0 {
		t.Fatal("expected segments in merge candidate")
	}
}

func TestTieredMergePolicyExcludesLargeSegments(t *testing.T) {
	policy := NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 3
	policy.MaxMergedSegmentDocs = 1000

	infos := []*SegmentCommitInfo{
		{Name: "_seg0", MaxDoc: 5000}, // too large
		{Name: "_seg1", MaxDoc: 5000}, // too large
		{Name: "_seg2", MaxDoc: 100},
		{Name: "_seg3", MaxDoc: 100},
		{Name: "_seg4", MaxDoc: 100},
		{Name: "_seg5", MaxDoc: 100},
	}

	result := policy.FindMerges(infos)
	if len(result) == 0 {
		t.Fatal("expected merge candidates")
	}

	// Verify large segments are not included
	for _, seg := range result[0].Segments {
		if seg.MaxDoc >= policy.MaxMergedSegmentDocs {
			t.Errorf("large segment %s (MaxDoc=%d) should not be in merge candidate",
				seg.Name, seg.MaxDoc)
		}
	}
}

func TestTieredMergePolicyFavorsDeletedDocs(t *testing.T) {
	policy := NewTieredMergePolicy()
	policy.SegmentsPerTier = 3
	policy.MaxMergeAtOnce = 3

	// Group A: segments with high deletion ratio (at the end after sorting)
	// Group B: segments with no deletions
	infos := []*SegmentCommitInfo{
		{Name: "_seg0", MaxDoc: 100, DelCount: 0},
		{Name: "_seg1", MaxDoc: 100, DelCount: 0},
		{Name: "_seg2", MaxDoc: 100, DelCount: 0},
		{Name: "_seg3", MaxDoc: 100, DelCount: 80}, // 80% deleted
		{Name: "_seg4", MaxDoc: 100, DelCount: 80},
		{Name: "_seg5", MaxDoc: 100, DelCount: 80},
	}

	result := policy.FindMerges(infos)
	if len(result) == 0 {
		t.Fatal("expected merge candidates")
	}

	// The merge candidate with high deletions should score higher
	hasDeletedSegment := false
	for _, seg := range result[0].Segments {
		if seg.DelCount > 0 {
			hasDeletedSegment = true
			break
		}
	}
	if !hasDeletedSegment {
		t.Error("expected merge candidate to include segments with deletions")
	}
}
