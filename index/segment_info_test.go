package index

import (
	"testing"

	"gosearch/store"
)

func TestNewSegmentInfos(t *testing.T) {
	si := NewSegmentInfos()
	if si.Generation != 0 {
		t.Errorf("Generation: got %d, want 0", si.Generation)
	}
	if si.Version != 0 {
		t.Errorf("Version: got %d, want 0", si.Version)
	}
	if len(si.Segments) != 0 {
		t.Errorf("Segments: got %d, want 0", len(si.Segments))
	}
}

func TestWritePendingAndReadBack(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	si := NewSegmentInfos()
	si.Segments = []*SegmentCommitInfo{
		{Name: "_seg0", MaxDoc: 10, Fields: []string{"body"}, Files: []string{"_seg0.meta"}},
		{Name: "_seg1", MaxDoc: 5, DelCount: 1, Fields: []string{"body", "title"}, Files: []string{"_seg1.meta"}},
	}

	pendingName, finalName, err := si.WritePending(dir)
	if err != nil {
		t.Fatal(err)
	}
	if pendingName != "pending_segments_1" {
		t.Errorf("pendingName: got %q, want %q", pendingName, "pending_segments_1")
	}
	if finalName != "segments_1" {
		t.Errorf("finalName: got %q, want %q", finalName, "segments_1")
	}
	if si.Generation != 1 {
		t.Errorf("Generation after write: got %d, want 1", si.Generation)
	}

	// Rename pending → final (simulating commit)
	if err := dir.Rename(pendingName, finalName); err != nil {
		t.Fatal(err)
	}

	// Read back
	readSI, err := ReadLatestSegmentInfos(dir)
	if err != nil {
		t.Fatal(err)
	}
	if readSI.Generation != 1 {
		t.Errorf("read Generation: got %d, want 1", readSI.Generation)
	}
	if len(readSI.Segments) != 2 {
		t.Fatalf("read Segments: got %d, want 2", len(readSI.Segments))
	}
	if readSI.Segments[0].Name != "_seg0" {
		t.Errorf("segment 0 name: got %q, want %q", readSI.Segments[0].Name, "_seg0")
	}
	if readSI.Segments[0].MaxDoc != 10 {
		t.Errorf("segment 0 MaxDoc: got %d, want 10", readSI.Segments[0].MaxDoc)
	}
	if readSI.Segments[1].DelCount != 1 {
		t.Errorf("segment 1 DelCount: got %d, want 1", readSI.Segments[1].DelCount)
	}
}

func TestWritePendingIncrementsGeneration(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	si := NewSegmentInfos()

	for i := 1; i <= 3; i++ {
		pending, final, err := si.WritePending(dir)
		if err != nil {
			t.Fatal(err)
		}
		dir.Rename(pending, final)
		if si.Generation != int64(i) {
			t.Errorf("generation after write %d: got %d, want %d", i, si.Generation, i)
		}
	}
}

func TestReadLatestSegmentInfosPicksHighestGeneration(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	si := NewSegmentInfos()

	// Write 3 generations
	for i := 0; i < 3; i++ {
		si.Segments = []*SegmentCommitInfo{
			{Name: "_seg0", MaxDoc: (i + 1) * 10},
		}
		pending, final, _ := si.WritePending(dir)
		dir.Rename(pending, final)
	}

	readSI, err := ReadLatestSegmentInfos(dir)
	if err != nil {
		t.Fatal(err)
	}
	if readSI.Generation != 3 {
		t.Errorf("Generation: got %d, want 3", readSI.Generation)
	}
	if readSI.Segments[0].MaxDoc != 30 {
		t.Errorf("MaxDoc: got %d, want 30 (from generation 3)", readSI.Segments[0].MaxDoc)
	}
}

func TestReadLatestSegmentInfosNoFile(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	_, err := ReadLatestSegmentInfos(dir)
	if err == nil {
		t.Error("expected error when no segments file exists")
	}
}

func TestReferencedFiles(t *testing.T) {
	si := &SegmentInfos{
		Generation: 2,
		Segments: []*SegmentCommitInfo{
			{Name: "_seg0", Files: []string{"_seg0.meta", "_seg0.body.tidx"}},
			{Name: "_seg1", Files: []string{"_seg1.meta", "_seg1.del"}},
		},
	}

	refs := si.ReferencedFiles()

	// Should contain the segments file itself
	if !refs["segments_2"] {
		t.Error("expected segments_2 in referenced files")
	}
	// Should contain all segment files
	for _, want := range []string{"_seg0.meta", "_seg0.body.tidx", "_seg1.meta", "_seg1.del"} {
		if !refs[want] {
			t.Errorf("expected %q in referenced files", want)
		}
	}
	// Should not contain unrelated files
	if refs["segments_1"] {
		t.Error("segments_1 should not be in referenced files")
	}
	if refs["_seg2.meta"] {
		t.Error("_seg2.meta should not be in referenced files")
	}
}

func TestSegmentCommitInfoFields(t *testing.T) {
	info := &SegmentCommitInfo{
		Name:     "_seg0",
		MaxDoc:   100,
		DelCount: 5,
		Fields:   []string{"body", "title"},
		Files:    []string{"_seg0.meta", "_seg0.body.tidx"},
	}

	if info.Name != "_seg0" {
		t.Errorf("Name: got %q", info.Name)
	}
	if info.MaxDoc != 100 {
		t.Errorf("MaxDoc: got %d", info.MaxDoc)
	}
	if info.DelCount != 5 {
		t.Errorf("DelCount: got %d", info.DelCount)
	}
	if len(info.Fields) != 2 {
		t.Errorf("Fields: got %d", len(info.Fields))
	}
	if len(info.Files) != 2 {
		t.Errorf("Files: got %d", len(info.Files))
	}
}
