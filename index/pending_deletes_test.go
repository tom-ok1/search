package index

import (
	"testing"

	"gosearch/store"
)

func TestNewPendingDeletes(t *testing.T) {
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 10}
	pd := NewPendingDeletes(info)

	if pd.NumPendingDeletes() != 0 {
		t.Errorf("NumPendingDeletes: got %d, want 0", pd.NumPendingDeletes())
	}
	// No deletions, all alive
	for i := range 10 {
		if pd.IsDeleted(i) {
			t.Errorf("doc %d should not be deleted", i)
		}
	}
	// GetLiveDocs should be nil when all alive
	if pd.GetLiveDocs() != nil {
		t.Error("GetLiveDocs should return nil when all docs alive")
	}
}

func TestPendingDeletesDelete(t *testing.T) {
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 5}
	pd := NewPendingDeletes(info)

	// First delete should return true (new deletion)
	if !pd.Delete(2) {
		t.Error("first Delete(2) should return true")
	}
	if pd.NumPendingDeletes() != 1 {
		t.Errorf("NumPendingDeletes: got %d, want 1", pd.NumPendingDeletes())
	}

	// Duplicate delete should return false
	if pd.Delete(2) {
		t.Error("duplicate Delete(2) should return false")
	}
	if pd.NumPendingDeletes() != 1 {
		t.Errorf("NumPendingDeletes after dup: got %d, want 1", pd.NumPendingDeletes())
	}

	// Delete another doc
	if !pd.Delete(4) {
		t.Error("Delete(4) should return true")
	}
	if pd.NumPendingDeletes() != 2 {
		t.Errorf("NumPendingDeletes: got %d, want 2", pd.NumPendingDeletes())
	}
}

func TestPendingDeletesIsDeleted(t *testing.T) {
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 5}
	pd := NewPendingDeletes(info)

	pd.Delete(1)
	pd.Delete(3)

	for i := range 5 {
		expected := i == 1 || i == 3
		if pd.IsDeleted(i) != expected {
			t.Errorf("IsDeleted(%d): got %v, want %v", i, pd.IsDeleted(i), expected)
		}
	}
}

func TestPendingDeletesCOWSemantics(t *testing.T) {
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 8}
	pd := NewPendingDeletes(info)

	// Delete and freeze
	pd.Delete(0)
	liveDocs1 := pd.GetLiveDocs()
	if liveDocs1 == nil {
		t.Fatal("GetLiveDocs should not be nil after deletion")
	}
	if !liveDocs1.Get(0) {
		t.Error("liveDocs1 should show doc 0 as deleted")
	}

	// Delete another doc — should NOT mutate the frozen snapshot
	pd.Delete(3)
	if liveDocs1.Get(3) {
		t.Error("frozen snapshot should not see new deletion")
	}

	// New snapshot should see both
	liveDocs2 := pd.GetLiveDocs()
	if !liveDocs2.Get(0) || !liveDocs2.Get(3) {
		t.Error("liveDocs2 should see both deletions")
	}
}

func TestPendingDeletesIsDeletedChecksWriteableThenLiveDocs(t *testing.T) {
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 8}
	pd := NewPendingDeletes(info)

	// Delete and freeze
	pd.Delete(1)
	pd.GetLiveDocs() // freeze → writeableLiveDocs becomes nil

	// IsDeleted should fall back to liveDocs
	if !pd.IsDeleted(1) {
		t.Error("IsDeleted(1) should be true (from liveDocs)")
	}

	// New delete → writeableLiveDocs is created
	pd.Delete(5)
	if !pd.IsDeleted(5) {
		t.Error("IsDeleted(5) should be true (from writeableLiveDocs)")
	}
}

func TestPendingDeletesWriteLiveDocs(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 8}
	pd := NewPendingDeletes(info)

	// No deletions — should write nothing
	name, err := pd.WriteLiveDocs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if name != "" {
		t.Errorf("expected empty name for no deletions, got %q", name)
	}

	// Delete and write
	pd.Delete(2)
	pd.Delete(5)
	name, err = pd.WriteLiveDocs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if name != "_seg0.del" {
		t.Errorf("expected _seg0.del, got %q", name)
	}
	if !dir.FileExists("_seg0.del") {
		t.Error("expected _seg0.del file to exist")
	}

	// After write, pending count should transfer to info.DelCount
	if pd.NumPendingDeletes() != 0 {
		t.Errorf("NumPendingDeletes after write: got %d, want 0", pd.NumPendingDeletes())
	}
	if info.DelCount != 2 {
		t.Errorf("info.DelCount: got %d, want 2", info.DelCount)
	}
}

func TestPendingDeletesWriteAndReadRoundtrip(t *testing.T) {
	dir, _ := store.NewFSDirectory(t.TempDir())
	info := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 16}
	pd := NewPendingDeletes(info)

	pd.Delete(0)
	pd.Delete(7)
	pd.Delete(15)
	pd.WriteLiveDocs(dir)

	// Read back
	delInput, err := store.OpenMMap(dir.FilePath("_seg0.del"))
	if err != nil {
		t.Fatal(err)
	}
	defer delInput.Close()

	info2 := &SegmentCommitInfo{Name: "_seg0", MaxDoc: 16}
	pd2, err := NewPendingDeletesFromDisk(info2, delInput)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 16 {
		expected := i == 0 || i == 7 || i == 15
		if pd2.IsDeleted(i) != expected {
			t.Errorf("roundtrip IsDeleted(%d): got %v, want %v", i, pd2.IsDeleted(i), expected)
		}
	}
}
