package index

import (
	"testing"

	"gosearch/store"
)

func newTestDir(t *testing.T) store.Directory {
	t.Helper()
	dir, err := store.NewFSDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestFileDeleter_DeleteIfUnreferenced_ImmediateDelete(t *testing.T) {
	dir := newTestDir(t)

	// Create a file
	out, err := dir.CreateOutput("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	out.Write([]byte("hello"))
	out.Close()

	fd := NewFileDeleter(dir)
	fd.DeleteIfUnreferenced([]string{"test.dat"})

	if dir.FileExists("test.dat") {
		t.Error("expected file to be deleted immediately when unreferenced")
	}
}

func TestFileDeleter_DeleteIfUnreferenced_DeferredWhenReferenced(t *testing.T) {
	dir := newTestDir(t)

	out, err := dir.CreateOutput("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	out.Write([]byte("hello"))
	out.Close()

	fd := NewFileDeleter(dir)

	// IncRef so the file is protected
	fd.IncRef([]string{"test.dat"})

	// Try to delete — should be deferred
	fd.DeleteIfUnreferenced([]string{"test.dat"})

	if !dir.FileExists("test.dat") {
		t.Error("expected file to still exist while referenced")
	}

	// DecRef — file should now be deleted
	fd.DecRef([]string{"test.dat"})

	if dir.FileExists("test.dat") {
		t.Error("expected file to be deleted after last reference released")
	}
}

func TestFileDeleter_MultipleRefs(t *testing.T) {
	dir := newTestDir(t)

	out, err := dir.CreateOutput("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	out.Write([]byte("hello"))
	out.Close()

	fd := NewFileDeleter(dir)

	// Two references
	fd.IncRef([]string{"test.dat"})
	fd.IncRef([]string{"test.dat"})

	fd.DeleteIfUnreferenced([]string{"test.dat"})

	// First DecRef — file should still exist
	fd.DecRef([]string{"test.dat"})
	if !dir.FileExists("test.dat") {
		t.Error("expected file to still exist with one remaining reference")
	}

	// Second DecRef — file should be deleted
	fd.DecRef([]string{"test.dat"})
	if dir.FileExists("test.dat") {
		t.Error("expected file to be deleted after all references released")
	}
}

func TestFileDeleter_DecRefWithoutPending(t *testing.T) {
	dir := newTestDir(t)

	out, err := dir.CreateOutput("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	out.Write([]byte("hello"))
	out.Close()

	fd := NewFileDeleter(dir)

	fd.IncRef([]string{"test.dat"})
	// DecRef without calling DeleteIfUnreferenced — file should not be deleted
	fd.DecRef([]string{"test.dat"})

	if !dir.FileExists("test.dat") {
		t.Error("expected file to remain when not pending deletion")
	}
}
