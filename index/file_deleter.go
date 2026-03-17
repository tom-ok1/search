package index

import (
	"sync"

	"gosearch/store"
)

// FileDeleter tracks per-file reference counts and defers deletion
// until all references are released.
type FileDeleter struct {
	mu       sync.Mutex
	dir      store.Directory
	refCount map[string]int  // filename → reference count
	pending  map[string]bool // files scheduled for deletion when refcount → 0
}

// NewFileDeleter creates a FileDeleter for the given directory.
func NewFileDeleter(dir store.Directory) *FileDeleter {
	return &FileDeleter{
		dir:      dir,
		refCount: make(map[string]int),
		pending:  make(map[string]bool),
	}
}

// IncRef increments reference counts for the given files.
func (fd *FileDeleter) IncRef(files []string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	for _, f := range files {
		fd.refCount[f]++
	}
}

// DecRef decrements reference counts for the given files.
// Any file whose count reaches 0 and is in the pending set is deleted immediately.
func (fd *FileDeleter) DecRef(files []string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	for _, f := range files {
		rc, ok := fd.refCount[f]
		if !ok {
			continue
		}
		rc--
		if rc <= 0 {
			delete(fd.refCount, f)
			if fd.pending[f] {
				delete(fd.pending, f)
				_ = fd.dir.DeleteFile(f)
			}
		} else {
			fd.refCount[f] = rc
		}
	}
}

// DeleteIfUnreferenced attempts to delete the given files.
// Files with refcount > 0 are added to the pending set and deleted later
// when their refcount reaches 0. Files with no references are deleted immediately.
func (fd *FileDeleter) DeleteIfUnreferenced(files []string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	for _, f := range files {
		if fd.refCount[f] > 0 {
			fd.pending[f] = true
		} else {
			_ = fd.dir.DeleteFile(f)
		}
	}
}
