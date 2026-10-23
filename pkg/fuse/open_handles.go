package fuse

import (
	"strings"
	"sync"
)

// OpenHandleIndex keeps O(1) indexes for currently open file handles.
// It is intentionally separate from HandleTable so FUSE fh lookup remains a
// simple id table while hot path lifecycle checks can avoid full-table scans.
type OpenHandleIndex struct {
	mu       sync.Mutex
	byPath   map[string]map[*fileHandle]struct{}
	byInode  map[uint64]map[*fileHandle]struct{}
	unlinked map[*fileHandle]struct{}
}

func NewOpenHandleIndex() *OpenHandleIndex {
	return &OpenHandleIndex{
		byPath:   make(map[string]map[*fileHandle]struct{}),
		byInode:  make(map[uint64]map[*fileHandle]struct{}),
		unlinked: make(map[*fileHandle]struct{}),
	}
}

func (idx *OpenHandleIndex) Add(fh *fileHandle) {
	if fh == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.addLocked(fh)
}

func (idx *OpenHandleIndex) addLocked(fh *fileHandle) {
	addHandleToSet(idx.byPath, fh.path, fh)
	addHandleToSet(idx.byInode, fh.ino, fh)
	delete(idx.unlinked, fh)
}

func (idx *OpenHandleIndex) Remove(fh *fileHandle) {
	if fh == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	removeHandleFromSet(idx.byPath, fh.path, fh)
	removeHandleFromSet(idx.byInode, fh.ino, fh)
	delete(idx.unlinked, fh)
}

// HasOpenPath reports live handles on a path.
func (idx *OpenHandleIndex) HasOpenPath(p string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return len(idx.byPath[p]) > 0
}

// HasOpenInode reports live handles on an inode.
func (idx *OpenHandleIndex) HasOpenInode(ino uint64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return len(idx.byInode[ino]) > 0
}

// UnlinkPath marks every handle of p as anonymous (v1 semantics: the fd
// stays usable but no longer owns the path).
func (idx *OpenHandleIndex) UnlinkPath(p string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for fh := range idx.byPath[p] {
		idx.unlinked[fh] = struct{}{}
	}
	delete(idx.byPath, p)
}

// RelinkPath rebinds handles onto a fresh incarnation of p.
func (idx *OpenHandleIndex) RelinkPath(p string, handles []*fileHandle) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, fh := range handles {
		fh.path = p
		idx.addLocked(fh)
	}
}

func (idx *OpenHandleIndex) Has(ino uint64, p string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if handles := idx.byPath[p]; len(handles) > 0 {
		return true
	}
	for fh := range idx.byInode[ino] {
		if _, anon := idx.unlinked[fh]; !anon {
			return true
		}
	}
	return false
}

func (idx *OpenHandleIndex) SnapshotPath(p string) []*fileHandle {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	out := make([]*fileHandle, 0, len(idx.byPath[p]))
	for fh := range idx.byPath[p] {
		out = append(out, fh)
	}
	return out
}

func (idx *OpenHandleIndex) SnapshotInode(ino uint64) []*fileHandle {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	out := make([]*fileHandle, 0, len(idx.byInode[ino]))
	for fh := range idx.byInode[ino] {
		out = append(out, fh)
	}
	return out
}

// RenamePathPrefix moves every handle under oldPath onto newPath,
// returning the rebinding map (v1 semantics; caller applies path rewrites).
func (idx *OpenHandleIndex) RenamePathPrefix(oldPath, newPath string) map[*fileHandle]string {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	renamed := make(map[*fileHandle]string)
	prefix := strings.TrimSuffix(oldPath, "/") + "/"
	for p, handles := range idx.byPath {
		if p != oldPath && !strings.HasPrefix(p, prefix) {
			continue
		}
		next := newPath + strings.TrimPrefix(p, oldPath)
		for fh := range handles {
			fh.path = next
			renamed[fh] = next
			delete(idx.byPath, p)
			addHandleToSet(idx.byPath, next, fh)
		}
	}
	return renamed
}

func (idx *OpenHandleIndex) Path(fh *fileHandle) (string, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, anon := idx.unlinked[fh]; anon {
		return "", false
	}
	if _, ok := idx.byPath[fh.path][fh]; ok {
		return fh.path, true
	}
	return "", false
}

func addHandleToSet[K comparable](m map[K]map[*fileHandle]struct{}, key K, fh *fileHandle) {
	set, ok := m[key]
	if !ok {
		set = make(map[*fileHandle]struct{})
		m[key] = set
	}
	set[fh] = struct{}{}
}

func removeHandleFromSet[K comparable](m map[K]map[*fileHandle]struct{}, key K, fh *fileHandle) {
	set, ok := m[key]
	if !ok {
		return
	}
	delete(set, fh)
	if len(set) == 0 {
		delete(m, key)
	}
}
