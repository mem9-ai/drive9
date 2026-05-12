package fuse

import (
	"strings"
	"sync"
)

// OpenHandleIndex keeps O(1) indexes for currently open file handles.
// It is intentionally separate from HandleTable so FUSE fh lookup remains a
// simple id table while hot path lifecycle checks can avoid full-table scans.
type OpenHandleIndex struct {
	mu           sync.RWMutex
	byInode      map[uint64]map[*FileHandle]struct{}
	byPath       map[string]map[*FileHandle]struct{}
	pathByHandle map[*FileHandle]string
}

func NewOpenHandleIndex() *OpenHandleIndex {
	return &OpenHandleIndex{
		byInode:      make(map[uint64]map[*FileHandle]struct{}),
		byPath:       make(map[string]map[*FileHandle]struct{}),
		pathByHandle: make(map[*FileHandle]string),
	}
}

func (idx *OpenHandleIndex) Add(fh *FileHandle) {
	if idx == nil || fh == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	addHandleToSet(idx.byInode, fh.Ino, fh)
	addHandleToSet(idx.byPath, fh.Path, fh)
	idx.pathByHandle[fh] = fh.Path
}

func (idx *OpenHandleIndex) Remove(fh *FileHandle) {
	if idx == nil || fh == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	removeHandleFromSet(idx.byInode, fh.Ino, fh)
	if p, ok := idx.pathByHandle[fh]; ok {
		removeHandleFromSet(idx.byPath, p, fh)
		delete(idx.pathByHandle, fh)
	} else {
		for p := range idx.byPath {
			removeHandleFromSet(idx.byPath, p, fh)
		}
	}
}

// Has reports whether any open handle matches ino or p.
// When both are supplied this intentionally uses OR semantics to preserve the
// old conservative inode/path checks from the pre-index table scan.
func (idx *OpenHandleIndex) Has(ino uint64, p string) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if ino != 0 && len(idx.byInode[ino]) > 0 {
		return true
	}
	return p != "" && len(idx.byPath[p]) > 0
}

func (idx *OpenHandleIndex) SnapshotPath(p string) []*FileHandle {
	if idx == nil || p == "" {
		return nil
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	handles := idx.byPath[p]
	if len(handles) == 0 {
		return nil
	}
	out := make([]*FileHandle, 0, len(handles))
	for fh := range handles {
		out = append(out, fh)
	}
	return out
}

func (idx *OpenHandleIndex) RenamePathPrefix(oldPath, newPath string) map[*FileHandle]string {
	if idx == nil || oldPath == "" || newPath == "" {
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	oldPrefix := oldPath + "/"
	type move struct {
		from string
		to   string
	}
	var moves []move
	for p := range idx.byPath {
		switch {
		case p == oldPath:
			moves = append(moves, move{from: p, to: newPath})
		case strings.HasPrefix(p, oldPrefix):
			moves = append(moves, move{from: p, to: newPath + strings.TrimPrefix(p, oldPath)})
		}
	}

	retargeted := make(map[*FileHandle]string)
	for _, mv := range moves {
		handles := idx.byPath[mv.from]
		delete(idx.byPath, mv.from)
		for fh := range handles {
			addHandleToSet(idx.byPath, mv.to, fh)
			idx.pathByHandle[fh] = mv.to
			retargeted[fh] = mv.to
		}
	}
	if len(retargeted) == 0 {
		return nil
	}
	return retargeted
}

func (idx *OpenHandleIndex) Path(fh *FileHandle) (string, bool) {
	if idx == nil || fh == nil {
		return "", false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	p, ok := idx.pathByHandle[fh]
	return p, ok
}

func addHandleToSet[K comparable](m map[K]map[*FileHandle]struct{}, key K, fh *FileHandle) {
	handles := m[key]
	if handles == nil {
		handles = make(map[*FileHandle]struct{})
		m[key] = handles
	}
	handles[fh] = struct{}{}
}

func removeHandleFromSet[K comparable](m map[K]map[*FileHandle]struct{}, key K, fh *FileHandle) {
	handles := m[key]
	if handles == nil {
		return
	}
	delete(handles, fh)
	if len(handles) == 0 {
		delete(m, key)
	}
}

func (fs *Dat9FS) allocateFileHandle(fh *FileHandle) uint64 {
	fhID := fs.fileHandles.Allocate(fh)
	fs.openHandles.Add(fh)
	return fhID
}

func (fs *Dat9FS) deleteFileHandle(fhID uint64, fh *FileHandle) {
	if fh == nil {
		var ok bool
		fh, ok = fs.fileHandles.Get(fhID)
		if !ok {
			return
		}
	}
	fs.fileHandles.Delete(fhID)
	fs.openHandles.Remove(fh)
}
