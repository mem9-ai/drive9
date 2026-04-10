package fuse

import (
	"strings"
	"sync"
	"time"
)

// InodeEntry holds metadata for a single inode in the FUSE filesystem.
type InodeEntry struct {
	Ino      uint64
	Path     string
	IsDir    bool
	Nlookup  int64 // kernel lookup reference count
	Size     int64
	Mtime    time.Time
	Revision int64 // server-side revision for cache validation
}

// InodeToPath provides a bidirectional mapping between inode numbers and
// filesystem paths. It is safe for concurrent use.
type InodeToPath struct {
	mu      sync.RWMutex
	byInode map[uint64]*InodeEntry
	byPath  map[string]uint64
	nextIno uint64
}

// NewInodeToPath creates a new InodeToPath initialized with the root inode
// (inode 1, corresponding to go-fuse FUSE_ROOT_ID).
func NewInodeToPath() *InodeToPath {
	root := &InodeEntry{
		Ino:     1,
		Path:    "/",
		IsDir:   true,
		Nlookup: 1,
	}
	return &InodeToPath{
		byInode: map[uint64]*InodeEntry{1: root},
		byPath:  map[string]uint64{"/": 1},
		nextIno: 2,
	}
}

// Lookup returns the inode for the given path. If the path already exists, its
// Nlookup count is incremented and its size/mtime are updated. If the path
// does not exist, a new inode is allocated and an entry is created.
func (m *InodeToPath) Lookup(path string, isDir bool, size int64, mtime time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok {
		entry := m.byInode[ino]
		entry.Nlookup++
		entry.Size = size
		entry.Mtime = mtime
		return ino
	}

	ino := m.nextIno
	m.nextIno++

	entry := &InodeEntry{
		Ino:     ino,
		Path:    path,
		IsDir:   isDir,
		Nlookup: 1,
		Size:    size,
		Mtime:   mtime,
	}
	m.byInode[ino] = entry
	m.byPath[path] = ino
	return ino
}

// EnsureInode returns the inode for the given path, allocating one if it does
// not exist. Unlike Lookup, it does NOT increment the Nlookup counter. Use
// this for readdir entries where the kernel does not track a lookup reference.
func (m *InodeToPath) EnsureInode(path string, isDir bool, size int64, mtime time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok {
		entry := m.byInode[ino]
		entry.Size = size
		entry.Mtime = mtime
		return ino
	}

	ino := m.nextIno
	m.nextIno++

	entry := &InodeEntry{
		Ino:     ino,
		Path:    path,
		IsDir:   isDir,
		Nlookup: 0, // no kernel lookup reference yet
		Size:    size,
		Mtime:   mtime,
	}
	m.byInode[ino] = entry
	m.byPath[path] = ino
	return ino
}

// IncrementLookup adds one kernel lookup reference to an existing inode.
// Returns false if the inode does not exist.
func (m *InodeToPath) IncrementLookup(ino uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return false
	}
	entry.Nlookup++
	return true
}

// GetPath returns the path associated with the given inode number. The second
// return value is false if the inode is not found.
func (m *InodeToPath) GetPath(ino uint64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return "", false
	}
	return entry.Path, true
}

// GetInode returns the inode number associated with the given path. The second
// return value is false if the path is not found.
func (m *InodeToPath) GetInode(path string) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ino, ok := m.byPath[path]
	return ino, ok
}

// GetEntry returns a copy of the InodeEntry for the given inode number. A copy
// is returned to avoid data races. The second return value is false if the
// inode is not found.
func (m *InodeToPath) GetEntry(ino uint64) (*InodeEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return nil, false
	}
	cp := *entry
	return &cp, true
}

// Forget decrements the Nlookup count for the given inode by nlookup. If the
// resulting Nlookup is less than or equal to zero and the inode is not the
// root (inode 1), the entry is removed from both maps.
func (m *InodeToPath) Forget(ino uint64, nlookup uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}

	entry.Nlookup -= int64(nlookup)
	if entry.Nlookup <= 0 && ino != 1 {
		if entry.IsDir {
			// Preserve directory inode->path mappings after lookup refs drop.
			// Later mkdir/rename/rmdir calls can still reference the inode.
			entry.Nlookup = 0
			return
		}
		delete(m.byPath, entry.Path)
		delete(m.byInode, ino)
	}
}

// UpdateSize updates the size of the entry identified by the given inode.
func (m *InodeToPath) UpdateSize(ino uint64, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Size = size
	}
}

// UpdateMtime updates the mtime of the entry identified by the given inode.
func (m *InodeToPath) UpdateMtime(ino uint64, mtime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Mtime = mtime
	}
}

// UpdateRevision updates the server revision of the entry identified by ino.
func (m *InodeToPath) UpdateRevision(ino uint64, revision int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Revision = revision
	}
}

// Rename updates the path mapping when a file or directory is moved from
// oldPath to newPath. If the entry is a directory, all descendant entries
// whose paths have the prefix oldPath+"/" are also updated.
func (m *InodeToPath) Rename(oldPath, newPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ino, ok := m.byPath[oldPath]
	if !ok {
		return
	}

	entry := m.byInode[ino]

	// Update the entry itself.
	delete(m.byPath, oldPath)
	m.byPath[newPath] = ino
	entry.Path = newPath

	// If it is a directory, update all children with a matching prefix.
	if entry.IsDir {
		oldPrefix := oldPath + "/"
		// Collect children first to avoid modifying the map during iteration.
		type child struct {
			oldChildPath string
			childIno     uint64
		}
		var children []child
		for p, cIno := range m.byPath {
			if strings.HasPrefix(p, oldPrefix) {
				children = append(children, child{oldChildPath: p, childIno: cIno})
			}
		}
		for _, c := range children {
			newChildPath := newPath + "/" + strings.TrimPrefix(c.oldChildPath, oldPrefix)
			delete(m.byPath, c.oldChildPath)
			m.byPath[newChildPath] = c.childIno
			m.byInode[c.childIno].Path = newChildPath
		}
	}
}

// ForEach calls fn for each entry in the map while holding a read lock.
// fn receives copies of entries to avoid data races.
func (m *InodeToPath) ForEach(fn func(ino uint64, entry InodeEntry)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for ino, e := range m.byInode {
		fn(ino, *e)
	}
}

// Remove deletes the entry for the given path from both maps.
func (m *InodeToPath) Remove(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ino, ok := m.byPath[path]
	if !ok {
		return
	}
	delete(m.byPath, path)
	delete(m.byInode, ino)
}
