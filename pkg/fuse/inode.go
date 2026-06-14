package fuse

import (
	"strings"
	"sync"
	"time"
)

// InodeEntry holds metadata for a single inode in the FUSE filesystem.
type InodeEntry struct {
	Ino        uint64
	Path       string
	Paths      map[string]struct{}
	ResourceID string
	Nlink      uint32
	IsDir      bool
	Nlookup    int64 // kernel lookup reference count
	Size       int64
	Atime      time.Time
	Mtime      time.Time
	Ctime      time.Time
	Uid        uint32
	Gid        uint32
	HasUID     bool
	HasGID     bool
	Mode       uint32 // permission bits
	HasMode    bool   // true when mode is explicitly known (including 0)
	Rdev       uint32
	Revision   int64 // server-side revision for cache validation
	Unlinked   bool  // path was removed while open handles still reference this inode
}

// InodeToPath provides a bidirectional mapping between inode numbers and
// filesystem paths. It is safe for concurrent use.
type InodeToPath struct {
	mu      sync.RWMutex
	byInode map[uint64]*InodeEntry
	byPath  map[string]uint64
	byID    map[string]uint64
	nextIno uint64
}

// NewInodeToPath creates a new InodeToPath initialized with the root inode
// (inode 1, corresponding to go-fuse FUSE_ROOT_ID).
func NewInodeToPath() *InodeToPath {
	root := &InodeEntry{
		Ino:     1,
		Path:    "/",
		Paths:   map[string]struct{}{"/": {}},
		IsDir:   true,
		Nlink:   2,
		Nlookup: 1,
	}
	return &InodeToPath{
		byInode: map[uint64]*InodeEntry{1: root},
		byPath:  map[string]uint64{"/": 1},
		byID:    make(map[string]uint64),
		nextIno: 2,
	}
}

// Lookup returns the inode for the given path. If the path already exists, its
// Nlookup count is incremented and its size/mtime are updated. If the path
// does not exist, a new inode is allocated and an entry is created.
func (m *InodeToPath) Lookup(path string, isDir bool, size int64, mtime time.Time) uint64 {
	return m.LookupWithIdentity(path, "", 0, isDir, size, mtime)
}

// LookupWithIdentity is like Lookup, but non-directory entries with a stable
// resourceID share one FUSE inode across all known hardlink paths.
func (m *InodeToPath) LookupWithIdentity(path, resourceID string, nlink uint32, isDir bool, size int64, mtime time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok {
		entry := m.byInode[ino]
		entry.Nlookup++
		m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
		return ino
	}
	if key := inodeResourceKey(resourceID, isDir); key != "" {
		if ino, ok := m.byID[key]; ok {
			entry := m.byInode[ino]
			entry.Nlookup++
			m.addPathLocked(entry, path)
			m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
			return ino
		}
	}

	ino := m.nextIno
	m.nextIno++

	entry := &InodeEntry{
		Ino:        ino,
		Path:       path,
		Paths:      map[string]struct{}{path: {}},
		ResourceID: inodeResourceKey(resourceID, isDir),
		Nlink:      nlink,
		IsDir:      isDir,
		Nlookup:    1,
		Size:       size,
		Mtime:      mtime,
	}
	if entry.Nlink == 0 && !entry.IsDir {
		entry.Nlink = 1
	}
	m.byInode[ino] = entry
	m.byPath[path] = ino
	if entry.ResourceID != "" {
		m.byID[entry.ResourceID] = ino
	}
	return ino
}

// EnsureInode returns the inode for the given path, allocating one if it does
// not exist. Unlike Lookup, it does NOT increment the Nlookup counter. Use
// this for readdir entries where the kernel does not track a lookup reference.
func (m *InodeToPath) EnsureInode(path string, isDir bool, size int64, mtime time.Time) uint64 {
	return m.EnsureInodeWithIdentity(path, "", 0, isDir, size, mtime)
}

// EnsureInodeWithIdentity is like EnsureInode, but preserves hardlink identity
// for non-directory entries when resourceID is known.
func (m *InodeToPath) EnsureInodeWithIdentity(path, resourceID string, nlink uint32, isDir bool, size int64, mtime time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok {
		entry := m.byInode[ino]
		m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
		return ino
	}
	if key := inodeResourceKey(resourceID, isDir); key != "" {
		if ino, ok := m.byID[key]; ok {
			entry := m.byInode[ino]
			m.addPathLocked(entry, path)
			m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
			return ino
		}
	}

	ino := m.nextIno
	m.nextIno++

	entry := &InodeEntry{
		Ino:        ino,
		Path:       path,
		Paths:      map[string]struct{}{path: {}},
		ResourceID: inodeResourceKey(resourceID, isDir),
		Nlink:      nlink,
		IsDir:      isDir,
		Nlookup:    0, // no kernel lookup reference yet
		Size:       size,
		Mtime:      mtime,
	}
	if entry.Nlink == 0 && !entry.IsDir {
		entry.Nlink = 1
	}
	m.byInode[ino] = entry
	m.byPath[path] = ino
	if entry.ResourceID != "" {
		m.byID[entry.ResourceID] = ino
	}
	return ino
}

// EnsureInodeNoUpdate returns the inode for path, allocating one if needed.
// Unlike EnsureInode, an existing mapping is returned without mutating its
// cached metadata. Use this when recovering stale snapshot references.
func (m *InodeToPath) EnsureInodeNoUpdate(path string, isDir bool, size int64, mtime time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok {
		return ino
	}

	ino := m.nextIno
	m.nextIno++

	entry := &InodeEntry{
		Ino:     ino,
		Path:    path,
		Paths:   map[string]struct{}{path: {}},
		IsDir:   isDir,
		Nlink:   1,
		Nlookup: 0,
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
	return copyInodeEntryLocked(entry), true
}

// Forget decrements the Nlookup count for the given inode by nlookup. Some
// visible mappings are retained after lookup refs drop so later lookups can
// preserve POSIX inode identity and local owner metadata across rename.
func (m *InodeToPath) Forget(ino uint64, nlookup uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}

	entry.Nlookup -= int64(nlookup)
	if entry.Nlookup <= 0 && ino != 1 {
		if shouldKeepForgetMapping(entry) {
			entry.Nlookup = 0
			return
		}
		m.removeEntryLocked(ino, entry)
	}
}

func shouldKeepForgetMapping(entry *InodeEntry) bool {
	if entry == nil {
		return false
	}
	if entry.IsDir || entry.ResourceID != "" {
		return true
	}
	if entry.HasUID || entry.HasGID {
		return true
	}
	return entryIsMetadataOnlySpecial(entry)
}

// ForgetKeepMapping decrements the kernel lookup count without removing the
// inode/path mapping. Use this when a regular file is still represented by
// local open or pending state even though the kernel dropped its lookup ref.
func (m *InodeToPath) ForgetKeepMapping(ino uint64, nlookup uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}
	entry.Nlookup -= int64(nlookup)
	if entry.Nlookup < 0 {
		entry.Nlookup = 0
	}
}

// RemoveFileIfUnreferenced removes a regular-file mapping only when the kernel
// no longer holds lookup refs. Directory and root mappings are intentionally
// preserved because later directory operations may still reference them.
func (m *InodeToPath) RemoveFileIfUnreferenced(ino uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return false
	}
	if ino == 1 || entry.IsDir || entry.Nlookup > 0 {
		return false
	}
	m.removeEntryLocked(ino, entry)
	return true
}

// AddAlias maps path to an existing inode and increments its lookup count for
// the new kernel dentry returned by a successful FUSE Link call.
func (m *InodeToPath) AddAlias(ino uint64, path, resourceID string, nlink uint32, isDir bool, size int64, mtime time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return false
	}
	if replaced, exists := m.byPath[path]; exists && replaced != ino {
		m.removePathLocked(path, false, false)
	}
	m.addPathLocked(entry, path)
	entry.Path = path
	entry.Nlookup++
	m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
	return true
}

// AddAliasIfAbsent maps path to an existing inode only when path is still free.
func (m *InodeToPath) AddAliasIfAbsent(ino uint64, path, resourceID string, nlink uint32, isDir bool, size int64, mtime time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.byPath[path]; exists {
		return false
	}
	entry, ok := m.byInode[ino]
	if !ok {
		return false
	}
	m.addPathLocked(entry, path)
	entry.Path = path
	entry.Nlookup++
	m.updateEntryLocked(entry, path, resourceID, nlink, isDir, size, mtime)
	return true
}

// SetIdentity records a stable resource identity for an existing inode.
func (m *InodeToPath) SetIdentity(ino uint64, resourceID string, nlink uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}
	m.setIdentityLocked(entry, resourceID)
	if nlink > 0 {
		entry.Nlink = nlink
	}
}

// UpdateLinkCount updates the known hardlink count for an inode.
func (m *InodeToPath) UpdateLinkCount(ino uint64, nlink uint32) {
	if nlink == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Nlink = nlink
	}
}

// AdjustLinkCount atomically adjusts the known link count for an inode.
func (m *InodeToPath) AdjustLinkCount(ino uint64, delta int32) (*InodeEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok || !entry.IsDir {
		return nil, false
	}
	nlink := entry.Nlink
	if nlink == 0 {
		nlink = 2
	}
	next := int64(nlink) + int64(delta)
	if next < 2 {
		next = 2
	}
	entry.Nlink = uint32(next)
	return copyInodeEntryLocked(entry), true
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

// UpdateAtime updates the atime of the entry identified by the given inode.
func (m *InodeToPath) UpdateAtime(ino uint64, atime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Atime = atime
	}
}

// UpdateCtime updates the ctime of the entry identified by the given inode.
func (m *InodeToPath) UpdateCtime(ino uint64, ctime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Ctime = ctime
	}
}

// UpdateOwner updates the uid/gid of the entry identified by the given inode.
func (m *InodeToPath) UpdateOwner(ino uint64, uid, gid uint32, hasUID, hasGID bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		if hasUID {
			entry.Uid = uid
			entry.HasUID = true
		}
		if hasGID {
			entry.Gid = gid
			entry.HasGID = true
		}
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

// UpdateMode updates the permission bits of the entry identified by ino.
func (m *InodeToPath) UpdateMode(ino uint64, mode uint32) {
	m.SetModeState(ino, mode, true)
}

// UpdateRdev updates the device number of the entry identified by ino.
func (m *InodeToPath) UpdateRdev(ino uint64, rdev uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Rdev = rdev
	}
}

// SetModeState updates both permission bits and whether they are authoritative.
func (m *InodeToPath) SetModeState(ino uint64, mode uint32, hasMode bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Mode = mode
		entry.HasMode = hasMode
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
	if replacedIno, ok := m.byPath[newPath]; ok && replacedIno != ino {
		m.removePathLocked(newPath, true, true)
	}
	m.byPath[newPath] = ino
	if entry.Paths == nil {
		entry.Paths = make(map[string]struct{})
	}
	delete(entry.Paths, oldPath)
	entry.Paths[newPath] = struct{}{}
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
			childEntry := m.byInode[c.childIno]
			if childEntry.Paths == nil {
				childEntry.Paths = make(map[string]struct{})
			}
			delete(childEntry.Paths, c.oldChildPath)
			childEntry.Paths[newChildPath] = struct{}{}
			childEntry.Path = newChildPath
		}
	}
}

// Snapshot returns a copy of all entries. The caller can iterate outside
// the lock, avoiding holding the read-lock during expensive callbacks
// (e.g. kernel inode/entry notifications).
func (m *InodeToPath) Snapshot() []InodeEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]InodeEntry, 0, len(m.byInode))
	for _, e := range m.byInode {
		cp := *e
		if e.Paths != nil {
			cp.Paths = make(map[string]struct{}, len(e.Paths))
			for p := range e.Paths {
				cp.Paths[p] = struct{}{}
			}
		}
		entries = append(entries, cp)
	}
	return entries
}

// Remove deletes the entry for the given path from both maps.
func (m *InodeToPath) Remove(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removePathLocked(path, false, false)
}

// RemoveLink removes one path mapping for a successful unlink-like operation.
func (m *InodeToPath) RemoveLink(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removePathLocked(path, true, false)
}

// RemoveLinkPreserve removes one visible path mapping while preserving the
// inode entry if this was the last link and an open file handle still needs it.
func (m *InodeToPath) RemoveLinkPreserve(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removePathLocked(path, true, true)
}

func inodeResourceKey(resourceID string, isDir bool) string {
	if isDir || resourceID == "" {
		return ""
	}
	return resourceID
}

func (m *InodeToPath) addPathLocked(entry *InodeEntry, path string) {
	if entry.Paths == nil {
		entry.Paths = make(map[string]struct{})
		if entry.Path != "" {
			entry.Paths[entry.Path] = struct{}{}
		}
	}
	entry.Paths[path] = struct{}{}
	m.byPath[path] = entry.Ino
	if entry.Path == "" {
		entry.Path = path
	}
}

func (m *InodeToPath) updateEntryLocked(entry *InodeEntry, path, resourceID string, nlink uint32, isDir bool, size int64, mtime time.Time) {
	m.addPathLocked(entry, path)
	entry.IsDir = isDir
	entry.Size = size
	entry.Mtime = mtime
	m.setIdentityLocked(entry, resourceID)
	if nlink > 0 {
		entry.Nlink = nlink
	} else if entry.Nlink == 0 && !isDir {
		entry.Nlink = 1
	}
}

func (m *InodeToPath) setIdentityLocked(entry *InodeEntry, resourceID string) {
	key := inodeResourceKey(resourceID, entry.IsDir)
	if key == "" || entry.ResourceID == key {
		return
	}
	if entry.ResourceID != "" && m.byID[entry.ResourceID] == entry.Ino {
		delete(m.byID, entry.ResourceID)
	}
	entry.ResourceID = key
	m.byID[key] = entry.Ino
}

func (m *InodeToPath) removeEntryLocked(ino uint64, entry *InodeEntry) {
	for p := range entry.Paths {
		delete(m.byPath, p)
	}
	if entry.Path != "" {
		delete(m.byPath, entry.Path)
	}
	if entry.ResourceID != "" && m.byID[entry.ResourceID] == ino {
		delete(m.byID, entry.ResourceID)
	}
	delete(m.byInode, ino)
}

func (m *InodeToPath) removePathLocked(path string, consumeLink bool, preserveIfLast bool) {
	ino, ok := m.byPath[path]
	if !ok {
		return
	}
	entry, ok := m.byInode[ino]
	if !ok {
		delete(m.byPath, path)
		return
	}
	delete(m.byPath, path)
	delete(entry.Paths, path)
	if consumeLink && !entry.IsDir {
		if entry.Nlink > 1 {
			entry.Nlink--
		} else if preserveIfLast {
			entry.Nlink = 0
		}
		entry.Ctime = time.Now()
	}
	if entry.Path == path {
		entry.Path = ""
		for p := range entry.Paths {
			entry.Path = p
			break
		}
	}
	if entry.Path == "" && len(entry.Paths) == 0 {
		if preserveIfLast && !entry.IsDir {
			entry.Path = path
			entry.Unlinked = true
			entry.Nlink = 0
			return
		}
		m.removeEntryLocked(ino, entry)
		return
	}
	entry.Unlinked = false
}

func copyInodeEntryLocked(entry *InodeEntry) *InodeEntry {
	cp := *entry
	if entry.Paths != nil {
		cp.Paths = make(map[string]struct{}, len(entry.Paths))
		for p := range entry.Paths {
			cp.Paths[p] = struct{}{}
		}
	}
	return &cp
}
