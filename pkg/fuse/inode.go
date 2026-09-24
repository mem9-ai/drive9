package fuse

import (
	"path"
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
	// ExtentIno is the JuiceFS inode for content_layout=extent files.
	// Shared across hardlink aliases of the same FUSE inode.
	ExtentIno uint64
	// MtimeOverride/AtimeOverride hold times explicitly set by a time-only
	// SetAttr (utimensat/futimens), including while the file's content commit
	// is still pending. While armed, derived refreshes (async commit
	// completion, remote stat, directory re-listings) must not clobber them;
	// a later local data mutation (write, truncate — see markDirtySize) or a
	// remote identity replacement clears them. Mode-only changes (chmod) do
	// not: POSIX does not advance mtime for them. The pointed-to values are
	// immutable once published; holders only replace the pointer, never
	// mutate through it.
	MtimeOverride *time.Time
	AtimeOverride *time.Time
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

// GetRevision reads the revision without copying the inode's paths or metadata.
// Zero means either no known server revision or no inode entry.
func (m *InodeToPath) GetRevision(ino uint64) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if entry := m.byInode[ino]; entry != nil {
		return entry.Revision
	}
	return 0
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
	// JuiceFS uses the juicefs inode as the FUSE nodeid, so Forget cannot
	// drop the identity. Dat9FS allocates its own nodeids and must keep the
	// juicefs Ino mapping (and last known length) across kernel Forget.
	if entry.ExtentIno != 0 {
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

// SetExtentIno records the JuiceFS inode for a FUSE inode. Hardlink aliases
// share the FUSE inode, so one update covers every path.
func (m *InodeToPath) SetExtentIno(ino uint64, extentIno uint64) {
	if m == nil || extentIno == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.byInode[ino]; ok {
		entry.ExtentIno = extentIno
	}
}

// ClearExtentIno drops the JuiceFS inode recorded for a FUSE inode. Callers
// use it when the path stops being the extent file that inode belonged to.
func (m *InodeToPath) ClearExtentIno(ino uint64) {
	if m == nil || ino == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.byInode[ino]; ok {
		entry.ExtentIno = 0
	}
}

// FindByExtentIno returns the FUSE inode that already owns this JuiceFS
// inode, so Lookup after rename/hardlink reuses the same nodeid.
func (m *InodeToPath) FindByExtentIno(extentIno uint64) (uint64, bool) {
	if m == nil || extentIno == 0 {
		return 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for ino, entry := range m.byInode {
		if entry != nil && !entry.IsDir && entry.ExtentIno == extentIno {
			return ino, true
		}
	}
	return 0, false
}

// FindPathInDir locates a child by basename under dir, ignoring extra slashes
// so finishLocalRename still finds the source after childPath normalization.
func (m *InodeToPath) FindPathInDir(dir, name string) (uint64, string, bool) {
	if m == nil || name == "" {
		return 0, "", false
	}
	dir = path.Clean("/" + strings.TrimPrefix(strings.TrimSuffix(dir, "/"), "/"))
	if dir == "." {
		dir = "/"
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for p, ino := range m.byPath {
		trimmed := strings.TrimSuffix(p, "/")
		if path.Base(trimmed) != name {
			continue
		}
		parent := path.Dir(trimmed)
		if parent == "." {
			parent = "/"
		}
		if parent == dir {
			return ino, p, true
		}
	}
	return 0, "", false
}

// FindByBaseName returns the unique path with this basename. pjdfstest
// names are unique hashes; rename can then find the source even when the
// parent path string does not match GetPath.
func (m *InodeToPath) FindByBaseName(name string) (uint64, string, bool) {
	if m == nil || name == "" || name == "/" || name == "." {
		return 0, "", false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	found := 0
	var ino uint64
	var pth string
	for p, i := range m.byPath {
		if path.Base(strings.TrimSuffix(p, "/")) != name {
			continue
		}
		found++
		ino, pth = i, p
		if found > 1 {
			return 0, "", false
		}
	}
	if found != 1 {
		return 0, "", false
	}
	return ino, pth, true
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
// This is the user-mutation variant: the new time is authoritative, so any
// armed local time override is cleared.
func (m *InodeToPath) UpdateMtime(ino uint64, mtime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Mtime = mtime
		entry.MtimeOverride = nil
	}
}

// UpdateAtime updates the atime of the entry identified by the given inode.
// This is the user-mutation variant: the new time is authoritative, so any
// armed local time override is cleared.
func (m *InodeToPath) UpdateAtime(ino uint64, atime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Atime = atime
		entry.AtimeOverride = nil
	}
}

// SetLocalMtime records an mtime explicitly requested via SetAttr and arms
// the local override so derived refreshes (commit completion, stat, listings)
// cannot clobber it until the next local mutation clears it.
func (m *InodeToPath) SetLocalMtime(ino uint64, mtime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Mtime = mtime
		t := mtime
		entry.MtimeOverride = &t
	}
}

// SetLocalAtime records an atime explicitly requested via SetAttr and arms
// the local override so derived refreshes cannot clobber it.
func (m *InodeToPath) SetLocalAtime(ino uint64, atime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		entry.Atime = atime
		t := atime
		entry.AtimeOverride = &t
	}
}

// MtimeOverride returns the armed local mtime override, if any.
func (m *InodeToPath) MtimeOverride(ino uint64) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	if !ok || entry.MtimeOverride == nil {
		return time.Time{}, false
	}
	return *entry.MtimeOverride, true
}

// HasMtimeOverride reports whether a local mtime override is armed.
func (m *InodeToPath) HasMtimeOverride(ino uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	return ok && entry.MtimeOverride != nil
}

// HasAtimeOverride reports whether a local atime override is armed.
func (m *InodeToPath) HasAtimeOverride(ino uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	return ok && entry.AtimeOverride != nil
}

// UpdateMtimeDerived updates the mtime from a derived source (async commit
// completion, remote stat refresh). An armed local override wins.
func (m *InodeToPath) UpdateMtimeDerived(ino uint64, mtime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok || entry.MtimeOverride != nil {
		return
	}
	entry.Mtime = mtime
}

// ClearLocalTimes drops any armed local time overrides without changing the
// current times. It marks the canonical user-mutation point (a local data
// write/truncate, see markDirtySize): later commit settles and stat
// refreshes become authoritative again, so utimensat-then-write cannot leave
// the mtime frozen at the explicitly requested value.
func (m *InodeToPath) ClearLocalTimes(ino uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.byInode[ino]; ok {
		clearTimeOverridesLocked(entry)
	}
}

// clearTimeOverridesLocked drops both local time overrides on an entry the
// caller holds locked. Used at every point where the inode's object or
// mutation history invalidates explicitly SetAttr'd times: local data
// mutations, kind changes, and remote identity replacements.
func clearTimeOverridesLocked(entry *InodeEntry) {
	entry.MtimeOverride = nil
	entry.AtimeOverride = nil
}

// localTimeOverrides is a snapshot of the armed local time overrides. The
// pointed-to values are immutable once published, so sharing the pointers
// is safe.
type localTimeOverrides struct {
	mtime *time.Time
	atime *time.Time
}

// SnapshotLocalTimes captures the armed local time overrides as a rollback
// point. A write-sync attempt clears them at its dirty-mutation hook; if
// the write fails and the content rolls back, the snapshot re-arms them.
func (m *InodeToPath) SnapshotLocalTimes(ino uint64) *localTimeOverrides {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	if !ok || (entry.MtimeOverride == nil && entry.AtimeOverride == nil) {
		return nil
	}
	return &localTimeOverrides{mtime: entry.MtimeOverride, atime: entry.AtimeOverride}
}

// RestoreLocalTimes re-arms overrides from a snapshot taken before a failed
// mutation. A slot armed after the snapshot (a newer concurrent utimensat)
// wins and is kept; a nil snapshot is a no-op.
func (m *InodeToPath) RestoreLocalTimes(ino uint64, snap *localTimeOverrides) {
	if snap == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}
	if snap.mtime != nil && entry.MtimeOverride == nil {
		entry.MtimeOverride = snap.mtime
	}
	if snap.atime != nil && entry.AtimeOverride == nil {
		entry.AtimeOverride = snap.atime
	}
}

// ReplacesIdentity reports whether binding the observed resourceID/isDir to
// the inode would replace a known identity — i.e. a different drive9 object
// (kind change or both resource ids known and different) now owns the path.
func (m *InodeToPath) ReplacesIdentity(ino uint64, resourceID string, isDir bool) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return false
	}
	if entry.IsDir != isDir {
		return true
	}
	return identityReplacedLocked(entry, inodeResourceKey(resourceID, isDir))
}

// dropIdentityLocked removes the identity index entry for an object that no
// longer owns this inode, so a stale byID mapping cannot rejoin the inode
// from the old resource id at another path.
func (m *InodeToPath) dropIdentityLocked(entry *InodeEntry) {
	if entry.ResourceID != "" {
		if m.byID[entry.ResourceID] == entry.Ino {
			delete(m.byID, entry.ResourceID)
		}
		entry.ResourceID = ""
	}
}

// SetIdentityWithKind is SetIdentity with the object kind of the incoming
// observation. A kind change or a known-and-different resource id means a
// different drive9 object now owns this inode: the old identity index entry
// and the previous object's explicit time overrides are dropped so the
// replacement's metadata is adopted.
func (m *InodeToPath) SetIdentityWithKind(ino uint64, resourceID string, nlink uint32, isDir bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.byInode[ino]
	if !ok {
		return
	}
	if entry.IsDir != isDir || identityReplacedLocked(entry, inodeResourceKey(resourceID, isDir)) {
		m.dropIdentityLocked(entry)
		clearTimeOverridesLocked(entry)
	}
	entry.IsDir = isDir
	m.setIdentityLocked(entry, resourceID)
	if nlink > 0 {
		entry.Nlink = nlink
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
	// A kind change means a different object owns this path now. setIdentityLocked
	// below cannot see it (its key is "" for a directory), so drop the old
	// JuiceFS routing here rather than leave a file inode's routing on a
	// directory.
	if entry.IsDir != isDir {
		entry.ExtentIno = 0
		// The new object must not inherit the previous object's explicitly
		// SetAttr'd times, and the old identity index entry must go too:
		// the replacement (a directory here) has an empty identity key, so
		// setIdentityLocked would early-return and leave the stale byID
		// mapping able to rejoin this inode from the old resource id.
		m.dropIdentityLocked(entry)
		clearTimeOverridesLocked(entry)
	}
	entry.IsDir = isDir
	// Listing/create-cache often still has size=0 after JuiceFS writes.
	// Shrinking an extent inode here makes ReadDirPlus publish i_size=0 so
	// the kernel returns EOF without FUSE READ. Truncate uses UpdateSize.
	if entry.ExtentIno == 0 || isDir || size >= entry.Size {
		entry.Size = size
	}
	// Clear the previous object's time overrides BEFORE the mtime line
	// below: an identity replacement (different drive9 file at this path)
	// adopts the incoming listing metadata instead.
	if identityReplacedLocked(entry, inodeResourceKey(resourceID, isDir)) {
		clearTimeOverridesLocked(entry)
	}
	// A listing reseed is a derived refresh: keep an explicitly SetAttr'd
	// time (still pending or diverged from the server view) instead of the
	// listing mtime, which for a just-committed file is the commit time.
	if entry.MtimeOverride == nil {
		entry.Mtime = mtime
	}
	m.setIdentityLocked(entry, resourceID)
	if nlink > 0 {
		entry.Nlink = nlink
	} else if entry.Nlink == 0 && !isDir {
		entry.Nlink = 1
	}
}

// identityReplacedLocked reports whether both the entry's current identity
// and the incoming key are known and differ — i.e. a different drive9 object
// now owns this path.
func identityReplacedLocked(entry *InodeEntry, key string) bool {
	return entry.ResourceID != "" && key != "" && entry.ResourceID != key
}

func (m *InodeToPath) setIdentityLocked(entry *InodeEntry, resourceID string) {
	key := inodeResourceKey(resourceID, entry.IsDir)
	if key == "" || entry.ResourceID == key {
		return
	}
	if entry.ResourceID != "" && m.byID[entry.ResourceID] == entry.Ino {
		delete(m.byID, entry.ResourceID)
	}
	// A *different* drive9 file now owns this path, so a JuiceFS inode recorded
	// for the previous one is stale and would route later opens to a replaced
	// (possibly drained) extent inode. Only clear when both identities are
	// known: create-shaped entries carry no resource id yet and must keep the
	// routing this mount just stamped.
	if identityReplacedLocked(entry, key) {
		entry.ExtentIno = 0
		// The new object also must not inherit the previous object's
		// explicitly SetAttr'd times: drop the local time overrides so the
		// incoming listing/stat metadata is adopted instead. (updateEntryLocked
		// clears these before applying a listing mtime; this covers identity
		// updates that arrive through SetIdentity/stat refreshes.)
		clearTimeOverridesLocked(entry)
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
