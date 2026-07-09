package fuse

import (
	"os"
	"sync"
	"sync/atomic"
)

// ReexecProtocolVersion is the wire protocol version for the reexec handshake.
// Both old and new processes must advertise the same version.
const ReexecProtocolVersion = 1

// ReexecRefusal describes a single reason why reexec was refused.
type ReexecRefusal struct {
	Gate   string `json:"gate"`
	Reason string `json:"reason"`
	Count  int    `json:"count,omitempty"`
}

// ReexecPreflightResult holds the complete preflight check output.
type ReexecPreflightResult struct {
	OK       bool            `json:"ok"`
	Refusals []ReexecRefusal `json:"refusals,omitempty"`
}

// reexecGuard prevents concurrent reexec attempts.
type reexecGuard struct {
	mu     sync.Mutex
	active atomic.Bool
}

func (g *reexecGuard) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active.Load() {
		return false
	}
	g.active.Store(true)
	return true
}

func (g *reexecGuard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active.Store(false)
}

func (g *reexecGuard) isActive() bool {
	return g.active.Load()
}

// ReexecPreflight checks all V0 gates and returns structured refusal reasons.
// It does NOT acquire the reexec guard or install a quiesce barrier — those
// are the caller's responsibility during the actual handshake.
//
// This function is only valid for normal FUSE mounts. Vault and WebDAV mounts
// have separate codepaths and are excluded from V0 reexec by design.
func (fs *Dat9FS) ReexecPreflight() ReexecPreflightResult {
	var refusals []ReexecRefusal
	refuse := func(gate, reason string, count int) {
		refusals = append(refusals, ReexecRefusal{Gate: gate, Reason: reason, Count: count})
	}

	// --- Static scope checks ---

	if fs.localOverlay != nil {
		refuse("local_overlay", "user/profile local overlay is enabled", 0)
	}

	if n := fs.transientOverlayEntryCount(); n > 0 {
		refuse("transient_overlay", "transient local overlay has entries", n)
	}

	if fs.git != nil {
		refuse("git_workspace", "git workspace layer is enabled", 0)
	}

	if fs.hasLayerOverlayState() {
		refuse("layer_overlay", "FS layer overlay state is active", 0)
	}

	// --- Runtime clean-state checks ---

	if fs.fileHandles != nil {
		if n := fs.fileHandles.Len(); n > 0 {
			refuse("file_handles", "open file handles exist", n)
		}
	}

	if fs.dirHandles != nil {
		if n := fs.dirHandles.Len(); n > 0 {
			refuse("dir_handles", "open directory handles exist", n)
		}
	}

	if n := fs.nonRootKernelLookupCount(); n > 0 {
		refuse("inode_nlookup", "non-root inodes have positive kernel lookup refs", n)
	}

	if n := fs.heldLockCount(); n > 0 {
		refuse("fuse_locks", "FUSE locks are held", n)
	}

	if n := fs.xattrCount(); n > 0 {
		refuse("xattrs", "in-memory xattrs exist", n)
	}

	if fs.commitQueue != nil {
		snap := fs.commitQueue.Snapshot()
		if snap.Pending > 0 {
			refuse("commit_queue_pending", "commit queue has pending entries", snap.Pending)
		}
		if snap.InFlight > 0 {
			refuse("commit_queue_inflight", "commit queue has in-flight entries", snap.InFlight)
		}
		if snap.Delayed > 0 {
			refuse("commit_queue_delayed", "commit queue has delayed entries", snap.Delayed)
		}
		if snap.Conflicts > 0 {
			refuse("commit_queue_conflicts", "commit queue has conflicts", snap.Conflicts)
		}
	}

	if fs.uploader != nil {
		snap := fs.uploader.Snapshot()
		if snap.Queued > 0 {
			refuse("uploader_queued", "uploader has queued entries", snap.Queued)
		}
		if snap.InFlight > 0 {
			refuse("uploader_inflight", "uploader has in-flight entries", snap.InFlight)
		}
		if snap.Cached > 0 {
			refuse("uploader_cached", "uploader has cached entries", snap.Cached)
		}
	}

	if fs.pendingIndex != nil {
		if n := fs.pendingIndex.Count(); n > 0 {
			refuse("pending_index", "pending index has entries", n)
		}
	}

	if fs.shadowStore != nil {
		if n := fs.shadowStore.PendingBytes(); n > 0 {
			refuse("shadow_store", "shadow store has pending bytes", int(n))
		}
	}

	if fs.journal != nil {
		if n := journalFrameCount(fs.journal); n > 0 {
			refuse("journal", "journal has replay-required frames", n)
		}
	}

	return ReexecPreflightResult{
		OK:       len(refusals) == 0,
		Refusals: refusals,
	}
}

// --- Count helpers for types that don't have them yet ---

func (fs *Dat9FS) nonRootKernelLookupCount() int {
	if fs.inodes == nil {
		return 0
	}
	return fs.inodes.NonRootLookupCount()
}

// NonRootLookupCount returns the number of non-root inodes with positive
// kernel lookup references (Nlookup > 0).
func (m *InodeToPath) NonRootLookupCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for ino, entry := range m.byInode {
		if ino != 1 && entry.Nlookup > 0 {
			count++
		}
	}
	return count
}

func (fs *Dat9FS) heldLockCount() int {
	if fs.locks == nil {
		return 0
	}
	return fs.locks.totalCount()
}

// totalCount returns the total number of held locks across all inodes.
func (t *fuseLockTable) totalCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for _, locks := range t.locks {
		count += len(locks)
	}
	return count
}

func (fs *Dat9FS) xattrCount() int {
	if fs.xattrs == nil {
		return 0
	}
	return fs.xattrs.TotalCount()
}

// TotalCount returns the total number of xattr entries across all paths.
func (s *XAttrStore) TotalCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, attrs := range s.data {
		count += len(attrs)
	}
	return count
}

func (fs *Dat9FS) transientOverlayEntryCount() int {
	if fs.transientLocalOverlay == nil {
		return 0
	}
	return localOverlayEntryCount(fs.transientLocalOverlay)
}

// localOverlayEntryCount returns the number of filesystem entries under the
// overlay root directory. Returns 0 if the root does not exist.
func localOverlayEntryCount(o *LocalOverlay) int {
	if o == nil || o.root == "" {
		return 0
	}
	entries, err := os.ReadDir(o.root)
	if err != nil {
		return 0
	}
	return len(entries)
}

func (fs *Dat9FS) hasLayerOverlayState() bool {
	fs.layerMu.RLock()
	defer fs.layerMu.RUnlock()
	return len(fs.layerWhiteouts) > 0 || len(fs.layerFiles) > 0 ||
		len(fs.layerDirs) > 0 || len(fs.layerSymlinks) > 0
}

// journalFrameCount returns the number of uncommitted frames in the journal.
func journalFrameCount(j *Journal) int {
	if j == nil || j.path == "" {
		return 0
	}
	data, err := os.ReadFile(j.path)
	if err != nil || len(data) == 0 {
		return 0
	}
	committed := make(map[string]bool)
	var pending []string
	scanJournalFrames(data, func(entry JournalEntry, _ []byte) {
		if entry.Op == JournalCommit {
			committed[entry.Path] = true
		} else {
			pending = append(pending, entry.Path)
		}
	})
	count := 0
	for _, p := range pending {
		if !committed[p] {
			count++
		}
	}
	return count
}
