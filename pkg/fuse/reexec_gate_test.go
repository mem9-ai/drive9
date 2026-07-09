package fuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func newTestReexecFS() *Dat9FS {
	return &Dat9FS{
		inodes:      NewInodeToPath(),
		fileHandles: NewHandleTable[*FileHandle](),
		dirHandles:  NewHandleTable[*DirHandle](),
		locks:       newFuseLockTable(),
		xattrs:      NewXAttrStore(),
	}
}

func TestReexecPreflightIdleMount(t *testing.T) {
	fs := newTestReexecFS()
	result := fs.ReexecPreflight()
	require.True(t, result.OK, "idle mount should pass preflight")
	require.Empty(t, result.Refusals)
}

func TestReexecPreflightRefusesOpenFileHandle(t *testing.T) {
	fs := newTestReexecFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/test.txt"})
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	require.Len(t, result.Refusals, 1)
	require.Equal(t, "file_handles", result.Refusals[0].Gate)
	require.Equal(t, 1, result.Refusals[0].Count)
}

func TestReexecPreflightRefusesOpenDirHandle(t *testing.T) {
	fs := newTestReexecFS()
	fs.dirHandles.Allocate(&DirHandle{Path: "/"})
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	require.Len(t, result.Refusals, 1)
	require.Equal(t, "dir_handles", result.Refusals[0].Gate)
}

func TestReexecPreflightRefusesNonRootNlookup(t *testing.T) {
	fs := newTestReexecFS()
	fs.inodes.Lookup("/foo", false, 0, time.Time{})
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "inode_nlookup" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse on non-root inode with Nlookup > 0")
}

func TestReexecPreflightRefusesFuseLock(t *testing.T) {
	fs := newTestReexecFS()
	fs.locks.set(nil, 42, 1, 100, gofuse.FileLock{
		Start: 0,
		End:   100,
		Typ:   uint32(syscall.F_WRLCK),
	}, false)
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "fuse_locks" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when FUSE lock is held")
}

func TestReexecPreflightRefusesXattr(t *testing.T) {
	fs := newTestReexecFS()
	fs.xattrs.Set("/test.txt", "user.test", []byte("value"))
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "xattrs" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when xattrs exist")
}

func TestReexecPreflightRefusesLocalOverlay(t *testing.T) {
	fs := newTestReexecFS()
	fs.localOverlay = &LocalOverlay{root: "/tmp/overlay"}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "local_overlay" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when local overlay is enabled")
}

func TestReexecPreflightRefusesGitWorkspace(t *testing.T) {
	fs := newTestReexecFS()
	fs.git = &gitWorkspaceLayer{}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "git_workspace" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when git workspace is enabled")
}

func TestReexecPreflightRefusesNonEmptyTransientOverlay(t *testing.T) {
	dir := t.TempDir()
	overlayRoot := filepath.Join(dir, "overlay")
	require.NoError(t, os.MkdirAll(overlayRoot, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(overlayRoot, "file.txt"), []byte("data"), 0o644))

	fs := newTestReexecFS()
	fs.transientLocalOverlay = &LocalOverlay{root: overlayRoot}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "transient_overlay" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when transient overlay has entries")
}

func TestReexecPreflightAllowsEmptyTransientOverlay(t *testing.T) {
	dir := t.TempDir()
	overlayRoot := filepath.Join(dir, "overlay")
	require.NoError(t, os.MkdirAll(overlayRoot, 0o755))

	fs := newTestReexecFS()
	fs.transientLocalOverlay = &LocalOverlay{root: overlayRoot}
	result := fs.ReexecPreflight()
	require.True(t, result.OK, "empty transient overlay should pass preflight")
}

func TestReexecPreflightRefusesPendingIndex(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	require.NoError(t, err)
	_, err = idx.Put("/test.txt", 100, PendingNew)
	require.NoError(t, err)

	fs := newTestReexecFS()
	fs.pendingIndex = idx
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "pending_index" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when pending index has entries")
}

func TestReexecPreflightRefusesJournal(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)
	require.NoError(t, j.Append(JournalEntry{Op: JournalWrite, Path: "/test.txt"}))

	fs := newTestReexecFS()
	fs.journal = j
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "journal" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when journal has uncommitted frames")
}

func TestReexecPreflightJournalPassesAfterCommit(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)
	require.NoError(t, j.Append(JournalEntry{Op: JournalWrite, Path: "/test.txt"}))
	require.NoError(t, j.Append(JournalEntry{Op: JournalCommit, Path: "/test.txt"}))

	fs := newTestReexecFS()
	fs.journal = j
	result := fs.ReexecPreflight()
	hasJournalRefusal := false
	for _, r := range result.Refusals {
		if r.Gate == "journal" {
			hasJournalRefusal = true
		}
	}
	require.False(t, hasJournalRefusal, "journal with committed frames should not refuse")
}

func TestReexecPreflightRefusesShadowStorePendingBytes(t *testing.T) {
	dir := t.TempDir()
	shadowDir := filepath.Join(dir, "shadow")
	ss, err := NewShadowStore(shadowDir)
	require.NoError(t, err)

	// Write a shadow file to create pending bytes.
	shadowPath := filepath.Join(shadowDir, "test.shadow")
	require.NoError(t, os.WriteFile(shadowPath, []byte("dirty data"), 0o644))
	ss.RecoverPendingBytes()
	require.Greater(t, ss.PendingBytes(), int64(0), "shadow store should have pending bytes")

	fs := newTestReexecFS()
	fs.shadowStore = ss
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "shadow_store" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when shadow store has pending bytes")
}

func TestReexecPreflightRefusesCommitQueuePendingWork(t *testing.T) {
	// CommitQueue requires a full client + shadow + pending setup.
	// Create a minimal CommitQueue with a pending entry via the public API.
	dir := t.TempDir()
	shadowDir := filepath.Join(dir, "shadow")
	ss, err := NewShadowStore(shadowDir)
	require.NoError(t, err)
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	require.NoError(t, err)
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	// Enqueue work — this will fail to commit (nil client) but the entry
	// stays pending, which is what we need for the snapshot check.
	seq, err := idx.Put("/test.txt", 10, PendingNew)
	require.NoError(t, err)
	_ = seq

	fs := newTestReexecFS()
	fs.commitQueue = cq
	fs.pendingIndex = idx
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	// Should have pending_index refusal at minimum
	hasIndex := false
	for _, r := range result.Refusals {
		if r.Gate == "pending_index" {
			hasIndex = true
		}
	}
	require.True(t, hasIndex, "should refuse when pending index has entries (commit queue scenario)")
}

func TestReexecPreflightRefusesLayerOverlayWhiteouts(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerWhiteouts = map[string]struct{}{"/deleted": {}}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "layer_overlay" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when layerWhiteouts is non-empty")
}

func TestReexecPreflightRefusesLayerOverlayFiles(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerFiles = map[string]uint32{"/new.txt": 0o644}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "layer_overlay" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when layerFiles is non-empty")
}

func TestReexecPreflightRefusesLayerOverlayDirs(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerDirs = map[string]uint32{"/newdir": 0o755}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "layer_overlay" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when layerDirs is non-empty")
}

func TestReexecPreflightRefusesLayerOverlaySymlinks(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerSymlinks = map[string]layerSymlinkState{"/link": {Target: "/target", Mode: 0o777}}
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "layer_overlay" {
			hasGate = true
		}
	}
	require.True(t, hasGate, "should refuse when layerSymlinks is non-empty")
}

func TestReexecPreflightRefusesUploaderQueued(t *testing.T) {
	ch := make(chan string, 10)
	ch <- "/queued.txt"
	u := &WriteBackUploader{
		uploadCh: ch,
		inflight: make(map[string]*pathState),
	}
	fs := newTestReexecFS()
	fs.uploader = u
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "uploader_queued" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when uploader has queued entries")
}

func TestReexecPreflightRefusesUploaderInFlight(t *testing.T) {
	u := &WriteBackUploader{
		uploadCh: make(chan string, 10),
		inflight: map[string]*pathState{"/uploading.txt": {done: make(chan struct{})}},
	}
	fs := newTestReexecFS()
	fs.uploader = u
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "uploader_inflight" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when uploader has in-flight entries")
}

func TestReexecPreflightRefusesUploaderCached(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	require.NoError(t, err)
	cache.mu.Lock()
	cache.metas["/cached.txt"] = &WriteBackMeta{Path: "/cached.txt", Size: 42}
	cache.mu.Unlock()

	u := &WriteBackUploader{
		uploadCh: make(chan string, 10),
		inflight: make(map[string]*pathState),
		cache:    cache,
	}
	fs := newTestReexecFS()
	fs.uploader = u
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "uploader_cached" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when uploader has cached entries")
}

func TestReexecPreflightRefusesCommitQueueInFlight(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	require.NoError(t, err)
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	require.NoError(t, err)
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	cq.mu.Lock()
	cq.inFlight["/inflight.txt"] = &CommitEntry{Path: "/inflight.txt", Size: 10}
	cq.mu.Unlock()

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "commit_queue_inflight" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when commit queue has in-flight entries")
}

func TestReexecPreflightRefusesCommitQueueDelayed(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	require.NoError(t, err)
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	require.NoError(t, err)
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	entry := &CommitEntry{Path: "/delayed.txt", Size: 10}
	cq.mu.Lock()
	cq.delayed[entry] = time.NewTimer(time.Hour)
	cq.mu.Unlock()

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "commit_queue_delayed" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when commit queue has delayed entries")
}

func TestReexecPreflightRefusesCommitQueueConflicts(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	require.NoError(t, err)
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	require.NoError(t, err)
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	require.NoError(t, err)

	// Add a conflict entry via the pending index.
	_, err = idx.Put("/conflict.txt", 100, PendingConflict)
	require.NoError(t, err)

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	hasGate := false
	for _, r := range result.Refusals {
		if r.Gate == "commit_queue_conflicts" {
			hasGate = true
			require.Equal(t, 1, r.Count)
		}
	}
	require.True(t, hasGate, "should refuse when commit queue has conflicts")
}

func TestReexecPreflightCollectsMultipleRefusals(t *testing.T) {
	fs := newTestReexecFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/a.txt"})
	fs.dirHandles.Allocate(&DirHandle{Path: "/"})
	fs.xattrs.Set("/b.txt", "user.test", []byte("val"))
	result := fs.ReexecPreflight()
	require.False(t, result.OK)
	require.GreaterOrEqual(t, len(result.Refusals), 3, "should collect all refusals, not stop at first")
}

func TestReexecGuardPreventsConcurrent(t *testing.T) {
	g := &reexecGuard{}
	require.True(t, g.tryAcquire(), "first acquire should succeed")
	require.True(t, g.isActive())
	require.False(t, g.tryAcquire(), "second acquire should fail")
	g.release()
	require.False(t, g.isActive())
	require.True(t, g.tryAcquire(), "acquire after release should succeed")
	g.release()
}

// --- Count helper tests ---

func TestNonRootLookupCount(t *testing.T) {
	m := NewInodeToPath()
	require.Equal(t, 0, m.NonRootLookupCount(), "empty inode table has no non-root lookups")

	m.Lookup("/foo", false, 0, time.Time{})
	require.Equal(t, 1, m.NonRootLookupCount())

	m.Lookup("/bar", false, 0, time.Time{})
	require.Equal(t, 2, m.NonRootLookupCount())
}

func TestFuseLockTableTotalCount(t *testing.T) {
	lt := newFuseLockTable()
	require.Equal(t, 0, lt.totalCount())

	lt.set(nil, 1, 100, 1, gofuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_RDLCK)}, false)
	require.Equal(t, 1, lt.totalCount())

	lt.set(nil, 2, 200, 2, gofuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)}, false)
	require.Equal(t, 2, lt.totalCount())

	lt.release(1, 100)
	require.Equal(t, 1, lt.totalCount())
}

func TestXAttrStoreTotalCount(t *testing.T) {
	s := NewXAttrStore()
	require.Equal(t, 0, s.TotalCount())

	s.Set("/a", "user.x", []byte("1"))
	require.Equal(t, 1, s.TotalCount())

	s.Set("/a", "user.y", []byte("2"))
	require.Equal(t, 2, s.TotalCount())

	s.Set("/b", "user.x", []byte("3"))
	require.Equal(t, 3, s.TotalCount())

	s.Remove("/a", "user.x")
	require.Equal(t, 2, s.TotalCount())
}

func TestLocalOverlayEntryCount(t *testing.T) {
	require.Equal(t, 0, localOverlayEntryCount(nil))

	dir := t.TempDir()
	o := &LocalOverlay{root: dir}
	require.Equal(t, 0, localOverlayEntryCount(o))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), nil, 0o644))
	require.Equal(t, 1, localOverlayEntryCount(o))

	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o755))
	require.Equal(t, 2, localOverlayEntryCount(o))
}

func TestJournalFrameCount(t *testing.T) {
	require.Equal(t, 0, journalFrameCount(nil))

	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "test.wal"))
	require.NoError(t, err)
	require.Equal(t, 0, journalFrameCount(j))

	require.NoError(t, j.Append(JournalEntry{Op: JournalWrite, Path: "/a"}))
	require.Equal(t, 1, journalFrameCount(j))

	require.NoError(t, j.Append(JournalEntry{Op: JournalWrite, Path: "/b"}))
	require.Equal(t, 2, journalFrameCount(j))

	require.NoError(t, j.Append(JournalEntry{Op: JournalCommit, Path: "/a"}))
	require.Equal(t, 1, journalFrameCount(j), "committed path should not count")
}
