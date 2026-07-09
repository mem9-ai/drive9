package fuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
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
	if !result.OK {
		t.Fatalf("idle mount should pass preflight, got refusals: %+v", result.Refusals)
	}
	if len(result.Refusals) != 0 {
		t.Fatalf("expected no refusals, got %d", len(result.Refusals))
	}
}

func TestReexecPreflightRefusesOpenFileHandle(t *testing.T) {
	fs := newTestReexecFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/test.txt"})
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with open file handle")
	}
	if len(result.Refusals) != 1 {
		t.Fatalf("expected 1 refusal, got %d", len(result.Refusals))
	}
	if result.Refusals[0].Gate != "file_handles" {
		t.Fatalf("expected gate file_handles, got %s", result.Refusals[0].Gate)
	}
	if result.Refusals[0].Count != 1 {
		t.Fatalf("expected count 1, got %d", result.Refusals[0].Count)
	}
}

func TestReexecPreflightRefusesOpenDirHandle(t *testing.T) {
	fs := newTestReexecFS()
	fs.dirHandles.Allocate(&DirHandle{Path: "/"})
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with open dir handle")
	}
	if len(result.Refusals) != 1 {
		t.Fatalf("expected 1 refusal, got %d", len(result.Refusals))
	}
	if result.Refusals[0].Gate != "dir_handles" {
		t.Fatalf("expected gate dir_handles, got %s", result.Refusals[0].Gate)
	}
}

func TestReexecPreflightRefusesNonRootNlookup(t *testing.T) {
	fs := newTestReexecFS()
	fs.inodes.Lookup("/foo", false, 0, time.Time{})
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with non-root nlookup")
	}
	assertHasGate(t, result.Refusals, "inode_nlookup", 1)
}

func TestReexecPreflightRefusesFuseLock(t *testing.T) {
	fs := newTestReexecFS()
	fs.locks.set(nil, 42, 1, 100, gofuse.FileLock{
		Start: 0,
		End:   100,
		Typ:   uint32(syscall.F_WRLCK),
	}, false)
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with held FUSE lock")
	}
	assertHasGate(t, result.Refusals, "fuse_locks", 1)
}

func TestReexecPreflightRefusesXattr(t *testing.T) {
	fs := newTestReexecFS()
	fs.xattrs.Set("/test.txt", "user.test", []byte("value"))
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with xattrs")
	}
	assertHasGate(t, result.Refusals, "xattrs", 1)
}

func TestReexecPreflightRefusesLocalOverlay(t *testing.T) {
	fs := newTestReexecFS()
	fs.localOverlay = &LocalOverlay{root: "/tmp/overlay"}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with local overlay enabled")
	}
	assertHasGate(t, result.Refusals, "local_overlay", -1)
}

func TestReexecPreflightRefusesGitWorkspace(t *testing.T) {
	fs := newTestReexecFS()
	fs.git = &gitWorkspaceLayer{}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with git workspace enabled")
	}
	assertHasGate(t, result.Refusals, "git_workspace", -1)
}

func TestReexecPreflightRefusesNonEmptyTransientOverlay(t *testing.T) {
	dir := t.TempDir()
	overlayRoot := filepath.Join(dir, "overlay")
	if err := os.MkdirAll(overlayRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(overlayRoot, "file.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fs := newTestReexecFS()
	fs.transientLocalOverlay = &LocalOverlay{root: overlayRoot}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with non-empty transient overlay")
	}
	assertHasGate(t, result.Refusals, "transient_overlay", 1)
}

func TestReexecPreflightAllowsEmptyTransientOverlay(t *testing.T) {
	dir := t.TempDir()
	overlayRoot := filepath.Join(dir, "overlay")
	if err := os.MkdirAll(overlayRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	fs := newTestReexecFS()
	fs.transientLocalOverlay = &LocalOverlay{root: overlayRoot}
	result := fs.ReexecPreflight()
	if !result.OK {
		t.Fatalf("empty transient overlay should pass preflight, got refusals: %+v", result.Refusals)
	}
}

func TestReexecPreflightRefusesPendingIndex(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	if _, err := idx.Put("/test.txt", 100, PendingNew); err != nil {
		t.Fatalf("Put: %v", err)
	}

	fs := newTestReexecFS()
	fs.pendingIndex = idx
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with pending index entries")
	}
	assertHasGate(t, result.Refusals, "pending_index", 1)
}

func TestReexecPreflightRefusesJournal(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/test.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	fs := newTestReexecFS()
	fs.journal = j
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with uncommitted journal frames")
	}
	assertHasGate(t, result.Refusals, "journal", -1)
}

func TestReexecPreflightJournalPassesAfterCommit(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/test.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalCommit, Path: "/test.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	fs := newTestReexecFS()
	fs.journal = j
	result := fs.ReexecPreflight()
	for _, r := range result.Refusals {
		if r.Gate == "journal" {
			t.Fatal("journal with committed frames should not refuse")
		}
	}
}

func TestReexecPreflightRefusesJournalFsyncAfterCommit(t *testing.T) {
	// Regression: Fsync → Commit → Fsync on the same path must refuse.
	// The second Fsync has a higher Seq than the Commit, so the path
	// is still replay-required.
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/same.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalCommit, Path: "/same.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/same.txt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	fs := newTestReexecFS()
	fs.journal = j
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail: Fsync after Commit on same path is replay-required")
	}
	assertHasGate(t, result.Refusals, "journal", -1)
}

func TestReexecPreflightRefusesShadowStorePendingBytes(t *testing.T) {
	dir := t.TempDir()
	shadowDir := filepath.Join(dir, "shadow")
	ss, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}

	shadowPath := filepath.Join(shadowDir, "test.shadow")
	if err := os.WriteFile(shadowPath, []byte("dirty data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	ss.RecoverPendingBytes()
	if ss.PendingBytes() <= 0 {
		t.Fatal("shadow store should have pending bytes after recovery")
	}

	fs := newTestReexecFS()
	fs.shadowStore = ss
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with shadow store pending bytes")
	}
	assertHasGate(t, result.Refusals, "shadow_store", -1)
}

func TestReexecPreflightRefusesLayerOverlayWhiteouts(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerWhiteouts = map[string]struct{}{"/deleted": {}}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with layer whiteouts")
	}
	assertHasGate(t, result.Refusals, "layer_overlay", -1)
}

func TestReexecPreflightRefusesLayerOverlayFiles(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerFiles = map[string]uint32{"/new.txt": 0o644}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with layer files")
	}
	assertHasGate(t, result.Refusals, "layer_overlay", -1)
}

func TestReexecPreflightRefusesLayerOverlayDirs(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerDirs = map[string]uint32{"/newdir": 0o755}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with layer dirs")
	}
	assertHasGate(t, result.Refusals, "layer_overlay", -1)
}

func TestReexecPreflightRefusesLayerOverlaySymlinks(t *testing.T) {
	fs := newTestReexecFS()
	fs.layerSymlinks = map[string]layerSymlinkState{"/link": {Target: "/target", Mode: 0o777}}
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with layer symlinks")
	}
	assertHasGate(t, result.Refusals, "layer_overlay", -1)
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
	if result.OK {
		t.Fatal("expected preflight to fail with uploader queued entries")
	}
	assertHasGate(t, result.Refusals, "uploader_queued", 1)
}

func TestReexecPreflightRefusesUploaderInFlight(t *testing.T) {
	u := &WriteBackUploader{
		uploadCh: make(chan string, 10),
		inflight: map[string]*pathState{"/uploading.txt": {done: make(chan struct{})}},
	}
	fs := newTestReexecFS()
	fs.uploader = u
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with uploader in-flight entries")
	}
	assertHasGate(t, result.Refusals, "uploader_inflight", 1)
}

func TestReexecPreflightRefusesUploaderCached(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatalf("NewWriteBackCache: %v", err)
	}
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
	if result.OK {
		t.Fatal("expected preflight to fail with uploader cached entries")
	}
	assertHasGate(t, result.Refusals, "uploader_cached", 1)
}

func TestReexecPreflightRefusesCommitQueuePending(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	cq.mu.Lock()
	cq.queue = append(cq.queue, &CommitEntry{Path: "/pending.txt", Size: 10})
	cq.mu.Unlock()

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with commit queue pending entries")
	}
	assertHasGate(t, result.Refusals, "commit_queue_pending", 1)
}

func TestReexecPreflightRefusesCommitQueueInFlight(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	cq.mu.Lock()
	cq.inFlight["/inflight.txt"] = &CommitEntry{Path: "/inflight.txt", Size: 10}
	cq.mu.Unlock()

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with commit queue in-flight entries")
	}
	assertHasGate(t, result.Refusals, "commit_queue_inflight", 1)
}

func TestReexecPreflightRefusesCommitQueueDelayed(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)
	entry := &CommitEntry{Path: "/delayed.txt", Size: 10}
	timer := time.NewTimer(time.Hour)
	t.Cleanup(func() { timer.Stop() })
	cq.mu.Lock()
	cq.delayed[entry] = timer
	cq.mu.Unlock()

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with commit queue delayed entries")
	}
	assertHasGate(t, result.Refusals, "commit_queue_delayed", 1)
}

func TestReexecPreflightRefusesCommitQueueConflicts(t *testing.T) {
	dir := t.TempDir()
	ss, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	j, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}

	if _, err := idx.Put("/conflict.txt", 100, PendingConflict); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cq := NewCommitQueue(nil, ss, idx, j, 1, 100)

	fs := newTestReexecFS()
	fs.commitQueue = cq
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with commit queue conflicts")
	}
	assertHasGate(t, result.Refusals, "commit_queue_conflicts", 1)
}

func TestReexecPreflightCollectsMultipleRefusals(t *testing.T) {
	fs := newTestReexecFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/a.txt"})
	fs.dirHandles.Allocate(&DirHandle{Path: "/"})
	fs.xattrs.Set("/b.txt", "user.test", []byte("val"))
	result := fs.ReexecPreflight()
	if result.OK {
		t.Fatal("expected preflight to fail with multiple dirty states")
	}
	if len(result.Refusals) < 3 {
		t.Fatalf("expected at least 3 refusals, got %d", len(result.Refusals))
	}
}

func TestReexecGuardPreventsConcurrent(t *testing.T) {
	g := &reexecGuard{}
	if !g.tryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	if !g.isActive() {
		t.Fatal("guard should be active after acquire")
	}
	if g.tryAcquire() {
		t.Fatal("second acquire should fail")
	}
	g.release()
	if g.isActive() {
		t.Fatal("guard should not be active after release")
	}
	if !g.tryAcquire() {
		t.Fatal("acquire after release should succeed")
	}
	g.release()
}

// --- Count helper tests ---

func TestNonRootLookupCount(t *testing.T) {
	m := NewInodeToPath()
	if got := m.nonRootLookupCount(); got != 0 {
		t.Fatalf("empty inode table: want 0, got %d", got)
	}

	m.Lookup("/foo", false, 0, time.Time{})
	if got := m.nonRootLookupCount(); got != 1 {
		t.Fatalf("after one lookup: want 1, got %d", got)
	}

	m.Lookup("/bar", false, 0, time.Time{})
	if got := m.nonRootLookupCount(); got != 2 {
		t.Fatalf("after two lookups: want 2, got %d", got)
	}
}

func TestFuseLockTableTotalCount(t *testing.T) {
	lt := newFuseLockTable()
	if got := lt.totalCount(); got != 0 {
		t.Fatalf("empty lock table: want 0, got %d", got)
	}

	lt.set(nil, 1, 100, 1, gofuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_RDLCK)}, false)
	if got := lt.totalCount(); got != 1 {
		t.Fatalf("after one lock: want 1, got %d", got)
	}

	lt.set(nil, 2, 200, 2, gofuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)}, false)
	if got := lt.totalCount(); got != 2 {
		t.Fatalf("after two locks: want 2, got %d", got)
	}

	lt.release(1, 100)
	if got := lt.totalCount(); got != 1 {
		t.Fatalf("after release: want 1, got %d", got)
	}
}

func TestXAttrStoreTotalCount(t *testing.T) {
	s := NewXAttrStore()
	if got := s.totalCount(); got != 0 {
		t.Fatalf("empty store: want 0, got %d", got)
	}

	s.Set("/a", "user.x", []byte("1"))
	if got := s.totalCount(); got != 1 {
		t.Fatalf("after one set: want 1, got %d", got)
	}

	s.Set("/a", "user.y", []byte("2"))
	if got := s.totalCount(); got != 2 {
		t.Fatalf("after two sets: want 2, got %d", got)
	}

	s.Set("/b", "user.x", []byte("3"))
	if got := s.totalCount(); got != 3 {
		t.Fatalf("after three sets: want 3, got %d", got)
	}

	s.Remove("/a", "user.x")
	if got := s.totalCount(); got != 2 {
		t.Fatalf("after remove: want 2, got %d", got)
	}
}

func TestLocalOverlayEntryCount(t *testing.T) {
	if got := localOverlayEntryCount(nil); got != 0 {
		t.Fatalf("nil overlay: want 0, got %d", got)
	}

	dir := t.TempDir()
	o := &LocalOverlay{root: dir}
	if got := localOverlayEntryCount(o); got != 0 {
		t.Fatalf("empty dir: want 0, got %d", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "a.txt"), nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := localOverlayEntryCount(o); got != 1 {
		t.Fatalf("one file: want 1, got %d", got)
	}

	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if got := localOverlayEntryCount(o); got != 2 {
		t.Fatalf("file + dir: want 2, got %d", got)
	}
}

func TestLocalOverlayEntryCountReturnsNegativeOnReadError(t *testing.T) {
	// A root that exists as a file (not directory) should cause ReadDir to fail.
	dir := t.TempDir()
	filePath := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	o := &LocalOverlay{root: filePath}
	if got := localOverlayEntryCount(o); got != -1 {
		t.Fatalf("read error: want -1, got %d", got)
	}
}

func TestJournalUncommittedFrameCount(t *testing.T) {
	dir := t.TempDir()
	j, err := NewJournal(filepath.Join(dir, "test.wal"))
	if err != nil {
		t.Fatalf("NewJournal: %v", err)
	}
	if got := j.UncommittedFrameCount(); got != 0 {
		t.Fatalf("empty journal: want 0, got %d", got)
	}

	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := j.UncommittedFrameCount(); got != 1 {
		t.Fatalf("one fsync: want 1, got %d", got)
	}

	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/b"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := j.UncommittedFrameCount(); got != 2 {
		t.Fatalf("two fsyncs: want 2, got %d", got)
	}

	if err := j.Append(JournalEntry{Op: JournalCommit, Path: "/a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := j.UncommittedFrameCount(); got != 1 {
		t.Fatalf("after commit of /a: want 1, got %d", got)
	}

	// Fsync after Commit on same path: must count as uncommitted again.
	if err := j.Append(JournalEntry{Op: JournalFsync, Path: "/a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := j.UncommittedFrameCount(); got != 2 {
		t.Fatalf("fsync after commit on /a: want 2, got %d", got)
	}
}

// assertHasGate checks that at least one refusal has the given gate name.
// If wantCount >= 0, it also asserts the count matches.
func assertHasGate(t *testing.T, refusals []ReexecRefusal, gate string, wantCount int) {
	t.Helper()
	for _, r := range refusals {
		if r.Gate == gate {
			if wantCount >= 0 && r.Count != wantCount {
				t.Fatalf("gate %s: want count %d, got %d", gate, wantCount, r.Count)
			}
			return
		}
	}
	t.Fatalf("gate %q not found in refusals: %+v", gate, refusals)
}
