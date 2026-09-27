package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// deferredUnlinkBackend records the order of remote calls so tests can
// assert that a DELETE lands after an in-flight PUT.
type deferredUnlinkBackend struct {
	mu         sync.Mutex
	order      []string
	exists     map[string]bool
	revs       map[string]int64
	dirs       map[string]bool
	putGate    chan struct{} // when non-nil, PUTs block until closed
	deleteGate chan struct{} // when non-nil, DELETEs block before counting
	chmodFails bool          // when true, chmod POSTs answer 500
	failHeads  bool          // when true, HEADs answer 503 (identity check unavailable)
	deletes    atomic.Int64
	puts       atomic.Int64
	heads      atomic.Int64
	lists      atomic.Int64
}

func newDeferredUnlinkBackend() *deferredUnlinkBackend {
	return &deferredUnlinkBackend{exists: map[string]bool{}, revs: map[string]int64{}, dirs: map[string]bool{}}
}

// seedDir registers a directory with the given child names on the backend.
// Children get revision 7 so LIST reports a verifiable identity.
func (b *deferredUnlinkBackend) seedDir(dir string, children ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exists[dir] = true
	b.dirs[dir] = true
	b.revs[dir] = 1
	for _, c := range children {
		b.exists[dir+"/"+c] = true
		b.revs[dir+"/"+c] = 7
	}
}

// seedFile registers a file path with a HEAD revision.
func (b *deferredUnlinkBackend) seedFile(path string, rev int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.exists[path] = true
	b.revs[path] = rev
}

// hasPath reports whether the path exists on the backend (mutex-guarded).
func (b *deferredUnlinkBackend) hasPath(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exists[path]
}

func (b *deferredUnlinkBackend) record(kind string) {
	b.mu.Lock()
	b.order = append(b.order, kind)
	b.mu.Unlock()
}

func (b *deferredUnlinkBackend) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.order...)
}

func (b *deferredUnlinkBackend) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.Method == http.MethodPut:
			b.puts.Add(1)
			b.record("PUT")
			if b.putGate != nil {
				<-b.putGate
			}
			b.mu.Lock()
			b.exists[p] = true
			b.revs[p]++
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodDelete:
			// Gate before counting so a dispatched delete worker cannot
			// move the counter during a pre-drain assertion.
			if b.deleteGate != nil {
				<-b.deleteGate
			}
			b.deletes.Add(1)
			b.record("DELETE")
			recursive := r.URL.Query().Has("recursive")
			b.mu.Lock()
			if recursive {
				prefix := strings.TrimSuffix(p, "/") + "/"
				for path := range b.exists {
					if path == p || strings.HasPrefix(path, prefix) {
						delete(b.exists, path)
						delete(b.dirs, path)
						delete(b.revs, path)
					}
				}
			} else if b.dirs[p] {
				// Server semantics: a non-recursive directory DELETE fails
				// when children remain (POSIX ENOTEMPTY authority).
				prefix := strings.TrimSuffix(p, "/") + "/"
				for path := range b.exists {
					if path != p && strings.HasPrefix(path, prefix) {
						b.mu.Unlock()
						w.WriteHeader(http.StatusConflict)
						_, _ = w.Write([]byte(`{"error":"directory not empty"}`))
						return
					}
				}
				delete(b.exists, p)
				delete(b.dirs, p)
				delete(b.revs, p)
			} else if b.exists[p] {
				delete(b.exists, p)
				delete(b.dirs, p)
				delete(b.revs, p)
			}
			b.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet:
			if !r.URL.Query().Has("list") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			b.lists.Add(1)
			b.record("LIST")
			b.mu.Lock()
			entries := []map[string]any{}
			prefix := strings.TrimSuffix(p, "/") + "/"
			for path := range b.exists {
				if !strings.HasPrefix(path, prefix) {
					continue
				}
				rest := path[len(prefix):]
				if rest == "" || strings.Contains(rest, "/") {
					continue
				}
				entries = append(entries, map[string]any{"name": rest, "isDir": b.dirs[path], "revision": b.revs[path]})
			}
			b.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			body, _ := json.Marshal(map[string]any{"entries": entries})
			_, _ = w.Write(body)
		case r.Method == http.MethodPost && r.URL.Query().Has("rename"):
			src := r.Header.Get("X-Dat9-Rename-Source")
			b.mu.Lock()
			if !b.exists[src] {
				b.mu.Unlock()
				w.WriteHeader(http.StatusNotFound)
				return
			}
			b.exists[p] = true
			b.revs[p] = b.revs[src] + 1
			delete(b.exists, src)
			delete(b.revs, src)
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && r.URL.Query().Has("symlink"):
			b.mu.Lock()
			if b.exists[p] {
				b.mu.Unlock()
				w.WriteHeader(http.StatusConflict)
				return
			}
			b.exists[p] = true
			b.revs[p]++
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			if b.chmodFails {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodHead:
			b.heads.Add(1)
			b.record("HEAD")
			if b.failHeads {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			b.mu.Lock()
			exists, rev := b.exists[p], b.revs[p]
			b.mu.Unlock()
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// newDeferredUnlinkFS wires a write-back Dat9FS with the full staging stack
// (commit queue + shadow + pending index + journal). Deferred unlink is the
// default write-back behavior, so the coalescing window ships enabled.
func newDeferredUnlinkFS(t *testing.T, ts *httptest.Server, cacheDir string) *Dat9FS {
	t.Helper()
	return newDeferredUnlinkFSWithPolicy(t, ts, cacheDir, WritePolicyWriteBack)
}

func newDeferredUnlinkFSWithPolicy(t *testing.T, ts *httptest.Server, cacheDir string, policy WritePolicy) *Dat9FS {
	t.Helper()
	opts := &MountOptions{}
	opts.setDefaults()
	opts.WritePolicy = policy
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	pIdx, err := NewPendingIndex(filepath.Join(cacheDir, "pending"))
	if err != nil {
		t.Fatalf("pending index: %v", err)
	}
	if err := pIdx.RecoverFromDisk(); err != nil {
		t.Fatalf("pending index recovery: %v", err)
	}
	fs.pendingIndex = pIdx

	shadow, err := NewShadowStoreWithQuota(filepath.Join(cacheDir, "shadow"), 0.1, 0)
	if err != nil {
		t.Fatalf("shadow store: %v", err)
	}
	fs.shadowStore = shadow
	pIdx.setShadowStore(shadow)

	jr, err := NewJournal(filepath.Join(cacheDir, "journal.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	fs.journal = jr
	pIdx.SetJournal(jr)

	wb, err := NewWriteBackCache(filepath.Join(cacheDir, "pending"))
	if err != nil {
		t.Fatalf("write-back cache: %v", err)
	}
	up := NewWriteBackUploader(newTestClient(ts.URL), wb, 2)
	up.OnSuccess = fs.onWriteBackUploadSuccess
	up.SnapshotStagingGens = fs.snapshotStagingGens
	fs.SetWriteBack(wb, up)

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pIdx, jr, 2, maxCommitQueuePending)
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnUploaded = fs.onCommitQueueUploaded
	cq.OnCleanup = fs.onCommitQueueCleanup
	cq.OnDiscard = fs.onCommitQueueDiscard
	cq.IsSuperseded = fs.commitEntrySuperseded
	cq.PathLock = fs.lockRemoteCommitPath
	cq.DurableWatermark = fs.latestCommittedRevision
	cq.RecordCommittedRevision = fs.recordCommittedRevision
	// Mirror mount.go's wiring: the coalescing window ships with the queue.
	cq.ConfigureDeleteCoalesceWindow(deferredDeleteCoalesceWindow)
	fs.commitQueue = cq

	t.Cleanup(func() {
		cq.DrainAll()
		up.DrainAll()
		_ = jr.Close()
	})
	return fs
}

// TestDeferredUnlinkNoSyncRoundTrips: with the flag on, unlink of an existing
// remote file answers from local state with zero synchronous backend round
// trips; the DELETE arrives via the commit queue.
func TestDeferredUnlinkNoSyncRoundTrips(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	// Gate DELETEs before counting so the dispatched delete worker cannot
	// race the pre-drain assertion; release before DrainAll (via cleanup in
	// case the assertion fails first).
	deleteGate := make(chan struct{})
	backend.deleteGate = deleteGate
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(deleteGate) }) }
	t.Cleanup(release)

	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.exists["/victim.txt"] = true
	fs.inodes.Lookup("/victim.txt", false, 128, time.Now())

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "victim.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := backend.deletes.Load() + backend.heads.Load(); got != 0 {
		t.Fatalf("unlink issued %d synchronous backend calls, want 0", got)
	}

	release()
	fs.commitQueue.DrainAll()
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("async DELETEs = %d, want 1", got)
	}
	// The confirming journal marker must exist so the intent does not
	// re-delete the path on the next mount.
	confirmed := false
	if err := fs.journal.Replay(func(e JournalEntry) {
		if e.Op == JournalCommit && e.Path == "/victim.txt" {
			confirmed = true
		}
	}); err != nil {
		t.Fatalf("journal replay: %v", err)
	}
	if !confirmed {
		t.Fatal("no confirming journal marker after deferred delete")
	}
}

// TestDeferredUnlinkOrdersAfterQueuedUpload: an upload already in the queue
// lands before the deferred DELETE (no resurrection), while unlink(2) does
// not wait for it.
func TestDeferredUnlinkOrdersAfterQueuedUpload(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	backend.putGate = make(chan struct{})
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	// Stage an upload for the path the way Release leaves it: shadow +
	// pending-index meta removed at enqueue time, commit entry in flight.
	const path = "/staged.txt"
	if err := fs.shadowStore.Ensure(path, 4, 0); err != nil {
		t.Fatalf("shadow ensure: %v", err)
	}
	if _, err := fs.pendingIndex.PutWithBaseRev(path, 4, PendingNew, 0); err != nil {
		t.Fatalf("pending put: %v", err)
	}
	gen := fs.shadowStore.ActiveGeneration(path)
	if err := fs.commitQueue.Enqueue(&CommitEntry{
		Path:      path,
		BaseRev:   0,
		Size:      4,
		Kind:      PendingNew,
		ShadowGen: gen,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Unlink must return without waiting for the gated PUT.
	deleted0 := backend.deletes.Load()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "staged.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	select {
	case <-backend.putGate:
	case <-time.After(50 * time.Millisecond):
		// PUT still blocked: unlink did not wait for it. Good.
	}

	close(backend.putGate)
	fs.commitQueue.DrainAll()

	if backend.deletes.Load() != deleted0+1 {
		t.Fatalf("async DELETEs = %d, want %d", backend.deletes.Load(), deleted0+1)
	}
	// Final consistency: whatever landed (the staged PUT may have been
	// canceled mid-flight, or committed before cancellation), the path must
	// be gone from the backend after the deferred delete applied.
	if backend.exists["/staged.txt"] {
		t.Fatalf("path still present on backend after deferred delete (calls: %v)", backend.calls())
	}
	// Local staged state must be gone too.
	if fs.shadowStore.Has("/staged.txt") {
		t.Fatal("shadow survived unlink")
	}
	if fs.pendingIndex.HasPending("/staged.txt") {
		t.Fatal("pending meta survived unlink")
	}
}

// TestDeferredUnlinkSyncOnCloseSyncTier: the strict durability tiers keep
// the synchronous DELETE before unlink returns (policy gate, no option).
func TestDeferredUnlinkSyncOnCloseSyncTier(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFSWithPolicy(t, ts, t.TempDir(), WritePolicyCloseSync)

	backend.exists["/plain.txt"] = true
	fs.inodes.Lookup("/plain.txt", false, 128, time.Now())

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "plain.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("sync DELETEs = %d, want 1 before drain", got)
	}
}

// TestDeferredUnlinkIntentSurvivesRestart: a crash between the local unlink
// and the queued DELETE leaves a JournalUnlink intent; replay must surface
// it for re-enqueue, and a newer confirm marker (or newer local produce)
// must suppress it.
func TestDeferredUnlinkIntentSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "journal.wal")

	j1, err := NewJournal(journalPath)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	// The four replay outcomes in one WAL: an unconfirmed intent (must
	// rebuild), an intent confirmed by a newer done marker (suppressed),
	// and an intent followed by a newer local produce (the path was
	// re-staged after the unlink — suppressed).
	if err := j1.Append(JournalEntry{Op: JournalFsync, Path: "/staged-restart.txt", Length: 4, BaseRev: 0}); err != nil {
		t.Fatalf("append fsync: %v", err)
	}
	if err := j1.Append(JournalEntry{Op: JournalUnlink, Path: "/pending-delete.txt", BaseRev: 7}); err != nil {
		t.Fatalf("append unlink: %v", err)
	}
	if err := j1.Append(JournalEntry{Op: JournalUnlink, Path: "/confirmed-delete.txt", BaseRev: 3}); err != nil {
		t.Fatalf("append unlink: %v", err)
	}
	if err := j1.Append(JournalEntry{Op: JournalCommit, Path: "/confirmed-delete.txt"}); err != nil {
		t.Fatalf("append done: %v", err)
	}
	if err := j1.Append(JournalEntry{Op: JournalUnlink, Path: "/recreated.txt", BaseRev: 2}); err != nil {
		t.Fatalf("append recreated unlink: %v", err)
	}
	if err := j1.Append(JournalEntry{Op: JournalFsync, Path: "/recreated.txt", Length: 9, BaseRev: 0}); err != nil {
		t.Fatalf("append recreated fsync: %v", err)
	}
	if err := j1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	j2, err := NewJournal(journalPath)
	if err != nil {
		t.Fatalf("reopen journal: %v", err)
	}
	defer func() { _ = j2.Close() }()
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index: %v", err)
	}
	deferred, err := replayJournalIntoPending(j2, idx, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	got := map[string]int64{}
	for _, d := range deferred {
		got[d.Path] = d.BaseRev
	}
	if len(got) != 1 {
		t.Fatalf("replay returned %d deferred deletes (%v), want exactly 1", len(got), got)
	}
	if rev, ok := got["/pending-delete.txt"]; !ok || rev != 7 {
		t.Fatalf("deferred deletes = %v, want only /pending-delete.txt at base rev 7", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "journal.wal")); err != nil {
		t.Fatalf("journal file missing: %v", err)
	}
}

// TestDeleteCoalesceWindowDelaysDispatch verifies the coalescing window
// holds a deferred delete briefly before dispatch (so rmdir aggregation gets
// a chance), without needing any test gate.
func TestDeleteCoalesceWindowDelaysDispatch(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.exists["/lagged.txt"] = true
	fs.inodes.Lookup("/lagged.txt", false, 128, time.Now())

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "lagged.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := backend.deletes.Load(); got != 0 {
		t.Fatalf("DELETE dispatched within the coalesce window, want held (got %d)", got)
	}
	if !fs.commitQueue.HasPath("/lagged.txt") {
		t.Fatal("delayed delete is invisible to HasPath")
	}
	deadline := time.Now().Add(3 * time.Second)
	for backend.deletes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("delayed delete never dispatched after the window")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRmdirAggregatesDeferredChildDeletes covers the rm -rf tail: children
// unlinked with deferred deletes still coalescing, then rmdir replaces the
// whole subtree's deletes with one recursive backend DELETE.
func TestRmdirAggregatesDeferredChildDeletes(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.seedDir("/d", "a", "b")
	fs.inodes.Lookup("/d", true, 0, time.Now())
	inoA := fs.inodes.Lookup("/d/a", false, 8, time.Now())
	fs.inodes.UpdateRevision(inoA, 7)
	fs.recordCommittedRevision("/d/a", 7)
	inoB := fs.inodes.Lookup("/d/b", false, 8, time.Now())
	fs.inodes.UpdateRevision(inoB, 7)
	fs.recordCommittedRevision("/d/b", 7)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "d/a"); st != gofuse.OK {
		t.Fatalf("Unlink d/a status = %v", st)
	}
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "d/b"); st != gofuse.OK {
		t.Fatalf("Unlink d/b status = %v", st)
	}

	// rmdir must take the coalescing deletes and issue one recursive DELETE.
	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "d"); st != gofuse.OK {
		t.Fatalf("Rmdir status = %v, want OK", st)
	}

	if got := backend.lists.Load(); got != 1 {
		t.Fatalf("safety LISTs = %d, want 1", got)
	}
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1 recursive delete", got)
	}
	backend.mu.Lock()
	remaining := len(backend.exists)
	backend.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("backend still holds %d paths after the recursive delete", remaining)
	}

	// The taken intents must be confirmed so replay does not re-delete.
	confirmed := map[string]bool{}
	if err := fs.journal.Replay(func(e JournalEntry) {
		if e.Op == JournalCommit {
			confirmed[e.Path] = true
		}
	}); err != nil {
		t.Fatalf("journal replay: %v", err)
	}
	if !confirmed["/d/a"] || !confirmed["/d/b"] {
		t.Fatalf("child intents not confirmed: %v", confirmed)
	}
}

// TestRmdirAggregationFallbackOnUnknownRemoteChild verifies the safety LIST:
// a remote child this mount did not delete (another actor's create) aborts
// the aggregation, re-enqueues the taken deletes, and the standard path
// returns ENOTEMPTY instead of recursively removing someone else's file.
func TestRmdirAggregationFallbackOnUnknownRemoteChild(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.seedDir("/d2", "a", "stranger")
	fs.inodes.Lookup("/d2", true, 0, time.Now())
	fs.inodes.Lookup("/d2/a", false, 8, time.Now())

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "d2/a"); st != gofuse.OK {
		t.Fatalf("Unlink d2/a status = %v", st)
	}

	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "d2"); st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	backend.mu.Lock()
	_, dirStillThere := backend.exists["/d2"]
	_, strangerStillThere := backend.exists["/d2/stranger"]
	_, aStillThere := backend.exists["/d2/a"]
	backend.mu.Unlock()
	if !dirStillThere || !strangerStillThere {
		t.Fatal("directory or foreign child was removed despite ENOTEMPTY")
	}
	if aStillThere {
		t.Fatal("re-enqueued deferred delete for the unlinked child did not run")
	}
}

// TestDeferredDeleteSkipsRecreatedFile covers the revision guard: if the
// backend path changed revision after the unlink intent was recorded
// (another writer recreated it), the deferred delete must retire the intent
// instead of removing the newer file.
func TestDeferredDeleteSkipsRecreatedFile(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	backend.deleteGate = make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(backend.deleteGate) }) }
	t.Cleanup(release)

	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.seedFile("/victim.txt", 7)
	ino := fs.inodes.Lookup("/victim.txt", false, 128, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision("/victim.txt", 7)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "victim.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}

	// Another writer recreates the path at a newer revision while the
	// delete is queued (gated before counting so it cannot run early).
	backend.mu.Lock()
	backend.exists["/victim.txt"] = true
	backend.revs["/victim.txt"] = 9
	backend.mu.Unlock()

	release()
	fs.commitQueue.DrainAll()

	if got := backend.deletes.Load(); got != 0 {
		t.Fatalf("DELETEs = %d, want 0: the recreated file must survive", got)
	}
	if !backend.hasPath("/victim.txt") {
		t.Fatal("recreated file was removed by the deferred delete")
	}
	// The intent must be retired so a later replay does not re-delete.
	confirmed := false
	if err := fs.journal.Replay(func(e JournalEntry) {
		if e.Op == JournalCommit && e.Path == "/victim.txt" {
			confirmed = true
		}
	}); err != nil {
		t.Fatalf("journal replay: %v", err)
	}
	if !confirmed {
		t.Fatal("skipped delete did not retire the journal intent")
	}
}

// TestDeferredDeleteProceedsWhenInflightCommitAdvancesRevision covers the
// Unconditional case: a commit already in flight at unlink time is the dying
// file's own upload — the revision change it causes must not stop the delete.
func TestDeferredDeleteProceedsWhenInflightCommitAdvancesRevision(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	const path = "/dying.txt"
	if err := fs.shadowStore.Ensure(path, 4, 0); err != nil {
		t.Fatalf("shadow ensure: %v", err)
	}
	if _, err := fs.pendingIndex.PutWithBaseRev(path, 4, PendingNew, 0); err != nil {
		t.Fatalf("pending put: %v", err)
	}
	// Revisions differ from the captured one: with the guard active this
	// would (wrongly) skip the delete.
	backend.seedFile(path, 42)
	fs.inodes.Lookup(path, false, 4, time.Now())

	if err := fs.commitQueue.Enqueue(&CommitEntry{
		Path:      path,
		BaseRev:   0,
		Size:      4,
		Kind:      PendingNew,
		ShadowGen: fs.shadowStore.ActiveGeneration(path),
	}); err != nil {
		t.Fatalf("enqueue upload: %v", err)
	}

	// Unlink while the upload entry is still queued; it gets canceled by the
	// commit-point cleanup, but the dispatched-worker window is what
	// Unconditional covers — emulate by re-enqueuing the delete directly the
	// way Unlink does for an in-flight producer.
	backend.deletes.Store(0)
	if err := fs.commitQueue.Enqueue(&CommitEntry{
		Path:          path,
		BaseRev:       7,
		Kind:          PendingDelete,
		Unconditional: true,
	}); err != nil {
		t.Fatalf("enqueue delete: %v", err)
	}
	fs.commitQueue.DrainAll()

	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1: unconditional deletes must run despite revision drift", got)
	}
}

// TestDeferredDeleteAfterStaleTrackerTruncate reproduces the E2E failure
// class from #998: a synchronous mutation (path truncate) lands rev2 on the
// backend and refreshes only the inode revision, while the
// committed-revision tracker stays at rev1. The deferred delete must capture
// the inode revision (not the stale tracker) as its guard base, or the guard
// mistakes this mount's own write for a third-party recreate and skips the
// DELETE — leaving the file on the backend forever.
func TestDeferredDeleteAfterStaleTrackerTruncate(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	const path = "/trunc-victim.txt"
	backend.seedFile(path, 1)
	ino := fs.inodes.Lookup(path, false, 16, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.recordCommittedRevision(path, 1)

	// Simulate a landed synchronous truncate: backend advances to rev2 and
	// the inode refreshes, but the tracker is not updated (the pre-fix gap).
	backend.mu.Lock()
	backend.revs[path] = 2
	backend.mu.Unlock()
	fs.inodes.UpdateRevision(ino, 2)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "trunc-victim.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	fs.commitQueue.DrainAll()

	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1: the guard must not skip on its own truncate's revision advance", got)
	}
	if backend.hasPath(path) {
		t.Fatal("file still present on backend after deferred delete")
	}
}

// TestDeferredDeleteFailClosedOnStatFailure: when the identity check (HEAD)
// is unavailable, the guarded deferred DELETE must not run blind — retries
// exhaust, no DELETE is issued, and the durable journal intent stays so the
// next mount re-verifies.
func TestDeferredDeleteFailClosedOnStatFailure(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	backend.failHeads = true
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.seedFile("/guarded.txt", 7)
	ino := fs.inodes.Lookup("/guarded.txt", false, 16, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision("/guarded.txt", 7)

	start := time.Now()
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "guarded.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	fs.commitQueue.DrainAll()
	t.Logf("drain took %s", time.Since(start))

	if got := backend.deletes.Load(); got != 0 {
		t.Fatalf("DELETEs = %d, want 0: identity-unavailable deletes must not run blind", got)
	}
	if !backend.hasPath("/guarded.txt") {
		t.Fatal("file was removed despite unavailable identity check")
	}
	confirmed := false
	unlinked := false
	if err := fs.journal.Replay(func(e JournalEntry) {
		switch e.Op {
		case JournalCommit:
			if e.Path == "/guarded.txt" {
				confirmed = true
			}
		case JournalUnlink:
			if e.Path == "/guarded.txt" {
				unlinked = true
			}
		}
	}); err != nil {
		t.Fatalf("journal replay: %v", err)
	}
	if confirmed {
		t.Fatal("intent was confirmed despite failing identity check")
	}
	if !unlinked {
		t.Fatal("durable JournalUnlink intent missing")
	}
}

// TestRmdirAggregationFallbackOnRecreatedSameName: a recreated child with a
// NEWER revision at the same name must abort the aggregation (revision
// mismatch vs the taken intent); the recreated file survives, the deferred
// delete skips it, and rmdir returns ENOTEMPTY.
func TestRmdirAggregationFallbackOnRecreatedSameName(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	backend.seedDir("/d3", "a")
	fs.inodes.Lookup("/d3", true, 0, time.Now())
	ino := fs.inodes.Lookup("/d3/a", false, 8, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision("/d3/a", 7)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "d3/a"); st != gofuse.OK {
		t.Fatalf("Unlink d3/a status = %v", st)
	}
	// Another client recreates the child at rev 9 before the rmdir.
	backend.mu.Lock()
	backend.exists["/d3/a"] = true
	backend.revs["/d3/a"] = 9
	backend.mu.Unlock()

	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "d3"); st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir status = %v, want ENOTEMPTY (lists=%d deletes=%d)", st, backend.lists.Load(), backend.deletes.Load())
	}
	if !backend.hasPath("/d3/a") {
		t.Fatal("recreated child was removed by the aggregation")
	}
	backend.mu.Lock()
	_, dirThere := backend.exists["/d3"]
	backend.mu.Unlock()
	if !dirThere {
		t.Fatal("directory was removed despite the recreated child")
	}
}

// TestChmodFailureMarksPendingChmodAndKeepsUnlinkable: when the queue lands
// the data PUT but the post-upload chmod fails, the durable pending state
// must be re-classified (PendingChmod) with the committed revision recorded,
// so a follow-up unlink still issues the remote DELETE instead of
// classifying the path as never-uploaded and leaking the remote object.
func TestChmodFailureMarksPendingChmodAndKeepsUnlinkable(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	backend.chmodFails = true
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	const path = "/chmod-fail.txt"
	// Mirror the meta-bearing staging flows (mknod local-first, fsync
	// staging): pending meta + shadow + a queue entry carrying the mode.
	if err := fs.shadowStore.Ensure(path, 4, 0); err != nil {
		t.Fatalf("shadow ensure: %v", err)
	}
	if _, err := fs.shadowStore.WriteAt(path, 0, []byte("data"), 0); err != nil {
		t.Fatalf("shadow write: %v", err)
	}
	gen, err := fs.pendingIndex.PutWithBaseRevAndMode(path, 4, PendingNew, 0, 0o600, true)
	if err != nil {
		t.Fatalf("pending put: %v", err)
	}
	if err := fs.commitQueue.Enqueue(&CommitEntry{
		Path:            path,
		BaseRev:         0,
		Size:            4,
		Kind:            PendingNew,
		ShadowGen:       fs.shadowStore.ActiveGeneration(path),
		PendingIndexGen: gen,
		Mode:            0o600,
		HasMode:         true,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	fs.commitQueue.DrainAll()

	if !backend.hasPath(path) {
		t.Fatal("remote object missing after PUT (data must have landed)")
	}
	meta, ok := fs.pendingIndex.GetMeta(path)
	if !ok {
		t.Fatal("pending meta missing after chmod failure")
	}
	if meta.Kind != PendingChmod {
		t.Fatalf("pending kind = %v, want PendingChmod", meta.Kind)
	}
	if rev := fs.latestCommittedRevision(path); rev <= 0 {
		t.Fatalf("committed revision tracker = %d, want > 0", rev)
	}

	// Unlink must still remove the landed remote object.
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "chmod-fail.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	fs.commitQueue.DrainAll()
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1 (remote object must not leak)", got)
	}
	if backend.hasPath(path) {
		t.Fatal("remote object leaked after unlink")
	}
}

// TestCreateAfterDeferredUnlinkSameName covers the git-ops failure class:
// rm X immediately followed by creating a new object at X. The deferred
// remote DELETE must land before the creating op reaches the server,
// otherwise the stale remote object answers 409 and the create spuriously
// fails with EEXIST.
func TestCreateAfterDeferredUnlinkSameName(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	const path = "/name-reuse"
	backend.seedFile(path, 7)
	ino := fs.inodes.Lookup(path, false, 16, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(path, 7)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "name-reuse"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	// The name is immediately recreated as a symlink.
	if st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "target-value", "name-reuse", &gofuse.EntryOut{}); st != gofuse.OK {
		t.Fatalf("Symlink status = %v, want OK (stale remote object must be gone first)", st)
	}
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1 before the symlink creates the name", got)
	}
}
