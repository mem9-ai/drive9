package fuse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// newSetattrTimeTestFS builds a Dat9FS with a commit queue wired exactly like
// production (PathLock + OnSuccess + OnCleanup) against a test server whose
// content PUT blocks until allowPut is closed. The returned channel closes
// once the first PUT starts, so tests can deterministically hold a commit
// in flight.
func newSetattrTimeTestFS(t *testing.T, path string, data []byte) (*Dat9FS, *CommitQueue, chan struct{}, chan struct{}, *atomic.Int32) {
	t.Helper()

	putStarted := make(chan struct{})
	allowPut := make(chan struct{})
	var putStartedOnce sync.Once
	var chmodCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putStartedOnce.Do(func() { close(putStarted) })
			<-allowPut
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(3)})
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "3")
			// Server mtime for a freshly committed file is the commit time,
			// which is newer than the explicitly requested (typically older)
			// utimensat time.
			w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(time.Now().Unix(), 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.RawQuery == "list=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []map[string]any{{
				"name":     strings.TrimPrefix(path, "/"),
				"size":     int64(len(data)),
				"isDir":    false,
				"mtime":    time.Now().Unix(),
				"revision": int64(3),
			}}})
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "chmod"):
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(ts.Close)

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// A write-back cache is required for GetAttr to consult pending metadata
	// (pendingIndex / writeBack.GetMeta) while a commit is in flight.
	wb, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.SetWriteBack(wb, nil)

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq
	t.Cleanup(cq.DrainAll)

	if err := shadow.WriteFull(path, data, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	return fs, cq, putStarted, allowPut, &chmodCalls
}

func enqueueSetattrTimeCommit(t *testing.T, fs *Dat9FS, cq *CommitQueue, shadow *ShadowStore, pending *PendingIndex, path string, size int64) uint64 {
	t.Helper()
	ino := fs.inodes.Lookup(path, false, size, time.Now())
	if err := cq.Enqueue(attachTestStagingGens(shadow, pending, &CommitEntry{
		Path:    path,
		Inode:   ino,
		Size:    size,
		Kind:    PendingNew,
		BaseRev: 0,
	})); err != nil {
		t.Fatal(err)
	}
	return ino
}

func waitCommitPathIdle(t *testing.T, cq *CommitQueue, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for cq.HasPath(path) {
		if time.Now().After(deadline) {
			t.Fatal("commit queue entry did not settle")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func getAttrMtime(t *testing.T, fs *Dat9FS, ino uint64) (int64, int64) {
	t.Helper()
	var out gofuse.AttrOut
	if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}
	return int64(out.Mtime), int64(out.Atime)
}

// TestSetAttrTimeOnlySkipsPendingCommitWait is the regression test for #954:
// a utimensat issued right after close() must not wait for the file's
// in-flight writeback commit, and the requested times must survive the
// commit's completion (dir-cache rewrite), remote stat refreshes, and
// directory reseedings from both the dir cache and the server listing.
func TestSetAttrTimeOnlySkipsPendingCommitWait(t *testing.T) {
	const p = "/utime.txt"
	data := []byte("payload")
	requestedMtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	requestedAtime := time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC)

	fs, cq, putStarted, allowPut, _ := newSetattrTimeTestFS(t, p, data)
	ino := enqueueSetattrTimeCommit(t, fs, cq, fs.shadowStore, fs.pendingIndex, p, int64(len(data)))

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("commit queue PUT did not start")
	}

	// The commit is in flight; a time-only SetAttr must return without
	// waiting for it (pre-fix it blocked for the full commit settle time).
	done := make(chan gofuse.Status, 1)
	var setOut gofuse.AttrOut
	go func() {
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MTIME | gofuse.FATTR_ATIME,
			Mtime:    uint64(requestedMtime.Unix()),
			Atime:    uint64(requestedAtime.Unix()),
		}}, &setOut)
	}()
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("time-only SetAttr: %v", st)
		}
	case <-time.After(700 * time.Millisecond):
		t.Fatal("time-only SetAttr blocked on the pending writeback commit")
	}
	if !cq.HasPath(p) {
		t.Fatal("commit should still be in flight when time-only SetAttr returned")
	}
	if setOut.Mtime != uint64(requestedMtime.Unix()) || setOut.Atime != uint64(requestedAtime.Unix()) {
		t.Fatalf("SetAttr reply mtime/atime = %d/%d, want %d/%d",
			setOut.Mtime, setOut.Atime, requestedMtime.Unix(), requestedAtime.Unix())
	}
	if !fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("local mtime override not armed")
	}

	// Immediate visibility: GetAttr must return the requested times even
	// while the commit is still in flight — the pending-metadata branch
	// (pendingIndex.GetMeta, whose staging mtime is newer) must not clobber
	// the just-acknowledged explicit times.
	if mtime, atime := getAttrMtime(t, fs, ino); mtime != requestedMtime.Unix() || atime != requestedAtime.Unix() {
		t.Fatalf("GetAttr while commit in flight = %d/%d, want %d/%d", mtime, atime, requestedMtime.Unix(), requestedAtime.Unix())
	}

	close(allowPut)
	waitCommitPathIdle(t, cq, p)

	// Commit completion rewrote the dir-cache entry; it must carry the
	// requested time, not the commit settle time.
	if res := fs.dirCache.Lookup("/", "utime.txt"); res.kind == namespaceLookupPositive {
		if !res.item.Mtime.Equal(requestedMtime) {
			t.Fatalf("dir cache mtime after commit settle = %v, want %v", res.item.Mtime, requestedMtime)
		}
	} else {
		t.Fatal("dir cache entry missing after commit settle")
	}

	// GetAttr goes through a remote stat whose mtime (commit time) is newer
	// than the requested time; the override must win.
	if mtime, atime := getAttrMtime(t, fs, ino); mtime != requestedMtime.Unix() || atime != requestedAtime.Unix() {
		t.Fatalf("GetAttr after settle = %d/%d, want %d/%d", mtime, atime, requestedMtime.Unix(), requestedAtime.Unix())
	}

	// Cached readdir reseeds the inode from the dir cache; the override must
	// survive it.
	if _, err := fs.listDir(context.Background(), "/"); err != nil {
		t.Fatalf("cached listDir: %v", err)
	}
	if mtime, _ := getAttrMtime(t, fs, ino); mtime != requestedMtime.Unix() {
		t.Fatalf("GetAttr after cached reseed = %d, want %d", mtime, requestedMtime.Unix())
	}

	// Same for a server-listing reseed (dir cache bypassed).
	fs.dirCache.Invalidate("/")
	if _, err := fs.listDir(context.Background(), "/"); err != nil {
		t.Fatalf("server listDir: %v", err)
	}
	if mtime, _ := getAttrMtime(t, fs, ino); mtime != requestedMtime.Unix() {
		t.Fatalf("GetAttr after server-listing reseed = %d, want %d", mtime, requestedMtime.Unix())
	}
}

// TestSetAttrModeStillWaitsForPendingCommit guards the other half of #954's
// fix scope: SetAttr requests that are not time-only (here chmod) keep the
// original ordering fence against the path's pending writeback commit.
func TestSetAttrModeStillWaitsForPendingCommit(t *testing.T) {
	const p = "/chmod.txt"
	data := []byte("payload")

	fs, cq, putStarted, allowPut, chmodCalls := newSetattrTimeTestFS(t, p, data)
	ino := enqueueSetattrTimeCommit(t, fs, cq, fs.shadowStore, fs.pendingIndex, p, int64(len(data)))

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("commit queue PUT did not start")
	}

	done := make(chan gofuse.Status, 1)
	var setOut gofuse.AttrOut
	go func() {
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o600,
		}}, &setOut)
	}()
	select {
	case st := <-done:
		t.Fatalf("chmod returned before the in-flight commit finished: %v", st)
	case <-time.After(300 * time.Millisecond):
	}

	close(allowPut)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("chmod SetAttr: %v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("chmod SetAttr did not finish after commit completed")
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("remote chmod calls = %d, want 1", got)
	}
}

// TestLocalTimeOverrideLifecycle covers the override state machine at the
// inode layer: derived refreshes never clobber it, listing reseeds preserve
// it, user mutations clear it, and a recreated path does not inherit it.
func TestLocalTimeOverrideLifecycle(t *testing.T) {
	inodes := NewInodeToPath()

	ino := inodes.Lookup("/a.txt", false, 5, time.Unix(1000, 0))
	requested := time.Unix(1234567890, 0)
	inodes.SetLocalMtime(ino, requested)
	inodes.SetLocalAtime(ino, requested.Add(time.Second))

	if !inodes.HasMtimeOverride(ino) {
		t.Fatal("override not armed after SetLocalMtime")
	}

	// Derived updates lose to the override (mtime has derived writers: commit
	// settles and stat refreshes; atime has none in the mainline path, so the
	// armed value simply persists).
	inodes.UpdateMtimeDerived(ino, time.Unix(9999999999, 0))
	entry, ok := inodes.GetEntry(ino)
	if !ok || !entry.Mtime.Equal(requested) || !entry.Atime.Equal(requested.Add(time.Second)) {
		t.Fatalf("derived update clobbered the override: mtime=%v atime=%v", entry.Mtime, entry.Atime)
	}

	// Listing reseeds (EnsureInodeWithIdentity/Lookup → updateEntryLocked)
	// preserve the override even when the listing time is newer.
	inodes.Lookup("/a.txt", false, 5, time.Unix(9999999999, 0))
	inodes.LookupWithIdentity("/a.txt", "res-1", 1, false, 5, time.Unix(9999999999, 0))
	entry, _ = inodes.GetEntry(ino)
	if !entry.Mtime.Equal(requested) {
		t.Fatalf("listing reseed clobbered the override: %v", entry.Mtime)
	}

	// A user mutation (write/truncate semantics) clears the override and
	// afterwards derived updates apply again.
	writeTime := time.Unix(2000000000, 0)
	inodes.UpdateMtime(ino, writeTime)
	if inodes.HasMtimeOverride(ino) {
		t.Fatal("override survived a user mutation")
	}
	inodes.UpdateMtimeDerived(ino, time.Unix(9999999999, 0))
	entry, _ = inodes.GetEntry(ino)
	if !entry.Mtime.Equal(time.Unix(9999999999, 0)) {
		t.Fatalf("derived update did not apply after override cleared: %v", entry.Mtime)
	}

	// Delete + recreate gets a fresh inode that does not inherit the
	// override (drive9 never reuses inode numbers within a mount).
	inodes.SetLocalMtime(ino, requested)
	inodes.Remove("/a.txt")
	ino2 := inodes.Lookup("/a.txt", false, 5, time.Unix(1000, 0))
	if ino2 == ino {
		t.Fatal("recreated path reused the old inode number")
	}
	if inodes.HasMtimeOverride(ino2) {
		t.Fatal("recreated path inherited the time override")
	}
	if _, ok := inodes.MtimeOverride(ino); ok {
		// The old inode's entry is gone; the lookup must not report it.
		t.Fatal("stale inode still reports an override")
	}

	// A remote replacement of the same path (both identities known and
	// different) must not inherit the previous object's overrides: the
	// listing metadata of the new object is adopted instead. Binding the
	// first identity is create-shaped (empty old id) and must keep the
	// override; replacing it with a different id clears both overrides.
	inodes.LookupWithIdentity("/a.txt", "res-1", 1, false, 5, time.Unix(2000, 0))
	inodes.SetLocalMtime(ino2, requested)
	inodes.SetLocalAtime(ino2, requested.Add(time.Minute))
	if !inodes.HasMtimeOverride(ino2) || !inodes.HasAtimeOverride(ino2) {
		t.Fatal("overrides not armed before identity replacement")
	}
	replacementMtime := time.Unix(8888888888, 0)
	inodes.LookupWithIdentity("/a.txt", "res-2", 1, false, 5, replacementMtime)
	if inodes.HasMtimeOverride(ino2) {
		t.Fatal("identity replacement inherited the mtime override")
	}
	if inodes.HasAtimeOverride(ino2) {
		t.Fatal("identity replacement inherited the atime override")
	}
	entry, _ = inodes.GetEntry(ino2)
	if !entry.Mtime.Equal(replacementMtime) {
		t.Fatalf("replacement mtime = %v, want listing mtime %v", entry.Mtime, replacementMtime)
	}
	inodes.UpdateAtime(ino2, time.Unix(1, 0))
	entry, _ = inodes.GetEntry(ino2)
	if !entry.Atime.Equal(time.Unix(1, 0)) {
		t.Fatal("atime override survived identity replacement")
	}

	// A kind change at the same path (file replaced by a directory) is also a
	// different object: overrides are dropped and the listing time adopted.
	inodes.SetLocalMtime(ino2, requested)
	inodes.Lookup("/a.txt", true, 0, time.Unix(7777777777, 0))
	if inodes.HasMtimeOverride(ino2) {
		t.Fatal("kind change inherited the mtime override")
	}
	entry, _ = inodes.GetEntry(ino2)
	if !entry.Mtime.Equal(time.Unix(7777777777, 0)) {
		t.Fatalf("kind-change mtime = %v, want listing mtime", entry.Mtime)
	}
}

// TestLocalMutationClearsTimeOverride guards the utimensat-then-write
// lifecycle: a local data mutation (Write/O_TRUNC/truncate all funnel through
// markDirtySize) must drop the armed override so the commit settle and later
// stat refreshes can advance the mtime again — `touch -d 2020 f; echo x >> f`
// must not leave the mtime frozen at 2020.
func TestLocalMutationClearsTimeOverride(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	requested := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	ino := fs.inodes.Lookup("/mut.txt", false, 3, time.Unix(1000, 0))
	fs.inodes.SetLocalMtime(ino, requested)
	if !fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("override not armed")
	}

	fs.markDirtySize(ino, 9)
	if fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("write mutation did not clear the time override")
	}

	// Restore registrations (writable re-open loading staged writeback/shadow
	// content, write-sync failure restore) re-register dirty state without a
	// data mutation and must keep the override armed.
	fs.inodes.SetLocalMtime(ino, requested)
	fs.markDirtySizeRestore(ino, 12)
	if !fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("restore registration cleared the time override")
	}
	if mtime, _ := fs.inodes.MtimeOverride(ino); !mtime.Equal(requested) {
		t.Fatalf("override mtime after restore registration = %v, want %v", mtime, requested)
	}

	// The central dir-cache settle stamp keeps an armed override visible.
	fs.cacheFileForPath("/mut.txt", 12, time.Now(), 4)
	if res := fs.dirCache.Lookup("/", "mut.txt"); res.kind == namespaceLookupPositive {
		if !res.item.Mtime.Equal(requested) {
			t.Fatalf("dir cache settle stamp = %v, want the override time %v", res.item.Mtime, requested)
		}
	} else {
		t.Fatal("dir cache entry missing after settle stamp")
	}

	// A genuine write after the re-arm clears the override again, and the
	// settle can advance the mtime.
	fs.markDirtySize(ino, 12)
	if fs.inodes.HasMtimeOverride(ino) {
		t.Fatal("write mutation after restore did not clear the time override")
	}

	commitTime := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	fs.inodes.UpdateMtimeDerived(ino, commitTime)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || !entry.Mtime.Equal(commitTime) {
		t.Fatalf("mtime after write settle = %v, want %v", entry.Mtime, commitTime)
	}
}

// TestAppendLogCompletionKeepsLocalTimeOverride guards the append-log async
// commit completion: it must not replace an explicitly requested mtime with
// the commit time nor clear the armed override while the time-only SetAttr
// that set it no longer fences behind the completion.
func TestAppendLogCompletionKeepsLocalTimeOverride(t *testing.T) {
	requested := time.Date(2019, 5, 4, 3, 2, 1, 0, time.UTC)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()

	fs.inodes.SetLocalMtime(fh.Ino, requested)
	if !fs.inodes.HasMtimeOverride(fh.Ino) {
		t.Fatal("override not armed")
	}

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed OK", result)
	}

	if !fs.inodes.HasMtimeOverride(fh.Ino) {
		t.Fatal("append-log completion cleared the local time override")
	}
	if mtime, _ := fs.inodes.MtimeOverride(fh.Ino); !mtime.Equal(requested) {
		t.Fatalf("override mtime = %v, want %v", mtime, requested)
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || !entry.Mtime.Equal(requested) {
		t.Fatalf("inode mtime after append-log completion = %v, want %v", entry.Mtime, requested)
	}
}
