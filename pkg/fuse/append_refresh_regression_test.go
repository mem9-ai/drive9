package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestAppendSnapshotsFsyncInAnyOrder(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, compat := range []bool{false, true} {
			for _, order := range [][]int{{0, 1}, {1, 0}, {0, 1, 2}, {2, 1, 0}, {1, 0, 2}, {1, 2, 0}} {
				t.Run(fmt.Sprintf("interactive=%t/compat=%t/%v", interactive, compat, order), func(t *testing.T) {
					const path = "/append.txt"
					server, ts := newCASFileServer(t, path, 1, []byte("base"))
					defer ts.Close()
					fs, ino := pr939HandleFS(t, path, "base")
					fs.opts.GVisorCompat = compat
					if interactive {
						fs.syncMode = SyncInteractive
					}
					fs.client = newTestClient(ts.URL)
					fs.client.SetSmallFileThresholdForTests(1 << 20)
					cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
					defer cq.DrainAll()
					fs.commitQueue = cq
					cq.PathLock = fs.lockRemoteCommitPath
					cq.DurableWatermark = fs.latestCommittedRevision
					cq.OnSuccess = fs.onCommitQueueSuccess
					cq.OnUploaded = fs.onCommitQueueUploaded
					cq.OnCleanup = fs.onCommitQueueCleanup
					cq.IsSuperseded = fs.commitEntrySuperseded
					cq.serializeMutationInodes = compat
					cq.OnDiscard = fs.onCommitQueueDiscard
					ids := make([]uint64, len(order))
					for i := range ids {
						_, ids[i] = pr939Handle(t, fs, ino, path, "base", false)
					}
					want := "base"
					for i, id := range ids {
						data := string(rune('A' + i))
						want += data
						if _, st := pr939Append(fs, ino, id, data); st != gofuse.OK {
							t.Fatalf("write %d: %v", i, st)
						}
					}
					for _, i := range order {
						in := &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[i]}
						if st := fs.Fsync(nil, in); st != gofuse.OK {
							t.Fatalf("fsync %d: %v", i, st)
						}
						waitCommitQueueIdle(t, cq)
						if st := fs.Fsync(nil, in); st != gofuse.OK {
							t.Fatalf("repeated fsync %d: %v", i, st)
						}
					}
					cq.DrainAll()
					if rev, body, _ := server.snapshot(); string(body) != want {
						t.Fatalf("remote rev=%d bytes=%q want=%q", rev, body, want)
					}
					if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
						t.Fatalf("conflicts=%d", conflicts)
					}
				})
			}
		}
	}
}

func TestWritableRehomePreservesProcessLocalAncestors(t *testing.T) {
	const path = "/rehome-ancestors"
	fs, ino := pr939HandleFS(t, path, "base")
	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = writeBack
	source, _ := pr939Handle(t, fs, ino, path, "baseA", true)
	source.ContentSnapshotID = "snapshot-parent"
	source.contentAncestors = []string{"snapshot-grandparent", "snapshot-root"}
	source.LineageTrusted = true

	source.Lock()
	if err := fs.stageShadowLocked(source, false); err != nil {
		source.Unlock()
		t.Fatal(err)
	}
	if err := fs.snapshotWriteBackLocked(source); err != nil {
		source.Unlock()
		t.Fatal(err)
	}
	want := append([]string(nil), source.stagedAncestors...)
	source.Unlock()

	pendingMeta, ok := fs.pendingIndex.GetMeta(path)
	if !ok || !slices.Equal(pendingMeta.liveAncestors, want) {
		t.Fatalf("pending ancestors=%v ok=%t, want %v", pendingMeta.liveAncestors, ok, want)
	}
	writeBackMeta, ok := fs.writeBack.GetMeta(path)
	if !ok || !slices.Equal(writeBackMeta.liveAncestors, want) {
		t.Fatalf("write-back ancestors=%v ok=%t, want %v", writeBackMeta.liveAncestors, ok, want)
	}

	shadowTarget, _ := pr939Handle(t, fs, ino, path, "base", false)
	shadowTarget.Lock()
	if err := fs.loadWritableHandleFromShadowLocked(shadowTarget, pendingMeta); err != nil {
		shadowTarget.Unlock()
		t.Fatal(err)
	}
	shadowAncestors := append([]string(nil), shadowTarget.contentAncestors...)
	shadowTarget.Unlock()
	if !slices.Equal(shadowAncestors, want) {
		t.Fatalf("shadow re-home ancestors=%v, want %v", shadowAncestors, want)
	}

	writeBackTarget, _ := pr939Handle(t, fs, ino, path, "base", false)
	writeBackTarget.Lock()
	loaded := fs.loadWritableHandleFromWriteBackLocked(writeBackTarget)
	writeBackAncestors := append([]string(nil), writeBackTarget.contentAncestors...)
	writeBackTarget.Unlock()
	if !loaded || !slices.Equal(writeBackAncestors, want) {
		t.Fatalf("write-back re-home loaded=%t ancestors=%v, want %v", loaded, writeBackAncestors, want)
	}
}

func TestAppendSnapshotLatchClearsAfterSynchronousCommit(t *testing.T) {
	const path = "/append-latch.txt"
	_, ts := newCASFileServer(t, path, 1, []byte("base"))
	defer ts.Close()
	fs, ino := pr939HandleFS(t, path, "base")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	defer cq.DrainAll()
	fs.commitQueue = cq
	cq.PathLock = fs.lockRemoteCommitPath
	cq.DurableWatermark = fs.latestCommittedRevision
	cq.OnUploaded, cq.OnSuccess = fs.onCommitQueueUploaded, fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup

	source, sourceID := pr939Handle(t, fs, ino, path, "base", false)
	_, targetID := pr939Handle(t, fs, ino, path, "base", false)
	if _, st := pr939Append(fs, ino, sourceID, "A"); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := pr939Append(fs, ino, targetID, "B"); st != gofuse.OK {
		t.Fatal(st)
	}
	if !source.appendSnapshot {
		t.Fatal("source append snapshot latch was not armed")
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID}); st != gofuse.OK {
		t.Fatal(st)
	}
	if source.appendSnapshot {
		t.Fatal("synchronous source snapshot commit retained append latch")
	}
}

// These tests exercise FUSE request handlers directly on the Linux validation
// host. They do not claim to exercise a mounted kernel filesystem.
func pr939HandleFS(t *testing.T, path, base string) (*Dat9FS, uint64) {
	t.Helper()
	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
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
	ino := fs.inodes.Lookup(path, false, int64(len(base)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	return fs, ino
}

func pr939Handle(t *testing.T, fs *Dat9FS, ino uint64, path, data string, dirty bool) (*FileHandle, uint64) {
	t.Helper()
	fh := &FileHandle{
		Ino: ino, Path: path,
		Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize: int64(len(data)), BaseRev: 1, LineageTrusted: true,
		Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND),
	}
	if _, err := fh.Dirty.Write(0, []byte(data)); err != nil {
		t.Fatal(err)
	}
	if dirty {
		fh.DirtySeq = fs.markDirtySize(ino, int64(len(data)))
	} else {
		fh.Dirty.ClearDirty()
	}
	id := fs.allocateFileHandle(fh)
	return fh, id
}

func pr939Append(fs *Dat9FS, ino, id uint64, data string) (uint32, gofuse.Status) {
	return fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino}, Fh: id,
	}, []byte(data))
}

func pr939HandleBytes(fh *FileHandle) string {
	fh.Lock()
	defer fh.Unlock()
	return string(fh.Dirty.Bytes())
}

func TestPR939ReviewNormalSequentialAppend(t *testing.T) {
	const path = "/_cacache/index-v5/review-normal"
	fs, ino := pr939HandleFS(t, path, "base")
	_, a := pr939Handle(t, fs, ino, path, "base", false)
	fhB, b := pr939Handle(t, fs, ino, path, "base", false)
	if n, st := pr939Append(fs, ino, a, "A"); st != gofuse.OK || n != 1 {
		t.Fatalf("first append: n=%d status=%v", n, st)
	}
	if n, st := pr939Append(fs, ino, b, "B"); st != gofuse.OK || n != 1 {
		t.Fatalf("second append: n=%d status=%v", n, st)
	}
	if got := pr939HandleBytes(fhB); got != "baseAB" {
		t.Fatalf("second handle=%q, want baseAB", got)
	}
}

func TestPR939ReviewShorterSiblingRefresh(t *testing.T) {
	const path = "/_cacache/index-v5/review-shrink"
	fs, ino := pr939HandleFS(t, path, "ABCDEF")
	fh, id := pr939Handle(t, fs, ino, path, "ABCDEF", false)
	pr939Handle(t, fs, ino, path, "A", true)
	if n, st := pr939Append(fs, ino, id, "X"); st != gofuse.OK || n != 1 {
		t.Fatalf("append: n=%d status=%v", n, st)
	}
	if got := pr939HandleBytes(fh); got != "AX" {
		t.Fatalf("after adopting shorter sibling: got=%q want=%q; stale tail/EOF survived refresh", got, "AX")
	}
}

func TestPR939ReviewBusySiblingMustNotAppendAtStaleEOF(t *testing.T) {
	const path = "/_cacache/index-v5/review-busy"
	fs, ino := pr939HandleFS(t, path, "base")
	src, srcID := pr939Handle(t, fs, ino, path, "base", false)
	target, targetID := pr939Handle(t, fs, ino, path, "base", false)
	if n, st := pr939Append(fs, ino, srcID, "A"); st != gofuse.OK || n != 1 {
		t.Fatalf("completed prior append: n=%d status=%v", n, st)
	}
	// Model another operation owning the source handle's mutex. Its previous
	// successful append is still authoritative even while the handle is busy.
	src.Lock()
	var release sync.Once
	unlock := func() { release.Do(src.Unlock) }
	t.Cleanup(unlock)
	type result struct {
		n  uint32
		st gofuse.Status
	}
	done := make(chan result, 1)
	go func() {
		n, st := pr939Append(fs, ino, targetID, "B")
		done <- result{n, st}
	}()
	var got result
	select {
	case got = <-done:
		unlock()
	case <-time.After(300 * time.Millisecond):
		// An implementation may wait for an authoritative source. Release it
		// and require bounded completion rather than forcing a specific fix.
		unlock()
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("append did not complete after releasing busy sibling")
		}
	}
	if got.st == gofuse.Status(syscall.EAGAIN) || got.st == gofuse.Status(syscall.EBUSY) {
		if got.n != 0 {
			t.Fatalf("retry response acknowledged bytes: n=%d status=%v", got.n, got.st)
		}
		got.n, got.st = pr939Append(fs, ino, targetID, "B")
	}
	if got.st != gofuse.OK || got.n != 1 {
		t.Fatalf("append after source available: n=%d status=%v", got.n, got.st)
	}
	if data := pr939HandleBytes(target); data != "baseAB" {
		t.Fatalf("successful append ignored completed busy-sibling write: got=%q want=%q", data, "baseAB")
	}
}

func TestAppendRefreshBusyTimeoutReturnsEAGAIN(t *testing.T) {
	const path = "/_cacache/index-v5/review-busy-timeout"
	fs, ino := pr939HandleFS(t, path, "base")
	fs.opts.RemoteCommitWaitTimeout = 10 * time.Millisecond
	src, srcID := pr939Handle(t, fs, ino, path, "base", false)
	_, targetID := pr939Handle(t, fs, ino, path, "base", false)
	if _, st := pr939Append(fs, ino, srcID, "A"); st != gofuse.OK {
		t.Fatal(st)
	}

	src.Lock()
	start := time.Now()
	n, st := pr939Append(fs, ino, targetID, "B")
	src.Unlock()
	if st != gofuse.Status(syscall.EAGAIN) || n != 0 {
		t.Fatalf("busy timeout: n=%d status=%v, want zero-byte EAGAIN", n, st)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("busy timeout took %s, want bounded retry", elapsed)
	}
}

func TestAppendRefreshLineageMismatchFailsFast(t *testing.T) {
	const path = "/_cacache/index-v5/review-lineage-mismatch"
	fs, ino := pr939HandleFS(t, path, "base")
	fs.opts.RemoteCommitWaitTimeout = 2 * time.Second
	target, targetID := pr939Handle(t, fs, ino, path, "baseA", true)
	pr939Handle(t, fs, ino, path, "baseB", true)

	before := pr939HandleBytes(target)
	start := time.Now()
	n, st := pr939Append(fs, ino, targetID, "X")
	if st != gofuse.Status(syscall.EAGAIN) || n != 0 {
		t.Fatalf("lineage mismatch: n=%d status=%v, want zero-byte EAGAIN", n, st)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("lineage mismatch retried for %s instead of failing fast", elapsed)
	}
	if after := pr939HandleBytes(target); after != before {
		t.Fatalf("lineage mismatch changed target from %q to %q", before, after)
	}
}

func TestPR939ReviewUnlinkedAppendKeepsAnonymousInode(t *testing.T) {
	const path = "/_cacache/index-v5/review-unlinked"
	fs, oldIno := pr939HandleFS(t, path, "OLD")
	old, oldID := pr939Handle(t, fs, oldIno, path, "OLD", false)
	// Use the production unlink transition and inode detachment. Bytes are
	// fully resident, so the remote-snapshot leg is unnecessary in this test.
	marked, anyOpen, err := fs.markOpenHandlesUnlinked(context.Background(), path, false)
	if err != nil || !anyOpen || len(marked) != 1 || !old.Unlinked {
		t.Fatalf("unlink transition: marked=%d any=%v unlinked=%v err=%v", len(marked), anyOpen, old.Unlinked, err)
	}
	if got := len(fs.openHandles.SnapshotPath(path)); got != 0 {
		t.Fatalf("old handle remained path-indexed: count=%d", got)
	}
	fs.inodes.RemoveLinkPreserve(path)
	newIno := fs.inodes.Lookup(path, false, 3, time.Now())
	if newIno == oldIno {
		t.Fatal("replacement unexpectedly reused anonymous inode")
	}
	replacement, _ := pr939Handle(t, fs, newIno, path, "NEW", true)
	if n, st := pr939Append(fs, oldIno, oldID, "X"); st != gofuse.OK || n != 1 {
		t.Fatalf("anonymous append: n=%d status=%v", n, st)
	}
	if got := pr939HandleBytes(old); got != "OLDX" {
		t.Errorf("anonymous inode adopted replacement bytes: got=%q want=%q", got, "OLDX")
	}
	if got := pr939HandleBytes(replacement); got != "NEW" {
		t.Errorf("replacement changed through anonymous fd: got=%q want=%q", got, "NEW")
	}
}

func TestAppendRefreshYieldsReservedPathLock(t *testing.T) {
	const path = "/reserved-append"
	fs, ino := pr939HandleFS(t, path, "base")
	fs.opts.RemoteCommitWaitTimeout = time.Second
	src, srcID := pr939Handle(t, fs, ino, path, "base", false)
	target, targetID := pr939Handle(t, fs, ino, path, "base", false)
	if _, st := pr939Append(fs, ino, srcID, "A"); st != gofuse.OK {
		t.Fatal(st)
	}
	target.RemoteCommitUnlock = fs.lockRemoteCommitPath(path)
	t.Cleanup(func() {
		target.Lock()
		fs.releaseHandleRemoteCommitPathLocked(target)
		target.Unlock()
	})
	src.Lock()
	done := make(chan struct{})
	go func() {
		unlock := fs.lockRemoteCommitPath(path)
		src.Unlock()
		unlock()
		close(done)
	}()
	if n, st := pr939Append(fs, ino, targetID, "B"); st != gofuse.OK || n != 1 {
		t.Fatalf("append did not yield its existing reservation: n=%d status=%v", n, st)
	}
	<-done
	if got := pr939HandleBytes(target); got != "baseAB" {
		t.Fatalf("bytes=%q want=baseAB", got)
	}
}

func TestAppendFsyncPendingModeWithGVisorCallbacks(t *testing.T) {
	const path = "/private.txt"
	server := &casFileServer{t: t, path: path, revision: 0}
	var reject atomic.Bool
	var chmods atomic.Int32
	reject.Store(true)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			chmods.Add(1)
			if reject.Load() {
				http.Error(w, "denied", http.StatusForbidden)
				return
			}
			var req struct{ Mode uint32 }
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mode != 0600 {
				t.Errorf("chmod mode=%o err=%v", req.Mode, err)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		server.serveHTTP(w, r)
	}))
	defer ts.Close()
	fs, ino := pr939HandleFS(t, path, "")
	fs.opts.GVisorCompat = true
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	defer cq.DrainAll()
	fs.commitQueue = cq
	cq.OnUploaded, cq.OnSuccess = fs.onCommitQueueUploaded, fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	cq.PathLock, cq.DurableWatermark = fs.lockRemoteCommitPath, fs.latestCommittedRevision
	fh, id := pr939Handle(t, fs, ino, path, "", false)
	fh.IsNew, fh.BaseRev = true, 0
	fs.setPendingModeLocked(fh, 0600, 0)
	if _, st := pr939Append(fs, ino, id, "private"); st != gofuse.OK {
		t.Fatal(st)
	}
	in := &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}
	if st := fs.Fsync(nil, in); st != gofuse.EACCES || !fh.HasPendingMode {
		t.Fatalf("rejected chmod: status=%v pending=%v", st, fh.HasPendingMode)
	}
	reject.Store(false)
	if st := fs.Fsync(nil, in); st != gofuse.OK || fh.HasPendingMode {
		t.Fatalf("mode retry: status=%v pending=%v", st, fh.HasPendingMode)
	}
	if _, body, puts := server.snapshot(); string(body) != "private" || len(puts) != 1 || chmods.Load() != 2 {
		t.Fatalf("body=%q puts=%d chmods=%d", body, len(puts), chmods.Load())
	}
}
