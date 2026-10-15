package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func reviewR2FS(t *testing.T, path string, interactive bool) (*Dat9FS, uint64, *casFileServer, func()) {
	t.Helper()
	server, ts := newCASFileServer(t, path, 1, []byte("base"))
	fs, ino := pr939HandleFS(t, path, "base")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	if interactive {
		fs.syncMode = SyncInteractive
	}
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 16)
	fs.commitQueue = cq
	cq.PathLock, cq.DurableWatermark = fs.lockRemoteCommitPath, fs.latestCommittedRevision
	cq.OnSuccess, cq.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
	cq.OnCleanup, cq.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
	cq.IsSuperseded = fs.commitEntrySuperseded
	t.Cleanup(ts.Close)
	t.Cleanup(cq.DrainAll)
	return fs, ino, server, ts.Close
}

func TestIssue1023ReviewRecoveredStaleAppendIsRejected(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/review-recovered.txt"
			fs, ino, server, _ := reviewR2FS(t, path, interactive)
			if err := fs.shadowStore.WriteFull(path, []byte("base"), 1); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.pendingIndex.PutWithBaseRevAndModeAndLineage(path, 4, PendingOverwrite, 1, 0, false, "persisted-base", "", true); err != nil {
				t.Fatal(err)
			}
			recovered, err := NewPendingIndex(fs.pendingIndex.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			recovered.setShadowStore(fs.shadowStore)
			fs.pendingIndex = recovered
			fs.commitQueue.index = recovered
			meta, _ := recovered.GetMeta(path)
			if meta.lineageTrusted {
				t.Fatal("disk recovery unexpectedly preserved process-local trust")
			}
			handles := make([]*FileHandle, 2)
			ids := make([]uint64, 2)
			for i := range ids {
				handles[i], ids[i] = pr939Handle(t, fs, ino, path, "base", false)
				handles[i].Lock()
				err := fs.loadWritableHandleFromShadowLocked(handles[i], meta)
				handles[i].Unlock()
				if err != nil {
					t.Fatal(err)
				}
				if handles[i].LineageTrusted {
					t.Fatal("restored handle unexpectedly trusted")
				}
			}
			issue1023ShadowAppend(t, fs, ino, ids[0], "A")
			issue1023ShadowAppend(t, fs, ino, ids[1], "B")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
			waitCommitQueueIdle(t, fs.commitQueue)
			_, body, _ := server.snapshot()
			if string(body) != "baseA" {
				t.Fatalf("landed=%q", body)
			}
			if fs.commitQueue.landedCommit(path).rev <= handles[1].BaseRev {
				t.Fatal("newer landed proof premise lost")
			}
			before, seq, buffer, version := pr939HandleBytes(handles[1]), handles[1].DirtySeq, handles[1].Dirty, handles[1].Dirty.contentVersion
			n, st := pr939Append(fs, ino, ids[1], "C")
			if n != 0 || st != gofuse.EAGAIN {
				t.Errorf("stale recovered append accepted: n=%d status=%v", n, st)
				if st == gofuse.OK {
					t.Logf("later fsync=%v", fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}))
					waitCommitQueueIdle(t, fs.commitQueue)
				}
			}
			if handles[1].Dirty != buffer || buffer.contentVersion != version || handles[1].DirtySeq != seq || pr939HandleBytes(handles[1]) != before {
				t.Error("rejected write changed recovered dirty image")
			}
			if _, after, _ := server.snapshot(); string(after) != "baseA" {
				t.Errorf("remote mutated=%q", after)
			}
		})
	}
}

func TestIssue1023ReviewPendingModeAncestor(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(fmt.Sprintf("interactive=%t", interactive), func(t *testing.T) {
			const path = "/review-mode.txt"
			fs, ino, server, closeOld := reviewR2FS(t, path, interactive)
			closeOld()
			var modeMu sync.Mutex
			remoteMode := uint32(0o644)
			modeCalls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
					var req struct{ Mode uint32 }
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					_, body, _ := server.snapshot()
					if string(body) != "baseABC" {
						t.Errorf("chmod applied against old content=%q", body)
					}
					modeMu.Lock()
					remoteMode = req.Mode
					modeCalls++
					modeMu.Unlock()
					w.WriteHeader(200)
					return
				}
				server.serveHTTP(w, r)
			}))
			t.Cleanup(ts.Close)
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			fs.commitQueue.client = fs.client
			fs.inodes.UpdateMode(ino, 0o644)
			_, a := pr939Handle(t, fs, ino, path, "base", false)
			older, b := pr939Handle(t, fs, ino, path, "base", false)
			issue1023ShadowAppend(t, fs, ino, a, "A")
			issue1023ShadowAppend(t, fs, ino, b, "B")
			issue1023ShadowAppend(t, fs, ino, a, "C")
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			var attr gofuse.AttrOut
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Valid: gofuse.FATTR_MODE | gofuse.FATTR_FH, Mode: 0o700}}, &attr); st != gofuse.OK {
				t.Fatal(st)
			}
			if !older.HasPendingMode || !older.Dirty.HasDirtyParts() {
				t.Fatal("dirty ancestor plus deferred mode premise lost")
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
				t.Errorf("metadata fsync=%v", st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, body, _ := server.snapshot(); string(body) != "baseABC" {
				t.Errorf("content=%q", body)
			}
			modeMu.Lock()
			gotMode, calls := remoteMode, modeCalls
			modeMu.Unlock()
			if gotMode != 0o700 || calls == 0 {
				t.Errorf("mode=%o calls=%d", gotMode, calls)
			}
			if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
				t.Errorf("drain conflicts=%d", conflicts)
			}
		})
	}
}

func TestIssue1023ReviewRetiredReservationDoesNotBlock(t *testing.T) {
	const path = "/review-retired-lock.txt"
	fs, ino, _, _ := reviewR2FS(t, path, true)
	_, a := pr939Handle(t, fs, ino, path, "base", false)
	old, b := pr939Handle(t, fs, ino, path, "base", false)
	issue1023ShadowAppend(t, fs, ino, a, "A")
	issue1023ShadowAppend(t, fs, ino, b, "B")
	issue1023ShadowAppend(t, fs, ino, a, "C")
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	waitCommitQueueIdle(t, fs.commitQueue)
	old.Lock()
	old.RemoteCommitUnlock = fs.lockRemoteCommitPath(path)
	old.Unlock()
	t.Cleanup(func() { old.Lock(); fs.releaseHandleRemoteCommitPathLocked(old); old.Unlock() })
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if unlock, ok := fs.tryLockRemoteCommitPath(path); !ok {
		t.Fatal("verified retired snapshot still blocks another same-path operation")
	} else {
		unlock()
	}
	if got := issue1023ShadowHandlerRead(t, fs, ino, b); got != "baseABC" {
		t.Fatalf("retired view=%q", got)
	}
}

// Both calls must make progress while preserving all append records. A
// callback-only retry that retains A's Flush lock stalls B's locked Fsync.
func TestIssue1023ReviewBorrowedReservationBusyHandoff(t *testing.T) {
	const path = "/review-busy-lock.txt"
	fs, ino, server, _ := reviewR2FS(t, path, false)
	fs.opts.RemoteCommitWaitTimeout = -1
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = cache
	a, aid := pr939Handle(t, fs, ino, path, "base", false)
	_, bid := pr939Handle(t, fs, ino, path, "base", false)
	issue1023ShadowAppend(t, fs, ino, aid, "A")
	issue1023ShadowAppend(t, fs, ino, bid, "B")
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: aid}); st != gofuse.OK {
		t.Fatal(st)
	}
	if a.RemoteCommitUnlock == nil {
		t.Fatal("real Flush reservation missing")
	}
	bdone := make(chan gofuse.Status, 1)
	go func() { bdone <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: bid}) }()
	stack := make([]byte, 1<<20)
	deadline := time.Now().Add(3 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		n := runtime.Stack(stack, true)
		for _, g := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(g, "(*Dat9FS).Fsync(") && strings.Contains(g, "(*Dat9FS).lockRemoteCommitPath(") {
				waiting = true
			}
		}
		if waiting {
			break
		}
		runtime.Gosched()
	}
	if !waiting {
		t.Fatal("peer did not hold its handle while waiting for the real Flush fence")
	}
	type result struct {
		n  uint32
		st gofuse.Status
	}
	adone := make(chan result, 1)
	go func() { n, st := pr939Append(fs, ino, aid, "C"); adone <- result{n, st} }()
	select {
	case r := <-adone:
		if r.n != 1 || r.st != gofuse.OK {
			t.Errorf("resumed append: n=%d status=%v", r.n, r.st)
		}
	case <-time.After(2 * time.Second):
		t.Error("retaining the borrowed Flush fence prevents append and peer fsync progress")
		a.Lock()
		fs.releaseHandleRemoteCommitPathLocked(a)
		a.Unlock()
		select {
		case <-adone:
		case <-time.After(3 * time.Second):
			t.Fatal("append did not recover after test released fence")
		}
	}
	select {
	case st := <-bdone:
		if st != gofuse.OK {
			t.Errorf("peer fsync=%v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("peer fsync did not finish")
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: aid}); st != gofuse.OK {
		t.Errorf("resumed fsync=%v", st)
	}
	waitCommitQueueIdle(t, fs.commitQueue)
	if _, data, _ := server.snapshot(); string(data) != "baseABC" {
		t.Errorf("handoff lost records: %q", data)
	}
}
