package fuse

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestWriteBufferCommittedPrefixLifecycle(t *testing.T) {
	wb := NewWriteBuffer("/proof", 100, 4)
	_, _ = wb.Write(0, []byte("old-data"))
	wb.ClearDirty()
	wb.markCommittedPrefix(1, 8)
	v := wb.contentVersion
	_, _ = wb.Write(8, []byte("!"))
	if wb.prefixRevision != 1 || wb.prefixEnd != 8 || wb.contentVersion == v {
		t.Fatal("append lost proof or version")
	}
	snapshot := wb.snapshot()
	_, _ = wb.Write(0, []byte("NEW"))
	wb.ClearDirty()
	if wb.prefixRevision != 0 {
		t.Fatal("cleaning resurrected prefix proof")
	}
	v = wb.contentVersion
	wb.restore(snapshot)
	if wb.contentVersion == v || wb.prefixRevision != 0 {
		t.Fatal("rollback restored proof or old version")
	}
	wb.markCommittedPrefix(2, 9)
	if wb.prefixRevision != 0 {
		t.Fatal("dirty bytes acquired committed proof")
	}
	wb.ClearDirty()
	wb.markCommittedPrefix(2, 9)
	if wb.prefixRevision != 2 || wb.prefixEnd != 9 {
		t.Fatal("clean committed baseline missing")
	}
	version := wb.contentVersion
	wb.ClearDirty()
	if wb.contentVersion != version {
		t.Fatal("ClearDirty reset mutation version")
	}
	_ = wb.Truncate(9)
	if wb.prefixRevision != 0 {
		t.Fatal("truncate retained proof")
	}
}

func TestIssue986UnstagedAdoption(t *testing.T) {
	const p = "/unstaged"
	fs, ino := pr939HandleFS(t, p, "original")
	source, sid := pr939Handle(t, fs, ino, p, "CHANGED!", true)
	defer fs.deleteFileHandle(sid, source)
	target, tid := pr939Handle(t, fs, ino, p, "original", false)
	defer fs.deleteFileHandle(tid, target)
	target.Dirty.markCommittedPrefix(1, 8)
	target.Lock()
	loaded := fs.loadWritableHandleFromOpenHandleLocked(target)
	target.Unlock()
	if !loaded || string(target.Dirty.Bytes()) != "CHANGED!" {
		t.Fatal("adoption did not occur")
	}
	if target.ContentSnapshotID != "" || len(target.contentAncestors) != 0 {
		t.Fatal("expected the untagged adoption counterexample")
	}
	if target.Dirty.prefixRevision != 0 {
		t.Fatal("adopted prefix falsely remains proved")
	}
	t.Log("inherited changed prefix with empty snapshot ID; buffer proof invalidated")
}

func TestIssue986PrefixRead(t *testing.T) {
	const p = "/prefix-plan"
	fs, ino := pr939HandleFS(t, p, "old-data")
	w := &FileHandle{Ino: ino, Path: p, BaseRev: 1, OrigSize: 8, Flags: uint32(syscall.O_APPEND | syscall.O_WRONLY), LineageTrusted: true, Dirty: fs.newWriteBuffer(p, maxPreloadSize, 4)}
	w.Dirty.totalSize, w.Dirty.remoteSize = 8, 8
	w.Dirty.markCommittedPrefix(1, 8)
	loads := 0
	w.Dirty.LoadPart = func(int) ([]byte, error) { loads++; return nil, syscall.EIO }
	wid := fs.allocateFileHandle(w)
	defer fs.deleteFileHandle(wid, w)
	if n, st := pr939Append(fs, ino, wid, "!"); n != 1 || st != gofuse.OK {
		t.Fatalf("append %d %v", n, st)
	}
	fs.readCache.Put(p, []byte("old-data"), 1)
	r := &FileHandle{Ino: ino, Path: p, BaseRev: 1, OrigSize: 8, WritePolicy: WritePolicyWriteBack}
	rid := fs.allocateFileHandle(r)
	defer fs.deleteFileHandle(rid, r)
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 8)
	if err != nil || st != gofuse.OK || string(data) != "old-data" || loads != 0 {
		t.Fatalf("prefix %q %v %v loads=%d", data, st, err, loads)
	}
	data, st, err = readDat9FSTestRange(fs, ino, rid, 8, 1)
	if err != nil || st != gofuse.OK || string(data) != "!" {
		t.Fatalf("tail %q %v %v", data, st, err)
	}
	rw, rwid := pr939Handle(t, fs, ino, p, "old-data", false)
	rw.Flags = uint32(syscall.O_RDWR)
	rw.WritePolicy = WritePolicyWriteBack
	rw.Dirty.markCommittedPrefix(1, 8)
	defer fs.deleteFileHandle(rwid, rw)
	for _, tc := range []struct {
		offset int64
		size   int
		want   string
	}{{0, 8, "old-data"}, {8, 1, "!"}} {
		got, status, readErr := readDat9FSTestRange(fs, ino, rwid, tc.offset, tc.size)
		if readErr != nil || status != gofuse.OK || string(got) != tc.want {
			t.Fatalf("readwrite offset=%d got=%q status=%v error=%v", tc.offset, got, status, readErr)
		}
	}
	if loads != 0 {
		t.Fatalf("prefix or tail unexpectedly loaded old parts %d times", loads)
	}
	fs.remoteReadTimeout = time.Millisecond
	_, st, _ = readDat9FSTestRange(fs, ino, rid, 0, 9)
	if st != gofuse.EIO {
		t.Fatalf("mixed request unexpectedly bypassed: %v", st)
	}
	w.Lock()
	w.Flags |= syscall.O_TRUNC
	w.Unlock()
	if fs.canReadAppendPrefix(p, ino, r, 0, 8) {
		t.Fatal("O_TRUNC allowed")
	}
	t.Log("proved prefix succeeds with zero loads; tail visible; mixed and truncate do not bypass")
}

func TestIssue986OrderedHandoff(t *testing.T) {
	const p = "/handoff-plan"
	_, ts := newCASFileServer(t, p, 1, []byte("hello"))
	defer ts.Close()
	fs, ino := pr939HandleFS(t, p, "hello")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	w, wid := pr939Handle(t, fs, ino, p, "hello", false)
	defer fs.deleteFileHandle(wid, w)
	w.Dirty.markCommittedPrefix(1, 5)
	if _, st := pr939Append(fs, ino, wid, "!"); st != gofuse.OK {
		t.Fatal(st)
	}
	cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue = cq
	release := make(chan struct{})
	cq.PathLock = func(string) func() { <-release; return func() {} }
	defer cq.DrainAll()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	cq.OnSuccess = func(e *CommitEntry, rev int64) {
		if !fs.shadowStore.Has(p) {
			t.Error("shadow removed before success publication")
		}
		fs.onCommitQueueSuccess(e, rev)
	}
	cq.OnCleanup = fs.onCommitQueueCleanup
	before := fs.dirtySeq
	testHookAfterAppendPrefixScan = func(path string) {
		if path != p {
			return
		}
		testHookAfterAppendPrefixScan = nil
		w.Lock()
		defer w.Unlock()
		if err := fs.stageShadowLocked(w, false); err != nil {
			t.Fatal(err)
		}
		if err := fs.enqueueStagedShadowCommitLocked(w); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { testHookAfterAppendPrefixScan = nil }()
	if fs.canReadAppendPrefix(p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 5) {
		t.Fatal("handoff incorrectly allowed bypass")
	}
	if w.DirtySeq != 0 || fs.dirtySeq != before || !fs.hasPendingLocalState(p) {
		t.Fatal("handoff state not reached")
	}
	r := &FileHandle{Ino: ino, Path: p, BaseRev: 1, OrigSize: 5}
	rid := fs.allocateFileHandle(r)
	defer fs.deleteFileHandle(rid, r)
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 16)
	if err != nil || st != gofuse.OK || string(data) != "hello!" {
		t.Fatalf("staged fallback %q %v %v", data, st, err)
	}
	t.Log("handoff did not advance dirtySeq; ordered pending check rejected bypass; normal read found staged bytes")
	unblock()
	cq.DrainAll()
	if fs.shadowStore.Has(p) || fs.hasQueuedCommit(p) {
		t.Fatal("commit cleanup did not finish")
	}
	data, st, err = readDat9FSTestRange(fs, ino, rid, 0, 16)
	if err != nil || st != gofuse.OK || string(data) != "hello!" {
		t.Fatalf("committed successor %q %v %v", data, st, err)
	}
	t.Log("after real commit cleanup, normal read found committed hello!")
}

func TestIssue986WriteFsyncABA(t *testing.T) {
	const p = "/aba-plan"
	server, ts := newCASFileServer(t, p, 1, []byte("hello"))
	defer ts.Close()
	fs, ino := pr939HandleFS(t, p, "hello")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.syncMode = SyncStrict
	r, rid := pr939Handle(t, fs, ino, p, "hello", false)
	r.Flags = uint32(syscall.O_RDWR)
	r.WritePolicy = WritePolicyWriteBack
	defer fs.deleteFileHandle(rid, r)
	w, wid := pr939Handle(t, fs, ino, p, "hello", false)
	defer fs.deleteFileHandle(wid, w)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	testHookAfterAppendRead = func(path string) {
		if path != p {
			return
		}
		testHookAfterAppendRead = nil
		if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid}, []byte("HELLO")); st != gofuse.OK {
			t.Fatal(st)
		}
		if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: rid}); st != gofuse.OK {
			t.Fatal(st)
		}
		if r.DirtySeq != 0 || r.Dirty.HasDirtyParts() {
			t.Fatal("ABA did not return clean")
		}
	}
	defer func() { testHookAfterAppendRead = nil }()
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
	_, remote, _ := server.snapshot()
	if err != nil || st != gofuse.OK || string(data) != "HELLO" || string(remote) != "HELLO" {
		t.Fatalf("ABA read=%q remote=%q %v %v", data, remote, st, err)
	}
	t.Log("real Write+Fsync returned to clean; buffer pointer/version fence selected HELLO")
}

func TestIssue986ReadWriteHandoffSelection(t *testing.T) {
	for _, writable := range []bool{false, true} {
		name := "readonly"
		if writable {
			name = "readwrite"
		}
		t.Run(name, func(t *testing.T) {
			const path = "/append-reader-handoff"
			fs, ino := pr939HandleFS(t, path, "hello")
			reader := &FileHandle{Ino: ino, Path: path, BaseRev: 1, OrigSize: 5, WritePolicy: WritePolicyWriteBack}
			if writable {
				reader.Flags = uint32(syscall.O_RDWR)
				reader.Dirty = fs.newWriteBuffer(path, maxPreloadSize, 0)
				if _, err := reader.Dirty.Write(0, []byte("hello")); err != nil {
					t.Fatal(err)
				}
				reader.Dirty.ClearDirty()
				reader.ShadowReady = true
			}
			readerID := fs.allocateFileHandle(reader)
			t.Cleanup(func() { fs.deleteFileHandle(readerID, reader) })
			if err := fs.shadowStore.WriteFull(path, []byte("hello"), 1); err != nil {
				t.Fatal(err)
			}
			reader.ShadowGen = fs.shadowStore.Pin(path)
			reader.ShadowPinned = true
			t.Cleanup(func() { fs.shadowStore.Unpin(reader.ShadowGen) })
			writer, writerID := pr939Handle(t, fs, ino, path, "hello", false)
			t.Cleanup(func() { fs.deleteFileHandle(writerID, writer) })
			if _, st := pr939Append(fs, ino, writerID, " gopher"); st != gofuse.OK {
				t.Fatal(st)
			}
			t.Cleanup(func() { testHookAfterSamePathDirtyHandleScan = nil })
			testHookAfterSamePathDirtyHandleScan = func(p string) {
				if p != path {
					return
				}
				testHookAfterSamePathDirtyHandleScan = nil
				writer.Lock()
				defer writer.Unlock()
				if err := fs.stageShadowLocked(writer, false); err != nil {
					t.Fatal(err)
				}
				// Post-publication handle cleanup; the queue handoff is exercised separately.
				writer.Dirty.ClearDirty()
				fs.clearDirtySize(ino, writer.DirtySeq)
				writer.DirtySeq = 0
			}
			data, st, err := readDat9FSTestRange(fs, ino, readerID, 0, 64)
			if err != nil || st != gofuse.OK || string(data) != "hello gopher" {
				t.Fatalf("handoff read=%q status=%v error=%v", data, st, err)
			}
		})
	}
}

func TestIssue986WaitStatus(t *testing.T) {
	const p = "/wait-plan"
	fs, ino := pr939HandleFS(t, p, "old-data")
	w := &FileHandle{Ino: ino, Path: p, BaseRev: 1, OrigSize: 8, Flags: uint32(syscall.O_APPEND), Dirty: fs.newWriteBuffer(p, maxPreloadSize, 4)}
	w.Dirty.totalSize = 9
	w.Dirty.remoteSize = 8
	w.DirtySeq = fs.markDirtySize(ino, 9)
	loads := 0
	w.Dirty.LoadPart = func(int) ([]byte, error) { loads++; return nil, syscall.EIO }
	id := fs.allocateFileHandle(w)
	defer fs.deleteFileHandle(id, w)
	fs.remoteReadTimeout = 10 * time.Millisecond
	start := time.Now()
	_, _, _, st, _, _ := fs.readActiveAppendVisibleRange(context.Background(), p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 8)
	elapsed := time.Since(start)
	if st != gofuse.EIO || elapsed >= 100*time.Millisecond {
		t.Fatalf("deadline %v %s", st, elapsed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, st, _, _ = fs.readActiveAppendVisibleRange(ctx, p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 8)
	if st != gofuse.EINTR {
		t.Fatalf("cancel %v", st)
	}
	ctx, done := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer done()
	_, _, _, st, _, _ = fs.readActiveAppendVisibleRange(ctx, p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 8)
	if st != gofuse.EIO {
		t.Fatalf("context deadline %v", st)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	w.Dirty.LoadPart = func(int) ([]byte, error) { cancel(); return nil, syscall.EIO }
	_, _, _, st, _, _ = fs.readActiveAppendVisibleRange(ctx, p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 8)
	if st != gofuse.EINTR {
		t.Fatalf("cancellation during part attempt: %v", st)
	}
	t.Logf("10ms timeout completed in %s with %d immediate failing loads; cancel=EINTR deadline=EIO", elapsed, loads)
}

func TestIssue986PrefixRejectsUncertainSources(t *testing.T) {
	const p = "/negative-plan"
	fs, ino := pr939HandleFS(t, p, "hello")
	w, wid := pr939Handle(t, fs, ino, p, "hello", false)
	defer fs.deleteFileHandle(wid, w)
	w.Dirty.markCommittedPrefix(1, 5)
	if _, st := pr939Append(fs, ino, wid, "!"); st != gofuse.OK {
		t.Fatal(st)
	}
	if !fs.canReadAppendPrefix(p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 5) {
		t.Fatal("positive setup")
	}
	w.Lock()
	bypass := fs.canReadAppendPrefix(p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 5)
	w.Unlock()
	if bypass {
		t.Fatal("busy source bypassed")
	}
	other, oid := pr939Handle(t, fs, ino, p, "OTHER", true)
	other.Flags = uint32(syscall.O_RDWR)
	if fs.canReadAppendPrefix(p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 5) {
		t.Fatal("non-append writer ignored")
	}
	fs.deleteFileHandle(oid, other)
	testHookAfterAppendPrefixScan = func(path string) {
		if path != p {
			return
		}
		testHookAfterAppendPrefixScan = nil
		w.Lock()
		defer w.Unlock()
		if _, err := w.Dirty.Write(0, []byte("X")); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { testHookAfterAppendPrefixScan = nil }()
	if fs.canReadAppendPrefix(p, ino, &FileHandle{Ino: ino, Path: p, BaseRev: 1}, 0, 5) {
		t.Fatal("source mutation after scan ignored")
	}
	t.Log("busy source, competing prefix writer, and post-scan source mutation all reject bypass")
}

// Uses the real Fsync -> flushHandle Path 2 -> conditional HTTP PUT path.
// The channel barrier fixes the interval before the handle mutex is reacquired.
func TestIssue986CommitBaselineGeneration(t *testing.T) {
	for _, tc := range []struct {
		name, mutation string
		wantProof      int64
	}{
		{"same-payload", "none", 2},
		{"new-successful-write", "write", 0},
		{"partial-failed-write", "partial", 0},
		{"replaced-buffer", "replace", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const p = "/commit-proof-plan"
			server, ts := newCASFileServer(t, p, 1, []byte("before!!"))
			defer ts.Close()
			server.firstPutStarted = make(chan struct{})
			server.releaseFirstPut = make(chan struct{})
			fs, ino := pr939HandleFS(t, p, "before!!")
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			fs.syncMode = SyncStrict
			h, id := pr939Handle(t, fs, ino, p, "ORIGINAL", true)
			defer fs.deleteFileHandle(id, h)
			h.Flags = uint32(syscall.O_RDWR)
			h.WritePolicy = WritePolicyWriteBack
			h.Dirty = fs.newWriteBuffer(p, maxPreloadSize, 4)
			if _, err := h.Dirty.Write(0, []byte("ORIGINAL")); err != nil {
				t.Fatal(err)
			}
			capturedBuffer, capturedVersion, capturedSeq := h.Dirty, h.Dirty.contentVersion, h.DirtySeq
			beforeRelock := make(chan struct{})
			resumeRelock := make(chan struct{})
			var httpOnce, relockOnce sync.Once
			unblockHTTP := func() { httpOnce.Do(func() { close(server.releaseFirstPut) }) }
			unblockRelock := func() { relockOnce.Do(func() { close(resumeRelock) }) }
			testHookBeforeFlushRelock = func(path string) {
				if path == p {
					close(beforeRelock)
					<-resumeRelock
				}
			}
			result := make(chan gofuse.Status, 1)
			complete := make(chan struct{})
			go func() {
				defer close(complete)
				result <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}()
			defer func() {
				unblockHTTP()
				unblockRelock()
				select {
				case <-complete:
				case <-time.After(5 * time.Second):
					t.Error("flush cleanup timed out")
				}
				testHookBeforeFlushRelock = nil
			}()
			select {
			case <-server.firstPutStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("PUT did not reach server barrier")
			}
			rev, body, _ := server.snapshot()
			if rev != 1 || string(body) != "before!!" {
				t.Fatal("server barrier did not pause commit")
			}
			unblockHTTP()
			select {
			case <-beforeRelock:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not reach response/relock barrier")
			}
			rev, body, puts := server.snapshot()
			if rev != 2 || string(body) != "ORIGINAL" || len(puts) != 1 {
				t.Fatalf("captured payload %d %q puts=%d", rev, body, len(puts))
			}
			switch tc.mutation {
			case "write":
				n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}, []byte("LATEST!!"))
				if n != 8 || st != gofuse.OK {
					t.Fatalf("concurrent Write %d %v", n, st)
				}
				if h.DirtySeq == capturedSeq {
					t.Fatal("successful write did not advance dirty sequence")
				}
			case "partial":
				h.Lock()
				h.Dirty.EvictPart(1)
				h.Dirty.RestorePart = func(int) ([]byte, error) { return nil, syscall.EIO }
				h.Unlock()
				n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}, []byte("CHANGED!"))
				if n != 0 || st != gofuse.EIO {
					t.Fatalf("expected actual failed partial Write, got %d %v", n, st)
				}
				if h.Dirty != capturedBuffer || h.DirtySeq != capturedSeq || h.Dirty.contentVersion == capturedVersion || string(h.Dirty.PartData(1)) != "CHAN" {
					t.Fatal("partial mutation/version-only rejection precondition missing")
				}
			case "replace":
				if err := fs.shadowStore.WriteFull(p, []byte("REPLACED"), 1); err != nil {
					t.Fatal(err)
				}
				if _, err := fs.pendingIndex.PutWithBaseRev(p, 8, PendingOverwrite, 1); err != nil {
					t.Fatal(err)
				}
				meta, ok := fs.pendingIndex.GetMeta(p)
				if !ok {
					t.Fatal("pending metadata missing")
				}
				h.Lock()
				err := fs.loadWritableHandleFromShadowLocked(h, meta)
				h.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				if h.Dirty == capturedBuffer || h.DirtySeq != capturedSeq || h.Dirty.contentVersion != capturedVersion {
					t.Fatal("buffer-identity-only rejection precondition missing")
				}
			}
			unblockRelock()
			select {
			case st := <-result:
				if st != gofuse.OK {
					t.Fatal(st)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not finish")
			}
			<-complete
			h.Lock()
			proof, limit := h.Dirty.prefixRevision, h.Dirty.prefixEnd
			dirty, seq := h.Dirty.HasDirtyParts(), h.DirtySeq
			currentHead := string(h.Dirty.PartData(1))
			h.Unlock()
			if proof != tc.wantProof {
				t.Fatalf("proof revision=%d want=%d", proof, tc.wantProof)
			}
			if proof > 0 && limit != 8 {
				t.Fatalf("proof bound=%d want=8", limit)
			}
			rev, body, puts = server.snapshot()
			if rev != 2 || string(body) != "ORIGINAL" || len(puts) != 1 {
				t.Fatal("concurrent local state changed captured upload")
			}
			if tc.mutation == "write" && (!dirty || seq == capturedSeq || currentHead != "LATE") {
				t.Fatal("new dirty write lost")
			}
			if tc.mutation == "replace" && currentHead != "REPLACED" {
				t.Fatal("replacement content lost")
			}
			if tc.mutation == "partial" && currentHead != "CHAN" {
				t.Fatal("partial content mutation lost")
			}
			t.Logf("remote=(rev=%d, %q) local_head=%q proof=(rev=%d,end=%d) clean=%t", rev, body, currentHead, proof, limit, !dirty && seq == 0)
		})
	}
}

func TestIssue986PrefixEstablishedByOpen(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "remote"
		if cached {
			name = "read-cache"
		}
		t.Run(name, func(t *testing.T) {
			const path = "/prefix-open"
			body := strings.Repeat("x", 8)
			server, ts := newCASFileServer(t, path, 1, []byte(body))
			defer ts.Close()
			fs, ino := pr939HandleFS(t, path, body)
			fs.client = newTestClient(ts.URL)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			if cached {
				fs.readCache.Put(path, []byte(body), 1)
			}
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			h, ok := fs.fileHandles.Get(out.Fh)
			if !ok {
				t.Fatal("opened handle missing")
			}
			t.Cleanup(func() { fs.deleteFileHandle(out.Fh, h) })
			if h.Dirty.prefixRevision != 1 || h.Dirty.prefixEnd != 8 {
				t.Fatalf("open proof=(%d,%d)", h.Dirty.prefixRevision, h.Dirty.prefixEnd)
			}
			if n, st := pr939Append(fs, ino, out.Fh, "!"); n != 1 || st != gofuse.OK {
				t.Fatalf("append %d %v", n, st)
			}
			if h.Dirty.prefixRevision != 1 || h.Dirty.prefixEnd != 8 {
				t.Fatal("append changed original prefix bound")
			}
			_, gets := server.proofRequestCounts()
			if cached && gets != 0 {
				t.Fatalf("cached preload unexpectedly fetched %d times", gets)
			}
		})
	}
}

func TestIssue986RebindNeedsExactCommittedSize(t *testing.T) {
	const path = "/prefix-rebind"
	fs, ino := pr939HandleFS(t, path, "hello")
	h, id := pr939Handle(t, fs, ino, path, "hello", false)
	defer fs.deleteFileHandle(id, h)
	h.Lock()
	defer h.Unlock()
	h.BaseRev = 2
	fs.recordCommittedRevision(path, 2)
	fs.rebindCleanWriteBufferToRemoteLocked(h, 9)
	if h.Dirty.prefixRevision != 0 {
		t.Fatal("locally enlarged size certified as committed")
	}
	fs.recordCommittedRevisionWithSize(path, 2, 7)
	fs.rebindCleanWriteBufferToRemoteLocked(h, 9)
	if h.Dirty.prefixRevision != 0 {
		t.Fatal("mismatched committed size certified")
	}
	fs.rebindCleanWriteBufferToRemoteLocked(h, 7)
	if h.Dirty.prefixRevision != 2 || h.Dirty.prefixEnd != 7 {
		t.Fatal("exact committed rebind lost proof")
	}
}

func TestIssue986UnlinkDuringAppendRead(t *testing.T) {
	const path = "/append-unlink-read"
	fs, ino := pr939HandleFS(t, path, "hello")
	reader, rid := pr939Handle(t, fs, ino, path, "hello", false)
	reader.Flags = uint32(syscall.O_RDWR)
	reader.WritePolicy = WritePolicyWriteBack
	defer fs.deleteFileHandle(rid, reader)
	writer, wid := pr939Handle(t, fs, ino, path, "hello", false)
	defer fs.deleteFileHandle(wid, writer)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	t.Cleanup(func() { testHookAfterAppendRead = nil })
	testHookAfterAppendRead = func(p string) {
		if p != path {
			return
		}
		testHookAfterAppendRead = nil
		reader.Lock()
		defer reader.Unlock()
		reader.Unlinked = true
		reader.UnlinkedSnapshot = true
		reader.UnlinkedData = []byte("private")
	}
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
	if err != nil || st != gofuse.OK || string(data) != "private" {
		t.Fatalf("unlinked read=%q status=%v error=%v", data, st, err)
	}
}

func TestIssue986CommittedHandoffSkipsOldPrefetch(t *testing.T) {
	const path = "/append-prefetch-handoff"
	_, ts := newCASFileServer(t, path, 1, []byte("hello"))
	defer ts.Close()
	fs, ino := pr939HandleFS(t, path, "hello")
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.syncMode = SyncStrict
	reader := &FileHandle{Ino: ino, Path: path, BaseRev: 1, OrigSize: 5, WritePolicy: WritePolicyWriteBack}
	reader.Prefetch = NewPrefetcher(fs.client, path, 5)
	defer reader.Prefetch.Close()
	rid := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(rid, reader)
	writer, wid := pr939Handle(t, fs, ino, path, "hello", false)
	defer fs.deleteFileHandle(wid, writer)
	if _, st := pr939Append(fs, ino, wid, " gopher"); st != gofuse.OK {
		t.Fatal(st)
	}
	t.Cleanup(func() { testHookAfterSamePathDirtyHandleScan = nil })
	testHookAfterSamePathDirtyHandleScan = func(p string) {
		if p != path {
			return
		}
		testHookAfterSamePathDirtyHandleScan = nil
		if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: wid}); st != gofuse.OK {
			t.Fatal(st)
		}
		// A reader-owned stale prefetch image must not override the published successor.
		ready := make(chan struct{})
		close(ready)
		reader.Prefetch.mu.Lock()
		reader.Prefetch.cache[0] = &prefetchBlock{offset: 0, data: []byte("hello"), ready: ready}
		reader.Prefetch.mu.Unlock()
	}
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 64)
	if err != nil || st != gofuse.OK || string(data) != "hello gopher" {
		t.Fatalf("committed handoff=%q status=%v error=%v", data, st, err)
	}
}

func TestIssue986PrefixSkipsUnprovedPinnedSnapshot(t *testing.T) {
	const path = "/append-prefix-pinned"
	fs, ino := pr939HandleFS(t, path, "old-data")
	reader := &FileHandle{Ino: ino, Path: path, BaseRev: 1, OrigSize: 8, WritePolicy: WritePolicyWriteBack}
	if err := fs.shadowStore.WriteFull(path, []byte("WRONG!!!"), 1); err != nil {
		t.Fatal(err)
	}
	reader.ShadowGen = fs.shadowStore.Pin(path)
	reader.ShadowPinned = true
	t.Cleanup(func() { fs.shadowStore.Unpin(reader.ShadowGen) })
	fs.shadowStore.Remove(path) // Retain the old pinned image, with no active pending source.
	reader.Prefetch = NewPrefetcher(fs.client, path, 8)
	defer reader.Prefetch.Close()
	ready := make(chan struct{})
	close(ready)
	reader.Prefetch.cache[0] = &prefetchBlock{offset: 0, data: []byte("WRONG!!!"), ready: ready}
	rid := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(rid, reader)
	writer, wid := pr939Handle(t, fs, ino, path, "old-data", false)
	defer fs.deleteFileHandle(wid, writer)
	writer.Dirty.markCommittedPrefix(1, 8)
	if _, st := pr939Append(fs, ino, wid, "!"); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.readCache.Put(path, []byte("old-data"), 1)
	if !fs.canReadAppendPrefix(path, ino, reader, 0, 8) {
		t.Fatal("prefix fixture is not eligible")
	}
	data, st, err := readDat9FSTestRange(fs, ino, rid, 0, 8)
	if err != nil || st != gofuse.OK || string(data) != "old-data" {
		t.Fatalf("proved prefix=%q status=%v error=%v", data, st, err)
	}
}
