package fuse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newFtruncateCommitFS(t *testing.T, putHooks ...func(http.ResponseWriter, *http.Request) bool) (*Dat9FS, uint64, uint64, uint64, func() string) {
	t.Helper()
	var mu sync.Mutex
	data, revision := "hello world", int64(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && len(putHooks) != 0 && putHooks[0](w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-IsDir", "false")
		case http.MethodGet:
			_, _ = io.WriteString(w, data)
		case http.MethodPut:
			if r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(revision, 10) {
				w.WriteHeader(http.StatusConflict)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			data, revision = string(body), revision+1
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			_, _ = io.WriteString(w, `{"status":"ok","revision":`+strconv.FormatInt(revision, 10)+`}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(ts.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	var err error
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack, err = NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue.PathLock = fs.lockRemoteCommitPath
	fs.commitQueue.OnUploaded = fs.onCommitQueueUploaded
	fs.commitQueue.OnSuccess = fs.onCommitQueueSuccess
	fs.commitQueue.OnCleanup = fs.onCommitQueueCleanup
	t.Cleanup(func() {
		for _, fh := range fs.openHandles.SnapshotPath("/file.bin") {
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
		}
		fs.commitQueue.DrainAll()
	})
	ino := fs.inodes.Lookup("/file.bin", false, 11, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	var a, b gofuse.OpenOut
	for _, out := range []*gofuse.OpenOut{&a, &b} {
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, out); st != gofuse.OK {
			t.Fatalf("open=%v", st)
		}
	}
	return fs, ino, a.Fh, b.Fh, func() string { mu.Lock(); defer mu.Unlock(); return data }
}

func TestFtruncateSiblingInheritsCompleteImage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sizes      []uint64
		offset     uint64
		data, want string
	}{
		{"partial", []uint64{5}, 0, "H", "Hello"},
		{"sparse", []uint64{5}, 8, "X", "hello\x00\x00\x00X"},
		{"regrow", []uint64{5, 8}, 0, "H", "Hello\x00\x00\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			for _, size := range tc.sizes {
				reviewFtruncate(t, fs, ino, a, size)
			}
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: tc.offset}, []byte(tc.data)); st != gofuse.OK || n != uint32(len(tc.data)) {
				t.Fatalf("write=%d/%v", n, st)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
				t.Fatalf("fsync=%v", st)
			}
			if got := remote(); got != tc.want {
				t.Fatalf("remote=%q want=%q", got, tc.want)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			fs.commitQueue.WaitPath("/file.bin")
			if got := remote(); got != tc.want {
				t.Fatalf("A release overwrote child: %q", got)
			}
		})
	}
}

func TestFtruncateSiblingRejectsParentFork(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}, []byte("X")); st != gofuse.EAGAIN {
		t.Fatalf("old A write=%v, want EAGAIN", st)
	}
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_FH | gofuse.FATTR_SIZE, Fh: a, Size: 3}}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
		t.Fatalf("old A truncate=%v, want EAGAIN", st)
	}
	fh, _ := fs.fileHandles.Get(a)
	if string(fh.Dirty.Bytes()) != "hello" {
		t.Fatalf("rejected A mutated bytes=%q", fh.Dirty.Bytes())
	}
}

func TestFtruncateSiblingAfterParentClose(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got := remote(); got != "Hello" {
		t.Fatalf("remote=%q", got)
	}
}

func TestFtruncateSiblingLockContention(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	fs.opts.RemoteCommitWaitTimeout = time.Millisecond
	unlock := fs.lockRemoteCommitPath("/file.bin")
	defer unlock()
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.EAGAIN {
		t.Fatalf("contended write=%v", st)
	}
}

func TestFtruncateSiblingHandoffQueueRefusal(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.commitQueue.mu.Lock()
	fs.commitQueue.stopped = true
	fs.commitQueue.mu.Unlock()
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("handoff failure not retained: %+v/%t", meta, ok)
	}
	data, err := fs.shadowStore.ReadAll("/file.bin")
	if err != nil || string(data) != "Hello" {
		t.Fatalf("retained child=%q/%v", data, err)
	}
	if got := remote(); got != "hello world" {
		t.Fatalf("queue refusal bypassed via upload: %q", got)
	}
	child, _ := fs.fileHandles.Get(b)
	if child.DirtySeq == 0 || child.PendingIndexGen != meta.Generation {
		t.Fatal("queue refusal lost child ownership")
	}
	fs.commitQueue.mu.Lock()
	fs.commitQueue.stopped = false
	fs.commitQueue.mu.Unlock()
}

func TestFtruncateSiblingHandoffMemoryChild(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	fs.commitQueue.WaitPath("/file.bin")
	if got := remote(); got != "Hello" {
		t.Fatalf("parent close did not hand off full child: %q", got)
	}
}

func TestFtruncateSiblingCopiedParentUpload(t *testing.T) {
	for _, parentFirst := range []bool{true, false} {
		t.Run(strconv.FormatBool(parentFirst), func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			parent, _ := fs.fileHandles.Get(a)
			parent.Lock()
			entry := &CommitEntry{Path: parent.Path, Inode: ino, MutationSeq: parent.DirtySeq, BaseRev: 1, Size: 5, Kind: PendingOverwrite}
			fs.bindCommitEntryToHandleLocked(entry, parent, 1)
			entry.bindPayload(parent.Dirty.Bytes())
			parent.Unlock()
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
				t.Fatal(st)
			}
			uploadParent := func() {
				unlock := fs.lockRemoteCommitPath("/file.bin")
				err := fs.commitQueue.commitNowPathLocked(context.Background(), entry)
				unlock()
				if parentFirst && err != nil {
					t.Fatal(err)
				}
			}
			if parentFirst {
				uploadParent()
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
				t.Fatal(st)
			}
			if !parentFirst {
				uploadParent()
			}
			if got := remote(); got != "Hello" {
				t.Fatalf("old copied payload overwrote child: %q", got)
			}
		})
	}
}

func TestFtruncateSiblingStrictFsyncWaitsForRemoteChild(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.EAGAIN {
		t.Fatalf("local-only child acknowledged as remote: %v", st)
	}
	if got := remote(); got != "hello world" {
		t.Fatalf("worker escaped gate: %q", got)
	}
	got, st, err := readDat9FSTestRange(fs, ino, b, 0, 1)
	if err != nil || st != gofuse.OK || string(got) != "H" {
		t.Fatalf("queued live B lost its accepted read: %q/%v/%v", got, st, err)
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatalf("landed child not acknowledged: %v", st)
	}
}

func TestFtruncateSiblingNewCopyCannotForkPendingChild(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}, []byte("X")); st != gofuse.EAGAIN {
		t.Fatalf("new copy created second pending branch: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}, []byte("X")); st != gofuse.OK {
		t.Fatalf("new copy remained blocked after commit: %v", st)
	}
}

func TestFtruncateSiblingReleaseRetainsOwnQueueFailure(t *testing.T) {
	fs, ino, a, _, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	fs.commitQueue.mu.Lock()
	fs.commitQueue.stopped = true
	fs.commitQueue.mu.Unlock()
	defer func() { fs.commitQueue.mu.Lock(); fs.commitQueue.stopped = false; fs.commitQueue.mu.Unlock() }()
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("missing retained failure: %+v/%t", meta, ok)
	}
	data, err := fs.shadowStore.ReadAll("/file.bin")
	if err != nil || string(data) != "hello" || remote() != "hello world" {
		t.Fatalf("queue refusal lost/tried uploading parent: %q/%v", data, err)
	}
}

func TestFtruncateSiblingDoesNotPromoteRecoveredAncestry(t *testing.T) {
	fs, ino, a, _, _ := newFtruncateCommitFS(t)
	fh, _ := fs.fileHandles.Get(a)
	fh.ContentSnapshotID = "recovered-snapshot"
	fh.contentAncestors = []string{"recovered-parent"}
	fh.LineageTrusted = false
	reviewFtruncate(t, fs, ino, a, 5)
	if slices.Contains(fh.stagedAncestors, "recovered-snapshot") || slices.Contains(fh.stagedAncestors, "recovered-parent") {
		t.Fatal("truncate promoted untrusted ancestry")
	}
}

func TestFtruncateSiblingCleanFlushDoesNotReservePath(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
	if !ok {
		t.Fatal("clean sibling Flush reserved parent path")
	}
	unlock()
}

func TestFtruncateSiblingRetryClearsRetainedAncestor(t *testing.T) {
	var rejected atomic.Bool
	fs, ino, a, b, remote := newFtruncateCommitFS(t, func(w http.ResponseWriter, _ *http.Request) bool {
		if rejected.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusConflict)
			return true
		}
		return false
	})
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	fs.commitQueue.WaitPath("/file.bin")
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("missing rejected child: %+v/%t", meta, ok)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got := remote(); got != "Hello" {
		t.Fatalf("retry content=%q", got)
	}
	if fs.pendingIndex.HasPending("/file.bin") || fs.shadowStore.Has("/file.bin") {
		t.Fatal("successful retry retained its exact ancestor conflict")
	}
}
