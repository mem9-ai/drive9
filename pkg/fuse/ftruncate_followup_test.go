package fuse

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func interceptFtruncateHTTP(t *testing.T, fs *Dat9FS, intercept func(http.ResponseWriter, *http.Request) bool) {
	t.Helper()
	target, err := url.Parse(fs.client.BaseURL())
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if intercept != nil && intercept(w, r) {
			return
		}
		w.Header().Set("X-Dat9-Resource-ID", "review-inode")
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fs.client = newTestClient(server.URL)
	fs.client.SetSmallFileThresholdForTests(8 << 20)
	fs.commitQueue.client = fs.client
}

func TestReviewFixChmodFailureThenSiblingWrite(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	parent, _ := fs.fileHandles.Get(a)
	parent.Lock()
	fs.setPendingModeLocked(parent, 0644, fs.nextPendingModeGen())
	if err := fs.snapshotWriteBackLocked(parent, true); err != nil {
		t.Fatal(err)
	}
	parent.Unlock()
	uploader := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", OnDataCommitted: fs.onWriteBackDataCommitted, SnapshotStagingGens: fs.snapshotStagingGens}
	if _, err := uploader.UploadSyncWithRevision(t.Context(), "/file.bin"); err == nil {
		t.Fatal("expected chmod failure")
	}
	if remote() != "hello" {
		t.Fatal("data PUT did not commit")
	}
	if fs.latestCommittedRevision("/file.bin") != 2 {
		t.Errorf("missing committed path revision: %d", fs.latestCommittedRevision("/file.bin"))
	}
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}

func TestReviewFixLargeLiveRange(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "inherited-write", true: "direct-truncate"}[direct], func(t *testing.T) {
			fs, ino, a, b, _ := newFtruncateCommitFS(t)
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			if direct {
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				b = out.Fh
			}
			hardlinkWrite(t, fs, ino, b, strings.Repeat("x", int(maxLandedPayloadBytes)+1))
			got, st, err := readDat9FSTestRange(fs, ino, ro, 0, 16)
			if err != nil || st != gofuse.OK || string(got) != strings.Repeat("x", 16) {
				t.Fatalf("range=%q/%v/%v", got, st, err)
			}
		})
	}
}

func TestReviewFixResetCommittedHandles(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "late-commit"}[late], func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			interceptFtruncateHTTP(t, fs, nil)
			fs.inodes.SetIdentity(ino, "review-inode", 1)
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			if late {
				gate := make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(gate) }) })
				original := fs.commitQueue.PathLock
				fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
				fs.resetMountView()
				once.Do(func() { close(gate) })
				fs.commitQueue.WaitPath("/file.bin")
			} else {
				hardlinkSync(t, fs, ino, a)
				fs.resetMountView()
			}
			if remote() != "hello" {
				t.Fatal(remote())
			}
			readHardlinkWant(t, fs, ino, ro, "hello")
			if !late {
				hardlinkWrite(t, fs, ino, b, "H")
				hardlinkSync(t, fs, ino, b)
				if remote() != "Hello" {
					t.Fatal(remote())
				}
			}
		})
	}
}

func TestReviewFixQueuedTruncateSurvivesAliasUnlink(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return true
		}
		return false
	})
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	reviewFtruncate(t, fs, ino, a, 5)
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	original := fs.commitQueue.PathLock
	fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "alias.bin"); st != gofuse.OK {
		t.Fatal(st)
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	if got := remote(); got != "hello" {
		t.Fatalf("surviving inode content=%q, want hello", got)
	}
}

func TestReviewFixAliasRenameOrdersNewWriter(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	fs.inodes.SetIdentity(ino, "review-inode", 2)
	var renamed atomic.Bool
	interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("rename") {
			renamed.Store(true)
			w.WriteHeader(http.StatusOK)
			return true
		}
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/moved.bin") && !renamed.Load() {
			w.WriteHeader(http.StatusNotFound)
			return true
		}
		return false
	})
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	reviewFtruncate(t, fs, ino, a, 5)
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	original := fs.commitQueue.PathLock
	fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "alias.bin", "moved.bin"); st != gofuse.OK {
		t.Fatal(st)
	}
	if p, _ := fs.inodes.GetPath(ino); p != "/moved.bin" {
		t.Fatalf("Open would use %s", p)
	}
	var opened gofuse.OpenOut
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &opened)
	}()
	select {
	case st := <-done:
		t.Fatalf("new alias bypassed pending inode commit: %v", st)
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatal(st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new Open did not resume")
	}
	if remote() != "hello" {
		t.Fatal(remote())
	}
	hardlinkWrite(t, fs, ino, opened.Fh, "Y")
	hardlinkSync(t, fs, ino, opened.Fh)
	if remote() != "Yello" {
		t.Fatal(remote())
	}
}

func TestReviewFixDataCommitPreservesNewerState(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	parent, _ := fs.fileHandles.Get(a)
	rootSeq := parent.DirtySeq
	hardlinkWrite(t, fs, ino, b, "X")
	child, _ := fs.fileHandles.Get(b)
	before := string(child.Dirty.Bytes())
	base := child.BaseRev
	meta := WriteBackMeta{Path: "/file.bin", Inode: ino, MutationSeq: rootSeq, Size: 5}
	fs.onWriteBackDataCommitted(meta, 3, StagingGens{})
	fs.onWriteBackDataCommitted(meta, 2, StagingGens{})
	if fs.latestCommittedRevision("/file.bin") != 3 {
		t.Fatal("late callback downgraded revision")
	}
	if child.BaseRev != base || string(child.Dirty.Bytes()) != before {
		t.Fatal("dirty buffer adopted a different commit")
	}
	entry, _ := fs.inodes.GetEntry(ino)
	if entry.Revision != 3 {
		t.Fatal("inode revision downgraded")
	}
}

func TestReviewFixExistingUnmarkedWriterAdmission(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	child, _ := fs.fileHandles.Get(b)
	child.pendingFtruncate.Store(nil) // Exercise admission independent of notification ownership.
	before, seq := string(child.Dirty.Bytes()), child.DirtySeq
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	original := fs.commitQueue.PathLock
	fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st != gofuse.EAGAIN {
		t.Fatal(st)
	}
	if child.DirtySeq != seq || string(child.Dirty.Bytes()) != before || child.ShadowStageGen != 0 || child.WriteBackGen != 0 {
		t.Fatal("refused write had side effects")
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}

func TestReviewFixResetRetiresInactiveStreamer(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	interceptFtruncateHTTP(t, fs, nil)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	child, _ := fs.fileHandles.Get(b)
	// A completed O_TRUNC uploader may remain attached. Use tiny parts so
	// a one-byte edit leaves untouched remote-backed parts in a small fixture.
	child.Streamer = NewStreamUploader(fs.client, child.Path, child.BaseRev, fs.remoteRoot())
	child.Dirty.partSize = 2
	fs.resetMountView()
	readHardlinkWant(t, fs, ino, b, "hello")
	if child.Streamer != nil {
		t.Fatal("lazy recovered buffer retained old uploader")
	}
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatalf("untouched tail changed: %q", remote())
	}
}

func TestReviewFixCreatedInodeIdentityRecovery(t *testing.T) {
	for _, commitFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "truncate-before-first-commit", true: "truncate-after-first-commit"}[commitFirst], func(t *testing.T) {
			fs, _, _, _, remote := newFtruncateCommitFS(t)
			var created atomic.Bool
			interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/created.bin") {
					if r.Method == http.MethodHead && !created.Load() {
						w.WriteHeader(http.StatusNotFound)
						return true
					}
					if r.Method == http.MethodPut && !created.Swap(true) {
						r.Header.Set("X-Dat9-Expected-Revision", "1")
					}
				}
				return false
			})
			var out gofuse.CreateOut
			if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: uint32(syscall.O_RDWR), Mode: 0644}, "created.bin", &out); st != gofuse.OK {
				t.Fatal(st)
			}
			ino, fd := out.NodeId, out.Fh
			hardlinkWrite(t, fs, ino, fd, "hello world")
			if commitFirst {
				hardlinkSync(t, fs, ino, fd)
			}
			reviewFtruncate(t, fs, ino, fd, 5)
			hardlinkSync(t, fs, ino, fd)
			entry, _ := fs.inodes.GetEntry(ino)
			if entry.ResourceID == "" {
				t.Fatal("committed truncate never captured resource identity")
			}
			fs.resetMountView()
			readHardlinkWant(t, fs, ino, fd, "hello")
			hardlinkWrite(t, fs, ino, fd, "H")
			hardlinkSync(t, fs, ino, fd)
			if remote() != "Hello" {
				t.Fatal(remote())
			}
		})
	}
}

func TestReviewFixCallbackRetiresInactiveStreamer(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	child, _ := fs.fileHandles.Get(b)
	child.Lock()
	if err := fs.prepareFtruncateMutationLocked(t.Context(), child); err != nil {
		child.Unlock()
		t.Fatal(err)
	}
	child.Unlock()
	child.Streamer = NewStreamUploader(fs.client, child.Path, child.BaseRev, fs.remoteRoot())
	child.Dirty.partSize = 2
	parent, _ := fs.fileHandles.Get(a)
	parent.Lock()
	fs.setPendingModeLocked(parent, 0644, fs.nextPendingModeGen())
	err := fs.snapshotWriteBackLocked(parent, true)
	parent.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	uploader := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", OnDataCommitted: fs.onWriteBackDataCommitted, SnapshotStagingGens: fs.snapshotStagingGens}
	if _, err := uploader.UploadSyncWithRevision(t.Context(), "/file.bin"); err == nil {
		t.Fatal("expected mode failure")
	}
	if child.Streamer != nil {
		t.Fatal("lazy callback buffer retained old uploader")
	}
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}
