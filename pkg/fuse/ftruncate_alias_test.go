package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newHardlinkFS(t *testing.T) (*Dat9FS, uint64, uint64, uint64, func() string) {
	t.Helper()
	fs, ino, a, unused, remote := newFtruncateCommitFS(t)
	// Keep this fixture strictly two writers.
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: unused})
	if !fs.inodes.AddAlias(ino, "/alias.bin", "shared-resource", 2, false, 11, time.Now()) {
		t.Fatal("alias")
	}
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.commitQueue.PathLock = fs.lockRemoteCommitPath

	return fs, ino, a, b.Fh, remote
}
func hardlinkWrite(t *testing.T, fs *Dat9FS, ino, fh uint64, data string) {
	t.Helper()
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh}, []byte(data)); st != gofuse.OK {
		t.Fatal(st)
	}
}
func hardlinkSync(t *testing.T, fs *Dat9FS, ino, fh uint64) {
	t.Helper()
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh}); st != gofuse.OK {
		t.Fatal(st)
	}
}
func TestHardlinkAliasSequential(t *testing.T) {
	for _, parentFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "child-first", true: "parent-first"}[parentFirst], func(t *testing.T) {
			fs, ino, a, b, remote := newHardlinkFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			if parentFirst {
				hardlinkSync(t, fs, ino, a)
			}
			hardlinkWrite(t, fs, ino, b, "H")
			hardlinkSync(t, fs, ino, b)
			hardlinkSync(t, fs, ino, a)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			t.Logf("REMOTE=%q parent_first=%t", remote(), parentFirst)
			if remote() != "Hello" {
				t.Fatal("tail restored")
			}
		})
	}
}
func TestHardlinkAliasFencesOtherAlias(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	for _, p := range []string{"/file.bin", "/alias.bin"} {
		unlock := fs.lockRemoteCommitPath(p)
		_, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
		unlock()
		if st != gofuse.EAGAIN {
			t.Fatalf("lock %s bypassed: %v", p, st)
		}
	}
	fh, _ := fs.fileHandles.Get(b)
	if fh.Dirty.Size() != 11 {
		t.Fatal("rejected writes mutated buffer")
	}
	hardlinkWrite(t, fs, ino, b, "H")
	t.Log("B rejected while either alias path lock held; inherited only after exclusion")
}
func TestHardlinkAliasInvalidAlias(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	fs.inodes.RemoveLinkPreserve("/alias.bin")
	fs.inodes.Lookup("/alias.bin", false, 3, time.Now())
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.EAGAIN {
		t.Fatalf("retargeted alias accepted=%v", st)
	}
	t.Log("old same-inode event cannot authorize replaced alias")
}
func TestHardlinkAliasQueuedParent(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	original := fs.commitQueue.PathLock
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.EAGAIN {
		t.Fatalf("queued alias bypassed=%v", st)
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatal(remote())
	}
	t.Log("parent queued on other alias fenced; retry after commit preserved truncate")
}
func TestHardlinkAliasDirtyChildParentRelease(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	fs.commitQueue.WaitPath("/alias.bin")
	hardlinkSync(t, fs, ino, b)
	if remote() != "Hello" {
		t.Fatal(remote())
	}
	t.Log("parent Release handed off dirty alias child")
}
func TestHardlinkAliasNoIndependentDirtyMerge(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	parent, _ := fs.fileHandles.Get(a)
	parent.Lock()
	err := fs.prepareFtruncateMutationLocked(context.Background(), parent)
	parent.Unlock()
	if err != syscall.EAGAIN {
		t.Fatalf("older branch permitted=%v", err)
	}
	t.Log("older parent mutation rejected while dirty descendant exists")
}

func TestHardlinkAliasClosedSourceOtherAliasReader(t *testing.T) {
	fs, ino, a, c, remote := newFtruncateCommitFS(t)
	if !fs.inodes.AddAlias(ino, "/alias.bin", "shared-resource", 2, false, 11, time.Now()) {
		t.Fatal("alias")
	}
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b.Fh, "H")
	hardlinkSync(t, fs, ino, b.Fh)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, 11)
	t.Logf("REMOTE=%q clean_reader_other_alias=%q status=%v err=%v", remote(), got, st, err)
	if err != nil || st != gofuse.OK || string(got) != "Hello" {
		t.Fatalf("alias commit not visible after source close: %q/%v/%v", got, st, err)
	}
}

func newHardlinkReadFS(t *testing.T) (*Dat9FS, uint64, uint64, uint64, uint64, *atomic.Bool, func() string) {
	t.Helper()
	reject := new(atomic.Bool)
	fs, ino, a, c, remote := newFtruncateCommitFS(t, func(w http.ResponseWriter, r *http.Request) bool {
		if reject.Load() {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		return false
	})
	if !fs.inodes.AddAlias(ino, "/alias.bin", "shared-resource", 2, false, 11, time.Now()) {
		t.Fatal("alias")
	}
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.commitQueue.PathLock = fs.lockRemoteCommitPath

	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if fs.openHandles.HasVisibleTruncate(ino) {
		t.Fatal("source marker remains")
	}
	return fs, ino, a, b.Fh, c, reject, remote
}
func readHardlinkWant(t *testing.T, fs *Dat9FS, ino, c uint64, want string) {
	t.Helper()
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, 11)
	if err != nil || st != gofuse.OK || string(got) != want {
		t.Fatalf("read=%q/%v/%v want=%q", got, st, err, want)
	}
}
func stateOf(fs *Dat9FS, ino uint64) inodeMutationState {
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()
	return fs.mutationInodes[ino]
}

func TestHardlinkAliasCommittedAfterSourceClose(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			fs, ino, _, b, c, _, remote := newHardlinkReadFS(t)
			before := stateOf(fs, ino)
			hardlinkWrite(t, fs, ino, b, "H")
			if async {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
				fs.commitQueue.WaitPath("/alias.bin")
			} else {
				hardlinkSync(t, fs, ino, b)
			}
			after := stateOf(fs, ino)
			if after.committedSeq <= before.committedSeq || after.committedRevision <= before.committedRevision || after.committedSize != 5 {
				t.Fatalf("commit not recorded before=%+v after=%+v", before, after)
			}
			readHardlinkWant(t, fs, ino, c, "Hello")
			t.Logf("SOURCE_CLOSED committed seq=%d revision=%d size=%d remote=%q read=Hello", after.committedSeq, after.committedRevision, after.committedSize, remote())
		})
	}
}
func TestHardlinkAliasLocalSuccessors(t *testing.T) {
	for _, mode := range []string{"memory", "queued", "failed-sync", "retained-closed-conflict"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, _, b, c, reject, remote := newHardlinkReadFS(t)
			before := stateOf(fs, ino)
			hardlinkWrite(t, fs, ino, b, "H")
			gate := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(gate) }) })
			switch mode {
			case "queued":
				original := fs.commitQueue.PathLock
				fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
			case "failed-sync":
				reject.Store(true)
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st == gofuse.OK {
					t.Fatal("failure not injected")
				}
			case "retained-closed-conflict":
				fs.commitQueue.DrainAll()
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
			}
			metaBefore, _ := fs.pendingIndex.GetMeta("/alias.bin")
			shadowBefore := fs.shadowStore.ActiveGeneration("/alias.bin")
			readHardlinkWant(t, fs, ino, c, "Hello")
			after := stateOf(fs, ino)
			metaAfter, _ := fs.pendingIndex.GetMeta("/alias.bin")
			if after.committedSeq != before.committedSeq || after.committedRevision != before.committedRevision {
				t.Fatal("uncommitted content recorded as committed")
			}
			if (metaBefore == nil) != (metaAfter == nil) || (metaBefore != nil && (metaBefore.Generation != metaAfter.Generation || metaBefore.Kind != metaAfter.Kind)) ||
				shadowBefore != fs.shadowStore.ActiveGeneration("/alias.bin") {
				t.Fatal("read changed staging ownership")
			}
			t.Logf("LOCAL mode=%s remote=%q read=Hello committed_unchanged=true ownership_unchanged=true", mode, remote())
			if mode == "queued" {
				once.Do(func() { close(gate) })
				fs.commitQueue.WaitPath("/alias.bin")
				readHardlinkWant(t, fs, ino, c, "Hello")
			}
		})
	}
}
func TestHardlinkAliasRollback(t *testing.T) {
	fs, ino, _, b, c, reject, _ := newHardlinkReadFS(t)
	before := stateOf(fs, ino)
	fh, _ := fs.fileHandles.Get(b)
	fh.WritePolicy = WritePolicyWriteSync
	reject.Store(true)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st == gofuse.OK {
		t.Fatal("write-sync must fail")
	}
	readHardlinkWant(t, fs, ino, c, "hello")
	after := stateOf(fs, ino)
	if after.committedSeq != before.committedSeq || fh.DirtySeq != 0 {
		t.Fatal("rolled back write remained accepted")
	}
	t.Logf("ROLLBACK rejected bytes=X, inode latestSeq=%d, effective read=hello", after.latestSeq)
}

func TestHardlinkAliasActiveSourceRead(t *testing.T) {
	fs, ino, a, c, _ := newFtruncateCommitFS(t)
	fs.inodes.AddAlias(ino, "/alias.bin", "shared", 2, false, 11, time.Now())
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b.Fh, "H")
	readHardlinkWant(t, fs, ino, c, "Hello")
}

func TestHardlinkAliasQueueIdentityAndBatchFence(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	fh, _ := fs.fileHandles.Get(b)
	fh.Lock()
	entry := &CommitEntry{Path: fh.Path, Inode: ino, MutationSeq: fh.DirtySeq, Size: 5, BaseRev: 1, Kind: PendingOverwrite}
	fs.bindCommitEntryToHandleLocked(entry, fh, 1)
	entry.bindPayload(fh.Dirty.Bytes())
	fh.Unlock()
	if len(entry.ftruncatePaths) != 2 || entry.ftruncateValid == nil {
		t.Fatal("queue lacks captured alias binding")
	}
	if fs.commitQueue.batchWriteEligible(entry) {
		t.Fatal("aliased lock set must not be batched")
	}
	unlock := fs.commitQueue.lockEntryPath(entry)
	for _, p := range []string{"/file.bin", "/alias.bin"} {
		if release, ok := fs.tryLockRemoteCommitPath(p); ok {
			release()
			t.Fatalf("queue did not lock %s", p)
		}
	}
	unlock()
	fs.inodes.RemoveLinkPreserve("/alias.bin")
	fs.inodes.Lookup("/alias.bin", false, 7, time.Now())
	if err := fs.commitQueue.validateEntryPayloadFreshCtx(context.Background(), entry); err != syscall.ESTALE {
		t.Fatalf("replacement accepted: %v", err)
	}
}

func TestHardlinkAliasDirtyIndependentTruncateRefused(t *testing.T) {
	fs, ino, a, b, _ := newHardlinkFS(t)
	hardlinkWrite(t, fs, ino, b, "H")
	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_FH | gofuse.FATTR_SIZE, Fh: a, Size: 5}}, &out)
	if st != gofuse.EAGAIN {
		t.Fatalf("independent dirty alias accepted truncate: %v", st)
	}
	parent, _ := fs.fileHandles.Get(a)
	if parent.Dirty.Size() != 11 {
		t.Fatal("refused truncate mutated parent")
	}
}

func TestHardlinkAliasWarmReadsUseVerifiedImage(t *testing.T) {
	fs, ino, _, b, c, _, _ := newHardlinkReadFS(t)
	target, err := url.Parse(fs.client.BaseURL())
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			reads.Add(1)
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fs.client = newTestClient(server.URL)
	fs.commitQueue.client = fs.client
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	// Exercise both immutable-root and landed-successor warm cache proofs.
	for _, want := range []string{"hello", "Hello", "Jello"} {
		if want != "hello" {
			hardlinkWrite(t, fs, ino, b, want[:1])
			hardlinkSync(t, fs, ino, b)
		}
		before := reads.Load()
		readHardlinkWant(t, fs, ino, c, want)
		validated := reads.Load()
		if validated <= before {
			t.Fatal("changed committed image was not validated")
		}
		for i := 0; i < 20; i++ {
			readHardlinkWant(t, fs, ino, c, want)
		}
		if got := reads.Load(); got != validated {
			t.Fatalf("warm reads added %d HTTP requests", got-validated)
		}
		t.Logf("image=%s initial validation requests=%d warm requests=0", want, validated-before)
	}
}

func TestHardlinkAliasClosedChildRetiresParentStaging(t *testing.T) {
	fs, ino, a, b, remote := newHardlinkFS(t)
	hardlinkWrite(t, fs, ino, a, "hello world")
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	if !fs.pendingIndex.HasPending("/file.bin") {
		t.Fatal("parent staging missing")
	}
	// Truncate supersedes A\'s staged image and releases its earlier Flush fence.
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	fs.commitQueue.PathLock = func(p string) func() { <-gate; return fs.lockRemoteCommitPath(p) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if fs.pendingIndex.HasPending("/file.bin") || fs.shadowStore.Has("/file.bin") {
		t.Fatal("retired parent leaked its own staging")
	}
	if !fs.pendingIndex.HasPending("/alias.bin") {
		t.Fatal("parent removed child staging")
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/alias.bin")
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}

func TestHardlinkAliasReadSupersedesOlderSourceStaging(t *testing.T) {
	fs, ino, a, c, _ := newFtruncateCommitFS(t)
	fs.inodes.AddAlias(ino, "/alias.bin", "shared", 2, false, 11, time.Now())
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	hardlinkWrite(t, fs, ino, a, "hello world")
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	readHardlinkWant(t, fs, ino, c, "hello")
	hardlinkWrite(t, fs, ino, b.Fh, "H")
	readHardlinkWant(t, fs, ino, c, "Hello")
}

func TestHardlinkAliasReadNewTruncateIgnoresLiveAncestor(t *testing.T) {
	fs, ino, a, c, _ := newFtruncateCommitFS(t)
	fs.inodes.AddAlias(ino, "/alias.bin", "shared", 2, false, 11, time.Now())
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	reviewFtruncate(t, fs, ino, b.Fh, 3)
	readHardlinkWant(t, fs, ino, c, "hel")
}

func TestHardlinkAliasOpenedAfterTruncateInherits(t *testing.T) {
	fs, ino, a, c, remote := newFtruncateCommitFS(t)
	fs.inodes.AddAlias(ino, "/alias.bin", "shared", 2, false, 11, time.Now())
	reviewFtruncate(t, fs, ino, a, 5)
	var b gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &b); st != gofuse.OK {
		t.Fatal(st)
	}
	hardlinkWrite(t, fs, ino, b.Fh, "H")
	hardlinkSync(t, fs, ino, b.Fh)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	readHardlinkWant(t, fs, ino, c, "Hello")
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}
