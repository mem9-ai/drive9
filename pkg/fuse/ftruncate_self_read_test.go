package fuse

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func readFtruncateSelfWant(t *testing.T, fs *Dat9FS, ino, id uint64, offset int64, size int, want string) {
	t.Helper()
	got, st, err := readDat9FSTestRange(fs, ino, id, offset, size)
	if err != nil || st != gofuse.OK || string(got) != want {
		t.Fatalf("read offset=%d size=%d = %q/%v/%v, want %q/OK", offset, size, got, st, err, want)
	}
}

// Capture the actual immutable upload before the next write, as a queue
// worker may do. Uploading it later exercises the parent/child ordering.
func copyFtruncateSelfCommit(t *testing.T, fs *Dat9FS, id uint64) *CommitEntry {
	t.Helper()
	h, ok := fs.fileHandles.Get(id)
	if !ok {
		t.Fatal("missing handle")
	}
	h.Lock()
	defer h.Unlock()
	e := &CommitEntry{Path: h.Path, Inode: h.Ino, MutationSeq: h.DirtySeq, BaseRev: h.BaseRev, Size: h.Dirty.Size(), Kind: PendingOverwrite}
	fs.bindCommitEntryToHandleLocked(e, h, h.BaseRev)
	e.bindPayload(h.Dirty.Bytes())
	return e
}
func commitFtruncateSelfCopy(t *testing.T, fs *Dat9FS, e *CommitEntry) {
	t.Helper()
	unlock := fs.lockRemoteCommitPath(e.Path)
	err := fs.commitQueue.commitNowPathLocked(context.Background(), e)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestFtruncateInheritedSelfRead(t *testing.T) {
	for _, mode := range []string{"pending", "root-before-write", "root-after-write", "own-commit", "later-ancestor", "rebound-parent", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			var reject atomic.Bool
			fs, ino, a, b, remote := newFtruncateCommitFS(t, func(w http.ResponseWriter, _ *http.Request) bool {
				if reject.Load() {
					w.WriteHeader(http.StatusConflict)
					return true
				}
				return false
			})
			if mode == "rebound-parent" {
				fs.opts.GVisorCompat = true
			}
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			// Notification alone leaves B clean; it must observe the truncation, not
			// promote its original pre-truncate buffer to a writable base.
			readFtruncateSelfWant(t, fs, ino, b, 0, 4096, "hello")
			var copied *CommitEntry
			if mode == "root-before-write" {
				hardlinkSync(t, fs, ino, a)
			}
			if mode == "root-after-write" {
				copied = copyFtruncateSelfCommit(t, fs, a)
			}
			hardlinkWrite(t, fs, ino, b, "H")
			want := "Hello"
			reader := b
			switch mode {
			case "root-after-write":
				commitFtruncateSelfCopy(t, fs, copied)
			case "own-commit", "later-ancestor":
				if mode == "own-commit" {
					hardlinkSync(t, fs, ino, b)
				} else {
					copied = copyFtruncateSelfCommit(t, fs, b)
				}
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("X")); n != 1 || st != gofuse.OK {
					t.Fatal(n, st)
				}
				if copied != nil {
					commitFtruncateSelfCopy(t, fs, copied)
				}
				want = "HXllo"
			case "rebound-parent":
				hardlinkSync(t, fs, ino, b)
				hardlinkWrite(t, fs, ino, a, "J")
				reader, want = a, "Jello"
			case "rollback":
				h, _ := fs.fileHandles.Get(b)
				seq := h.DirtySeq
				h.WritePolicy = WritePolicyWriteSync
				reject.Store(true)
				n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("X"))
				reject.Store(false)
				if n != 0 || st != gofuse.Status(syscall.EEXIST) || h.DirtySeq != seq {
					t.Fatalf("rollback=%d/%v seq=%d want 0/EEXIST/%d", n, st, h.DirtySeq, seq)
				}
				h.WritePolicy = WritePolicyWriteBack
			}
			readFtruncateSelfWant(t, fs, ino, reader, 0, 5, want)
			readFtruncateSelfWant(t, fs, ino, reader, 1, 4, want[1:])
			readFtruncateSelfWant(t, fs, ino, reader, 0, 4096, want)
			readFtruncateSelfWant(t, fs, ino, reader, 5, 4096, "")
			readFtruncateSelfWant(t, fs, ino, ro, 0, 4096, want)
			hardlinkSync(t, fs, ino, reader)
			// Either old handle may be finalized later; neither may restore the tail.
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
			fs.commitQueue.WaitPath("/file.bin")
			if remote() != want {
				t.Fatalf("final remote=%q want=%q", remote(), want)
			}
		})
	}
}

func TestFtruncateInheritedSelfReadRanges(t *testing.T) {
	for _, mode := range []string{"memory", "shadow", "sparse", "regrow", "many-writes", "large-range"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, _ := newFtruncateCommitFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			if mode == "regrow" {
				reviewFtruncate(t, fs, ino, a, 8)
			}
			hardlinkWrite(t, fs, ino, b, "H")
			want := "Hello"
			h, _ := fs.fileHandles.Get(b)
			switch mode {
			case "regrow":
				want = "Hello\x00\x00\x00"
			case "sparse":
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 8}, []byte("X")); st != gofuse.OK {
					t.Fatal(st)
				}
				want = "Hello\x00\x00\x00X"
			case "large-range":
				payload := strings.Repeat("Q", 9<<20)
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 5}, []byte(payload)); st != gofuse.OK {
					t.Fatal(st)
				}
				want += payload
			case "many-writes":
				hardlinkSync(t, fs, ino, b)
				for i := 0; i < 70; i++ {
					hardlinkWrite(t, fs, ino, b, "H")
				}
			case "shadow":
				h.Lock()
				err := fs.stageShadowLocked(h, false)
				h.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			// Reads of already-local data must not introduce HTTP reads or commits.
			var requests atomic.Int32
			interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, _ *http.Request) bool { requests.Add(1); w.WriteHeader(500); return true })
			for _, r := range []struct {
				offset int64
				size   int
			}{{0, 5}, {1, 4}, {0, 4096}, {int64(len(want) - 2), 4096}, {int64(len(want)), 5}, {int64(len(want) + 1), 5}} {
				end := min(r.offset+int64(r.size), int64(len(want)))
				expected := ""
				if r.offset < end {
					expected = want[r.offset:end]
				}
				readFtruncateSelfWant(t, fs, ino, b, r.offset, r.size, expected)
			}
			if requests.Load() != 0 {
				t.Fatalf("local self-read made %d HTTP requests", requests.Load())
			}
		})
	}
}

func TestFtruncateInheritedSelfReadReboundCommit(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	fs.opts.GVisorCompat = true
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	hardlinkSync(t, fs, ino, b)
	hardlinkWrite(t, fs, ino, a, "J")
	readFtruncateSelfWant(t, fs, ino, a, 0, 4096, "Jello")
	hardlinkSync(t, fs, ino, a)
	readFtruncateSelfWant(t, fs, ino, a, 0, 4096, "Jello")
	for _, id := range []uint64{a, b} {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
	fs.commitQueue.WaitPath("/file.bin")
	if remote() != "Jello" {
		t.Fatalf("remote after rebound commit and release = %q, want Jello", remote())
	}
}

func TestFtruncateInheritedSelfReadHandoff(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	readFtruncateSelfWant(t, fs, ino, b, 0, 4096, "Hello")
	if remote() != "hello world" {
		t.Fatal("upload escaped gate")
	}
	release()
	fs.commitQueue.WaitPath("/file.bin")
	readFtruncateSelfWant(t, fs, ino, b, 0, 4096, "Hello")
	if remote() != "Hello" {
		t.Fatal(remote())
	}
}

func TestFtruncateInheritedSelfReadRejectsUnknownBase(t *testing.T) {
	for _, mode := range []string{"untrusted", "different-event", "unrelated-revision", "unknown-commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, _ := newFtruncateCommitFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkWrite(t, fs, ino, b, "H")
			h, _ := fs.fileHandles.Get(b)
			h.Lock()
			switch mode {
			case "untrusted":
				h.LineageTrusted = false
			case "different-event":
				e := *h.pendingFtruncate.Load()
				e.id = "unrelated"
				h.pendingFtruncate.Store(&e)
			case "unrelated-revision":
				fs.recordCommittedRevisionWithSize(h.Path, 99, 5)
				fs.inodes.UpdateRevision(ino, 99)
			case "unknown-commit":
				// Fault fixture: a surviving newer writer cannot use an unidentified
				// intermediate committed image as proof of its untouched bytes.
				h.DirtySeq = 3
				fs.restoreDirtySize(ino, 3, 5)
				fs.recordCommittedMutation(ino, 2, 2, 5)
				fs.recordCommittedRevisionWithSize(h.Path, 2, 5)
				fs.inodes.UpdateRevision(ino, 2)
			}
			h.Unlock()
			got, st, err := readDat9FSTestRange(fs, ino, b, 0, 5)
			if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("unknown base=%q/%v/%v", got, st, err)
			}
		})
	}
}

func TestFtruncateInheritedSelfReadLazy(t *testing.T) {
	for _, invalidate := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-revision", true: "changed-revision"}[invalidate], func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkWrite(t, fs, ino, b, "H")
			hardlinkSync(t, fs, ino, b)
			var change atomic.Bool
			var reads atomic.Int32
			interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet {
					reads.Add(1)
					http.ServeContent(w, r, "file.bin", time.Time{}, strings.NewReader(remote()))
					return true
				}
				if r.Method == http.MethodHead && change.Load() {
					w.Header().Set("X-Dat9-Revision", "3")
					w.Header().Set("Content-Length", "5")
					return true
				}
				return false
			})
			h, _ := fs.fileHandles.Get(b)
			h.Lock()
			h.Dirty.partSize = 2
			fs.rebindCleanWriteBufferToRemoteLocked(h, 5)
			h.Unlock()
			hardlinkWrite(t, fs, ino, b, "X")
			if h.Dirty.IsPartLoaded(1) || h.Dirty.IsPartLoaded(2) {
				t.Fatal("fixture did not leave inherited parts lazy")
			}
			before := reads.Load()
			change.Store(invalidate)
			got, st, err := readDat9FSTestRange(fs, ino, b, 0, 5)
			if invalidate {
				if err != nil || st != gofuse.EAGAIN || len(got) != 0 || h.Dirty.IsPartLoaded(1) || h.Dirty.IsPartLoaded(2) {
					t.Fatalf("failed lazy proof=%q/%v/%v", got, st, err)
				}
			} else if err != nil || st != gofuse.OK || string(got) != "Xello" {
				t.Fatalf("lazy=%q/%v/%v", got, st, err)
			}
			if reads.Load()-before != 2 {
				t.Fatalf("lazy GETs=%d want2", reads.Load()-before)
			}
		})
	}
}
