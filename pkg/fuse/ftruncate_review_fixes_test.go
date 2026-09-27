package fuse

import (
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateAppendRetryKeepsStrictFence(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	fs.opts.RemoteCommitWaitTimeout = 500 * time.Millisecond
	child, _ := fs.fileHandles.Get(b)
	child.Flags |= uint32(syscall.O_APPEND)
	readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	reader, _ := fs.fileHandles.Get(readerID)
	reader.Lock()
	readerLocked := true
	defer func() {
		if readerLocked {
			reader.Unlock()
		}
	}()
	done := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X"))
		done <- st
	}()
	deadline := time.Now().Add(2 * time.Second)
	var holdPath func()
	for time.Now().Before(deadline) {
		if child.TryLock() {
			// Nonempty inherited root proves initial strict preparation already ran.
			// Nil token proves the append retry yielded that original path fence.
			if child.ftruncateInherited != "" && child.RemoteCommitUnlock == nil {
				unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
				if ok {
					holdPath = unlock
					child.Unlock()
					break
				}
			}
			child.Unlock()
		}
		runtime.Gosched()
	}
	if holdPath == nil {
		t.Fatal("did not observe the append retry after strict preparation")
	}
	defer func() {
		if holdPath != nil {
			holdPath()
		}
	}()
	reader.Unlock()
	readerLocked = false
	var st gofuse.Status
	select {
	case st = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not complete")
	}
	unlock, acquired := fs.tryLockRemoteCommitPath("/file.bin")
	if acquired {
		unlock()
		t.Fatal("test lost its competing path lock")
	}
	child.Lock()
	data := string(child.Dirty.Bytes())
	child.Unlock()
	t.Logf("OBSERVED after append retry: Write=%v buffer=%q competing_path_lock_still_held=true", st, data)
	if data != "hello" {
		t.Fatalf("rejected append modified data: %q", data)
	}
	if st != gofuse.EAGAIN {
		t.Fatalf("participating append bypassed strict fence: Write=%v", st)
	}
	holdPath()
	holdPath = nil
	reviewFtruncate(t, fs, ino, a, 3)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st != gofuse.OK {
		t.Fatalf("retry did not adopt new truncate: %v", st)
	}
	child.Lock()
	data = string(child.Dirty.Bytes())
	child.Unlock()
	if data != "helX" {
		t.Fatalf("retry appended to stale truncate: %q", data)
	}
}

func TestFtruncateOpenTruncCreatesWritableDescendant(t *testing.T) {
	fs, ino, a, _, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c); st != gofuse.OK {
		t.Fatalf("Open(O_TRUNC)=%v", st)
	}
	for i := 0; i < 2; i++ {
		if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}, []byte("new")); st != gofuse.OK || n != 3 {
			t.Fatalf("write %d=%d/%v", i, n, st)
		}
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if got := remote(); got != "new" {
		t.Fatalf("O_TRUNC restored an old tail: %q", got)
	}
}

func TestFtruncateOpenTruncRejectsPendingBranchBeforeMutation(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c); st != gofuse.EAGAIN {
		t.Fatalf("conflicting Open=%v", st)
	}
	if c.Fh != 0 {
		t.Fatal("failed Open exposed a handle")
	}
	entry, _ := fs.inodes.GetEntry(ino)
	child, _ := fs.fileHandles.Get(b)
	if entry.Size != 5 || string(child.Dirty.Bytes()) != "Hello" {
		t.Fatal("failed Open mutated accepted child")
	}
}

func TestFtruncateOpenTruncClosedSuccessorDoesNotRestoreBaseline(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st != gofuse.EAGAIN {
		t.Fatalf("used stale T while successor was pending: %v", st)
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got := remote(); got != "X" {
		t.Fatalf("restored original truncate baseline: %q", got)
	}
}

func TestFtruncateUnrelatedOpenKeepsLegacyTimeout(t *testing.T) {
	fs, ino, _, _, _ := newFtruncateCommitFS(t)
	fs.opts.RemoteCommitWaitTimeout = time.Millisecond
	unlock := fs.lockRemoteCommitPath("/file.bin")
	defer unlock()
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &out); st != gofuse.OK {
		t.Fatalf("unrelated Open changed timeout policy: %v", st)
	}
}

func TestFtruncateClosedChildBlocksOldParentMutation(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	gate := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a, Offset: 1}, []byte("X")); st != gofuse.EAGAIN {
		t.Fatalf("old parent forked closed pending child: %v", st)
	}
	parent, _ := fs.fileHandles.Get(a)
	if string(parent.Dirty.Bytes()) != "hello" {
		t.Fatal("rejected parent write changed T")
	}
	once.Do(func() { close(gate) })
	fs.commitQueue.WaitPath("/file.bin")
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a, Offset: 1}, []byte("X")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got := remote(); got != "HXllo" {
		t.Fatalf("parent did not inherit committed child: %q", got)
	}
}

func TestFtruncateKernelSplitOpenTruncRetiresParent(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	for _, id := range []uint64{a, b} {
		fh, _ := fs.fileHandles.Get(id)
		fh.OpenPID = 123
	}
	reviewFtruncate(t, fs, ino, a, 5)
	var c gofuse.OpenOut
	header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 123}}
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: uint32(syscall.O_RDWR)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: c.Fh}, []byte("new")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: c.Fh}); st != gofuse.OK {
		t.Fatal(st)
	}
	for _, id := range []uint64{a, b, c.Fh} {
		if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: id}); st != gofuse.OK {
			t.Fatalf("close flush %d: %v", id, st)
		}
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: id})
	}
	if got := remote(); got != "new" {
		t.Fatalf("remote=%q", got)
	}
}

func TestFtruncateKernelSplitOpenTruncRejectsPendingChild(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
		t.Fatalf("path zero erased pending child: %v", st)
	}
	child, _ := fs.fileHandles.Get(b)
	if string(child.Dirty.Bytes()) != "Hello" {
		t.Fatal("rejected path zero changed child bytes")
	}
}
