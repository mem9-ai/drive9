//go:build failpoint

package fuse

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateSiblingAttributePublicationFence(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateBeforeAttrPublish"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
		if observed == fs && fh.Ino == ino {
			close(entered)
			<-gate
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_FH | gofuse.FATTR_SIZE, Fh: a, Size: 5}}, &gofuse.AttrOut{})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("truncate did not reach publication boundary")
	}
	for _, id := range []uint64{a, b} {
		if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Offset: 8}, []byte("X")); st != gofuse.EAGAIN {
			t.Fatalf("write bypassed attribute fence: %v", st)
		}
	}
	once.Do(func() { close(gate) })
	if st := <-done; st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 8}, []byte("X")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got := remote(); got != "hello\x00\x00\x00X" {
		t.Fatalf("lost newer EOF: %q", got)
	}
}

func TestFtruncateSiblingReleaseWaitsForRollback(t *testing.T) {
	putEntered, putGate := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	fs, ino, a, b, remote := newFtruncateCommitFS(t, func(w http.ResponseWriter, r *http.Request) bool {
		if first.CompareAndSwap(false, true) {
			close(putEntered)
			<-putGate
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		return false
	})
	var gateOnce sync.Once
	t.Cleanup(func() { gateOnce.Do(func() { close(putGate) }) })
	reviewFtruncate(t, fs, ino, a, 5)
	child, _ := fs.fileHandles.Get(b)
	child.WritePolicy = WritePolicyWriteSync
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
		writeDone <- st
	}()
	select {
	case <-putEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not reach upload")
	}
	waiting := make(chan struct{})
	var waitOnce sync.Once
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseWait"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
		if observed == fs && fh.Ino == ino {
			waitOnce.Do(func() { close(waiting) })
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	releaseDone := make(chan struct{})
	go func() {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
		close(releaseDone)
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("Release did not preserve parent while child was busy")
	}
	gateOnce.Do(func() { close(putGate) })
	if st := <-writeDone; st == gofuse.OK {
		t.Fatal("injected failed write succeeded")
	}
	select {
	case <-releaseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Release failed to resume after rollback")
	}
	fs.commitQueue.WaitPath("/file.bin")
	if got := remote(); got != "hello" {
		t.Fatalf("rollback plus parent close lost truncate: %q", got)
	}
}
