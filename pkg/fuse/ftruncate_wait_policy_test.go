package fuse

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateReleaseRetainsFailedMode(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	_ = truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 5)
	queue := fs.commitQueue
	fs.commitQueue = nil
	t.Cleanup(func() { fs.commitQueue = queue })
	var reject atomic.Bool
	reject.Store(true)
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
			if reject.Load() {
				w.WriteHeader(http.StatusForbidden)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return true
		}
		return false
	})
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino}, Fh: a, Valid: gofuse.FATTR_MODE, Mode: 0o600,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	h, ok := fs.fileHandles.Get(a)
	if !ok {
		t.Fatal("chmod failure dropped the closed owner")
	}
	h.Lock()
	modePending := h.HasPendingMode && h.PendingMode == 0o600
	h.Unlock()
	if !modePending {
		t.Fatal("chmod failure discarded or rolled back pending mode")
	}
	data, committedRevision, _ := remote.snapshot()
	if string(data) != "hello" {
		t.Fatal("content ACK prerequisite missing")
	}
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	if response := fs.Drain(ctx); response.OK {
		t.Fatal("failed mode was reported drained")
	}
	if _, ok := fs.fileHandles.Get(a); !ok {
		t.Fatal("failed retry dropped owner")
	}
	reject.Store(false)
	ctx2, stop2 := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop2()
	if response := fs.Drain(ctx2); !response.OK {
		t.Fatalf("mode retry=%+v", response)
	}
	if _, ok := fs.fileHandles.Get(a); ok {
		t.Fatal("successful mode retry did not finalize owner")
	}
	if _, revision, _ := remote.snapshot(); revision != committedRevision {
		t.Fatal("mode recovery republished already acknowledged content")
	}
}

func TestFtruncateConcurrentReleaseAndDrain(t *testing.T) {
	var puts atomic.Int32
	fs, ino, a, _, remote := newFtruncateCommitFS(t, func(http.ResponseWriter, *http.Request) bool { puts.Add(1); return false })
	reviewFtruncate(t, fs, ino, a, 5)
	start, done := make(chan struct{}), make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_ = fs.Drain(ctx)
	}()
	go func() { workers.Wait(); close(done) }()
	close(start)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("competing finalizers did not finish")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if response := fs.Drain(ctx); !response.OK {
		t.Fatalf("final Drain=%+v", response)
	}
	if _, ok := fs.fileHandles.Get(a); ok {
		t.Fatal("closed owner remains registered")
	}
	if remote() != "hello" || puts.Load() != 1 {
		t.Fatalf("content=%q commits=%d", remote(), puts.Load())
	}
}

func TestFtruncateSinglePathFlushWaits(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	sibling, _ := fs.fileHandles.Get(b)
	sibling.Lock()
	unlock := sync.OnceFunc(sibling.Unlock)
	t.Cleanup(unlock)
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}) }()
	select {
	case st := <-done:
		t.Fatalf("transient sibling lock failed Flush immediately: %v", st)
	case <-time.After(25 * time.Millisecond):
	}
	unlock()
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatal(st)
		}
	case <-time.After(time.Second):
		t.Fatal("Flush did not resume after contention ended")
	}
	if remote() != "hello world" {
		t.Fatal("writeback Flush became a remote commit")
	}
}

func TestFtruncateQueuedWriteWaits(t *testing.T) {
	for _, timeout := range []time.Duration{500 * time.Millisecond, 0, -time.Second} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			fs.opts.RemoteCommitWaitTimeout = timeout
			reviewFtruncate(t, fs, ino, a, 5)
			entered, gate := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(gate) })
			var once sync.Once
			t.Cleanup(unblock)
			fs.commitQueue.PathLock = func(p string) func() {
				once.Do(func() { close(entered) })
				<-gate
				return fs.lockRemoteCommitPath(p)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			<-entered
			done := make(chan gofuse.Status, 1)
			go func() {
				n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H"))
				if st == gofuse.OK && n != 1 {
					t.Errorf("successful Write accepted %d bytes", n)
				}
				done <- st
			}()
			select {
			case st := <-done:
				t.Fatalf("queued truncate failed Write immediately: %v", st)
			case <-time.After(25 * time.Millisecond):
			}
			unblock()
			select {
			case st := <-done:
				if st != gofuse.OK {
					t.Fatal(st)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Write did not resume after queued truncate")
			}
			hardlinkSync(t, fs, ino, b)
			if remote() != "Hello" {
				t.Fatalf("remote=%q", remote())
			}
		})
	}
}

func TestFtruncateReleaseCancellationRetainsOwner(t *testing.T) {
	fs, ino, a, b, remote := newFtruncateCommitFS(t)
	fs.opts.LegacyInterruptibleMutations = true
	reviewFtruncate(t, fs, ino, a, 5)
	sibling, _ := fs.fileHandles.Get(b)
	sibling.Lock()
	unlock := sync.OnceFunc(sibling.Unlock)
	t.Cleanup(unlock)
	cancel := make(chan struct{})
	close(cancel)
	done := make(chan struct{})
	go func() {
		fs.Release(cancel, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		unlock()
		<-done
		t.Fatal("Release busy loop ignored cancellation")
	}
	owner, ok := fs.fileHandles.Get(a)
	if !ok {
		t.Fatal("failed Release discarded the only accepted image")
	}
	if pending := fs.snapshotDrainPending(); pending.DirtyHandles == 0 {
		t.Fatal("retained owner omitted from drain status")
	}
	unlock()
	owner.Lock()
	if owner.Dirty == nil || owner.Dirty.Size() != 5 || !owner.Dirty.HasDirtyParts() {
		t.Error("accepted image was destroyed")
	}
	owner.Unlock()
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	if response := fs.Drain(ctx); !response.OK {
		t.Fatalf("retained Drain=%+v", response)
	}
	if _, ok := fs.fileHandles.Get(a); ok {
		t.Fatal("completed retained owner was not finalized")
	}
	if remote() != "hello" {
		t.Fatalf("retained bytes not committed: %q", remote())
	}
}
