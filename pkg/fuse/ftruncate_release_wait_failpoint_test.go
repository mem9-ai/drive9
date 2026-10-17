//go:build failpoint

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestFtruncateReleaseSpillRelockDeadline(t *testing.T) {
	for _, boundary := range []string{"ftruncateStreamCommitBeforeRelock", "ftruncateModeBeforeRelock"} {
		t.Run(boundary, func(t *testing.T) {
			fs, ino, a, remote := newFtruncateReleaseSpill(t)
			fs.opts.LegacyInterruptibleMutations = true
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
					w.WriteHeader(http.StatusNoContent)
					return true
				}
				return false
			})
			if boundary == "ftruncateModeBeforeRelock" {
				if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					InHeader: gofuse.InHeader{NodeId: ino}, Fh: a, Valid: gofuse.FATTR_MODE, Mode: 0o600,
				}}, &gofuse.AttrOut{}); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			owner, _ := fs.fileHandles.Get(a)
			entered, resume, cancel, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			interrupt := sync.OnceFunc(func() { close(cancel) })
			var once sync.Once
			var unlock func()
			point := "github.com/mem9-ai/drive9/pkg/fuse/" + boundary
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, h *FileHandle) {
				if observed == fs && h == owner {
					once.Do(func() { close(entered); <-resume })
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if unlock != nil {
					unlock()
				}
				interrupt()
				unblock()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("Release stuck")
				}
				_ = failpoint.Disable(point)
			})
			go func() {
				fs.Release(cancel, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("fallback missed boundary")
			}
			owner.Lock()
			unlock = sync.OnceFunc(owner.Unlock)
			interrupt()
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("fallback ignored cancellation")
			}
			if _, ok := fs.fileHandles.Get(a); !ok {
				t.Fatal("fallback lost owner")
			}
			if boundary == "ftruncateModeBeforeRelock" && !owner.HasPendingMode {
				t.Fatal("lost mode generation")
			}
			unlock()
			if release, ok := fs.tryLockRemoteCommitPath("/file.bin"); !ok {
				t.Fatal("fallback leaked path fence")
			} else {
				release()
			}
			_, revision, _ := remote.snapshot()
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			if response := fs.Drain(ctx); !response.OK {
				t.Fatalf("recovery=%+v", response)
			}
			if _, ok := fs.fileHandles.Get(a); ok {
				t.Fatal("recovery did not finalize owner")
			}
			if data, next, _ := remote.snapshot(); len(data) != releaseSpillTestSize || next != revision {
				t.Fatal("fallback recovery republished or lost data")
			}
		})
	}
}

func TestFtruncateModeWaitDoesNotReplayCompletedGeneration(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 5)
	var mu sync.Mutex
	modeCalls := 0
	interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
			mu.Lock()
			modeCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return true
		}
		return false
	})
	setMode := func(mode uint32) {
		if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Valid: gofuse.FATTR_MODE, Mode: mode,
		}}, &gofuse.AttrOut{}); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	setMode(0o600)
	owner, _ := fs.fileHandles.Get(a)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	unblock := sync.OnceFunc(func() { close(resume) })
	var once sync.Once
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateModeWait"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, h *FileHandle) {
		if observed == fs && h == owner {
			once.Do(func() { close(entered); <-resume })
		}
	}); err != nil {
		t.Fatal(err)
	}
	releaseFence := sync.OnceFunc(fs.lockRemoteCommitPath("/file.bin"))
	t.Cleanup(func() { releaseFence(); unblock(); _ = failpoint.Disable(point) })
	go func() {
		owner.Lock()
		state := &handleLockState{held: true, participating: true, deadline: time.Now().Add(5 * time.Second)}
		err := fs.applyPendingModeBudgetedLocked(t.Context(), owner, state)
		if state.held {
			owner.Unlock()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("mode did not yield")
	}
	releaseFence()
	hardlinkSync(t, fs, ino, b)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	b = truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	setMode(0o644)
	hardlinkSync(t, fs, ino, b)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	owner.Lock()
	stillPending := owner.HasPendingMode
	owner.Unlock()
	if stillPending {
		t.Fatal("new generation was not fully completed")
	}
	mu.Lock()
	before := modeCalls
	mu.Unlock()
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("old waiter stuck")
	}
	mu.Lock()
	after := modeCalls
	mu.Unlock()
	if before < 2 || after != before {
		t.Fatalf("old waiter replayed mode: before=%d after=%d", before, after)
	}
	// The source still owns the uncommitted truncate; clean sibling fsyncs
	// above complete their metadata without committing the source's bytes.
	retiredReadWant(t, fs, ino, a, "hello")
	hardlinkSync(t, fs, ino, a)
	if data, _, _ := remote.snapshot(); string(data) != "hello" {
		t.Fatal("data changed")
	}
}

func TestFtruncateReleasePreservesUploadBudget(t *testing.T) {
	fs, ino, _ := truncateHandoffFS(t, []byte("hello world"))
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 5)
	owner, _ := fs.fileHandles.Get(a)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseBudget"
	var remaining time.Duration
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, h *FileHandle, ctx context.Context) {
		if observed == fs && h == owner {
			deadline, _ := ctx.Deadline()
			remaining = time.Until(deadline)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	if remaining < 55*time.Second || remaining > releaseTimeout(5) {
		t.Fatalf("upload budget=%s", remaining)
	}
}

func TestFtruncateRetiredWriterQueueAdmissionDoesNotCancel(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	fs.opts.RemoteCommitWaitTimeout = 50 * time.Millisecond
	a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	_, revision, _ := remote.snapshot()
	if _, err := fs.client.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("hello"), revision); err != nil {
		t.Fatal(err)
	}
	(&SSEWatcher{fs: fs, actor: "local"}).handleEvent(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"}, nil)
	retiredReadWant(t, fs, ino, b, "hello")
	if truncateHandoffEvent(fs, b) != nil || truncateHandoffEvent(fs, a) == nil {
		t.Fatal("independent retirement prerequisite missing")
	}
	hardlinkWrite(t, fs, ino, a, "H")
	owner, _ := fs.fileHandles.Get(a)
	writer, _ := fs.fileHandles.Get(b)
	beforeStage, stageGate := make(chan struct{}), make(chan struct{})
	beforeLock, writerGate := make(chan struct{}), make(chan struct{})
	queued, queueGate := make(chan struct{}), make(chan struct{})
	stage, write, upload := sync.OnceFunc(func() { close(stageGate) }), sync.OnceFunc(func() { close(writerGate) }), sync.OnceFunc(func() { close(queueGate) })
	var stagedOnce, writerOnce, queuedOnce sync.Once
	stagePoint := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReleaseBeforeStage"
	writePoint := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateWriteAdmission"
	if err := failpoint.EnableCall(stagePoint, func(observed *Dat9FS, h *FileHandle) {
		if observed == fs && h == owner {
			stagedOnce.Do(func() { close(beforeStage); <-stageGate })
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := failpoint.EnableCall(writePoint, func(observed *Dat9FS, h *FileHandle) {
		if observed == fs && h == writer {
			writerOnce.Do(func() { close(beforeLock); <-writerGate })
		}
	}); err != nil {
		t.Fatal(err)
	}
	fs.commitQueue.PathLock = func(path string) func() {
		queuedOnce.Do(func() { close(queued) })
		<-queueGate
		return fs.lockRemoteCommitPath(path)
	}
	released, written := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		stage()
		write()
		upload()
		select {
		case <-released:
		case <-time.After(3 * time.Second):
			t.Error("Release stuck")
		}
		select {
		case <-written:
		case <-time.After(3 * time.Second):
			t.Error("Write stuck")
		}
		_ = failpoint.Disable(stagePoint)
		_ = failpoint.Disable(writePoint)
	})
	go func() {
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
		close(released)
	}()
	select {
	case <-beforeStage:
	case <-time.After(3 * time.Second):
		t.Fatal("Release did not reach staging boundary")
	}
	var n uint32
	var status gofuse.Status
	go func() {
		n, status = fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("B"))
		close(written)
	}()
	select {
	case <-beforeLock:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not pass the empty queue check")
	}
	stage()
	select {
	case <-released:
	case <-time.After(3 * time.Second):
		t.Fatal("Release waited on the paused writer")
	}
	<-queued
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	if !ok {
		t.Fatal("queued durable image missing")
	}
	write()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("queued writer exceeded its budget")
	}
	if n != 0 || status != gofuse.EAGAIN {
		t.Fatalf("blocked write=%d/%v", n, status)
	}
	if !fs.commitQueue.hasFtruncateInode(ino, fs.ftruncateAliasPaths(ino)) || fs.pendingIndex.Generation("/file.bin") != meta.Generation {
		t.Fatal("writer canceled or consumed its sibling's queued image")
	}
	upload()
	fs.commitQueue.WaitPath("/file.bin")
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: 1}, []byte("J")); n != 1 || st != gofuse.OK {
		t.Fatalf("retry=%d/%v", n, st)
	}
	hardlinkSync(t, fs, ino, b)
	if data, _, _ := remote.snapshot(); string(data) != "HJllo" {
		t.Fatalf("accepted sibling bytes lost: %q", data)
	}
}

func TestFtruncateReleaseUploadRelockDeadline(t *testing.T) {
	for _, size := range []int{64 << 10, 9 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), size+4096))
			fs.opts.LegacyInterruptibleMutations = true
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, size)
			queue := fs.commitQueue
			fs.commitQueue = nil
			t.Cleanup(func() { fs.commitQueue = queue })
			owner, _ := fs.fileHandles.Get(a)
			entered, resume, cancel, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			interrupt := sync.OnceFunc(func() { close(cancel) })
			var once sync.Once
			var unlock func()
			points := []string{"retiredFtruncateUploadBeforeRelock", "ftruncateStreamCommitBeforeRelock"}
			for _, point := range points {
				if err := failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/fuse/"+point, func(observed *Dat9FS, fh *FileHandle) {
					if observed == fs && fh == owner {
						once.Do(func() { close(entered); <-resume })
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				if unlock != nil {
					unlock()
				}
				interrupt()
				unblock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("Release did not finish")
				}
				for _, point := range points {
					_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/fuse/" + point)
				}
			})
			go func() {
				fs.Release(cancel, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not reach its relock window")
			}
			owner.Lock()
			unlock = sync.OnceFunc(owner.Unlock)
			interrupt()
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Release ignored cancellation while reacquiring mu")
			}
			if current, exists := fs.fileHandles.Get(a); !exists || current != owner {
				t.Fatal("unlocked return lost the owner")
			}
			if owner.Dirty == nil || owner.Dirty.Size() != int64(size) {
				t.Fatal("unlocked return destroyed the image")
			}
			if fs.snapshotDrainPending().DirtyHandles == 0 {
				t.Fatal("locked retained owner was reported clean")
			}
			unlock()
			before, revision, _ := remote.snapshot()
			if len(before) != size {
				t.Fatal("upload ACK prerequisite failed")
			}
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			if response := fs.Drain(ctx); !response.OK {
				t.Fatalf("retry drain=%+v", response)
			}
			if _, exists := fs.fileHandles.Get(a); exists {
				t.Fatal("drained owner not finalized")
			}
			after, nextRevision, _ := remote.snapshot()
			if !bytes.Equal(before, after) || nextRevision != revision {
				t.Fatal("completed image was lost or needlessly republished")
			}
		})
	}
}
