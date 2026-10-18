package fuse

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

const releaseReceiptTestSize = 2 << 20

// Keep the normal queue installed while forcing a real successful direct
// upload to lose its handle relock, leaving Release's exact completion owner.
func newRetainedReleaseReceipt(t *testing.T, prepare func(*Dat9FS, uint64, uint64)) (*Dat9FS, uint64, *FileHandle, *truncateHandoffRemote) {
	t.Helper()
	fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), releaseReceiptTestSize+4096))
	fs.opts.LegacyInterruptibleMutations = true
	id := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, id, releaseReceiptTestSize)
	if prepare != nil {
		prepare(fs, ino, id)
	}
	owner, _ := fs.fileHandles.Get(id)
	entered, resume, cancel, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	interrupt := sync.OnceFunc(func() { close(cancel) })
	var once sync.Once
	var unlock func()
	testHookBeforeFlushRelock = func(path string) {
		if path == "/file.bin" {
			once.Do(func() { close(entered); <-resume })
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
		testHookBeforeFlushRelock = nil
	})
	go func() {
		fs.Release(cancel, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload missed post-ACK relock boundary")
	}
	owner.Lock()
	unlock = sync.OnceFunc(owner.Unlock)
	interrupt()
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Release ignored cancellation while reacquiring mu")
	}
	if current, exists := fs.fileHandles.Get(id); !exists || current != owner || owner.deferredRelease.Load() != id {
		t.Fatal("Release lost its unfinished owner")
	}
	r := owner.releaseCommit
	if r == nil || r.revision.Load() != 2 || r.view != fs.mountViewGeneration.Load() || owner.BaseRev != 1 || owner.Dirty.Size() != releaseReceiptTestSize {
		t.Fatal("completed receipt prerequisite failed")
	}
	unlock()
	testHookBeforeFlushRelock = nil
	wantReleaseReceiptRemote(t, remote)
	if _, exists := fs.pendingIndex.GetMeta("/file.bin"); exists || fs.commitQueue == nil {
		t.Fatal("expected completed direct upload with normal queue and no pending metadata")
	}
	return fs, id, owner, remote
}

func wantReleaseReceiptRemote(t *testing.T, remote *truncateHandoffRemote) {
	t.Helper()
	data, revision, puts := remote.snapshot()
	if !bytes.Equal(data, bytes.Repeat([]byte("p"), releaseReceiptTestSize)) || revision != 2 || puts != 1 {
		t.Fatalf("completed image changed: size=%d revision=%d uploads=%d", len(data), revision, puts)
	}
}

func wantReleaseReceiptDrained(t *testing.T, fs *Dat9FS, id uint64, remote *truncateHandoffRemote) {
	t.Helper()
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		response := fs.Drain(ctx)
		cancel()
		if !response.OK {
			t.Fatalf("drain=%+v", response)
		}
	}
	if _, exists := fs.fileHandles.Get(id); exists {
		t.Fatal("completed owner was not finalized")
	}
	if _, exists := fs.pendingIndex.GetMeta("/file.bin"); exists {
		t.Fatal("completed owner left pending metadata")
	}
	wantReleaseReceiptRemote(t, remote)
}

func TestReleaseCommitResetRecovery(t *testing.T) {
	for _, mode := range []string{"no-reset", "reset", "hardlink-reset"} {
		t.Run(mode, func(t *testing.T) {
			var armed atomic.Bool
			var heads atomic.Int32
			fs, id, owner, remote := newRetainedReleaseReceipt(t, func(fs *Dat9FS, _, _ uint64) {
				interceptRetiredReadHTTP(t, fs, func(_ http.ResponseWriter, r *http.Request) bool {
					if armed.Load() && r.Method == http.MethodHead {
						heads.Add(1)
					}
					return false
				})
			})
			if mode == "hardlink-reset" {
				addTruncateHandoffAlias(t, fs, owner.Ino, remote)
			}
			armed.Store(true)
			if mode != "no-reset" {
				(&SSEWatcher{fs: fs}).handleReset()
			}
			wantReleaseReceiptDrained(t, fs, id, remote)
			if (mode == "no-reset" && heads.Load() != 0) || (mode != "no-reset" && heads.Load() == 0) {
				t.Fatalf("unexpected verification count: %d", heads.Load())
			}
		})
	}
}

func TestReleaseCommitResetVerificationRetry(t *testing.T) {
	for _, mode := range []string{"resource", "revision", "size", "directory", "http-error", "reset-during-stat", "invalidate-during-stat", "cancel"} {
		for _, pendingMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/mode=%t", mode, pendingMode), func(t *testing.T) {
				var reject atomic.Bool
				var modeCalls atomic.Int32
				base, stop := context.WithCancel(t.Context())
				defer stop()
				fs, id, owner, remote := newRetainedReleaseReceipt(t, func(fs *Dat9FS, ino, id uint64) {
					interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
						if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
							modeCalls.Add(1)
							w.WriteHeader(http.StatusNoContent)
							return true
						}
						if !reject.Load() || r.Method != http.MethodHead || r.URL.Path != "/v1/fs/file.bin" {
							return false
						}
						switch mode {
						case "http-error":
							w.WriteHeader(http.StatusForbidden)
							return true
						case "reset-during-stat":
							(&SSEWatcher{fs: fs}).handleReset()
							return false
						case "invalidate-during-stat":
							(&SSEWatcher{fs: fs}).handleChange(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"})
							return false
						case "cancel":
							stop()
							return false
						}
						w.Header().Set("X-Dat9-Resource-ID", "handoff-resource")
						w.Header().Set("X-Dat9-Revision", "2")
						w.Header().Set("Content-Length", strconv.Itoa(releaseReceiptTestSize))
						w.Header().Set("X-Dat9-IsDir", "false")
						switch mode {
						case "resource":
							w.Header().Set("X-Dat9-Resource-ID", "replacement")
						case "revision":
							w.Header().Set("X-Dat9-Revision", "3")
						case "size":
							w.Header().Set("Content-Length", "1")
						case "directory":
							w.Header().Set("X-Dat9-IsDir", "true")
						}
						return true
					})
					if pendingMode {
						if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
							InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Valid: gofuse.FATTR_MODE, Mode: 0o600,
						}}, &gofuse.AttrOut{}); st != gofuse.OK {
							t.Fatal(st)
						}
					}
				})
				(&SSEWatcher{fs: fs}).handleReset()
				reject.Store(true)
				ctx, cancel := context.WithTimeout(base, 5*time.Second)
				response := fs.Drain(ctx)
				cancel()
				if response.OK {
					t.Fatal("unverified completion was accepted")
				}
				if current, exists := fs.fileHandles.Get(id); !exists || current != owner {
					t.Fatal("verification failure lost the owner")
				}
				owner.Lock()
				retained := owner.releaseCommit != nil && owner.releaseCommit.revision.Load() == 2 && owner.BaseRev == 1 && owner.Dirty.HasDirtyParts() && owner.HasPendingMode == pendingMode
				owner.Unlock()
				if !retained || modeCalls.Load() != 0 {
					t.Fatal("verification failure changed the completed image")
				}
				if _, exists := fs.pendingIndex.GetMeta("/file.bin"); exists {
					t.Fatal("verification failure manufactured pending/conflict metadata")
				}
				wantReleaseReceiptRemote(t, remote)
				reject.Store(false)
				wantReleaseReceiptDrained(t, fs, id, remote)
			})
		}
	}
}

func TestReleaseCommitViewLockKeepsDrainBudget(t *testing.T) {
	fs, id, owner, remote := newRetainedReleaseReceipt(t, nil)
	fs.mountViewMu.Lock()
	unlock := sync.OnceFunc(fs.mountViewMu.Unlock)
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- fs.Drain(ctx).OK }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("view-lock contention escaped the original drain budget")
		}
	case <-time.After(5 * time.Second):
		unlock()
		<-done
		t.Fatal("view-lock wait ignored deadline")
	}
	if current, exists := fs.fileHandles.Get(id); !exists || current != owner {
		t.Fatal("view-lock timeout lost owner")
	}
	if _, exists := fs.pendingIndex.GetMeta("/file.bin"); exists {
		t.Fatal("view-lock timeout manufactured pending metadata")
	}
	unlock()
	wantReleaseReceiptDrained(t, fs, id, remote)
}

func TestReleaseCommitResetPreservesPendingMode(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	var calls atomic.Int32
	fs, id, owner, remote := newRetainedReleaseReceipt(t, func(fs *Dat9FS, ino, id uint64) {
		interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
				calls.Add(1)
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
			InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Valid: gofuse.FATTR_MODE, Mode: 0o600,
		}}, &gofuse.AttrOut{}); st != gofuse.OK {
			t.Fatal(st)
		}
	})
	(&SSEWatcher{fs: fs}).handleReset()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	response := fs.Drain(ctx)
	cancel()
	if response.OK || calls.Load() == 0 {
		t.Fatal("failed metadata obligation was not attempted/retained")
	}
	owner.Lock()
	pending := owner.HasPendingMode && owner.releaseCommit != nil
	owner.Unlock()
	if _, exists := fs.fileHandles.Get(id); !exists || !pending {
		t.Fatal("failed chmod lost completed payload or pending mode")
	}
	if _, exists := fs.pendingIndex.GetMeta("/file.bin"); exists {
		t.Fatal("failed chmod manufactured conflict")
	}
	wantReleaseReceiptRemote(t, remote)
	reject.Store(false)
	wantReleaseReceiptDrained(t, fs, id, remote)
}

func TestReleaseCommitResetPreservesSuccessorStaging(t *testing.T) {
	fs, _, owner, remote := newRetainedReleaseReceipt(t, nil)
	(&SSEWatcher{fs: fs}).handleReset()
	newData := []byte("successor")
	if err := fs.shadowStore.WriteFull("/file.bin", newData, 2); err != nil {
		t.Fatal(err)
	}
	shadow := fs.shadowStore.ActiveGeneration("/file.bin")
	pending, err := fs.pendingIndex.PutWithBaseRev("/file.bin", int64(len(newData)), PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}
	owner.Lock()
	handled, err := fs.consumeReleaseCommitLocked(t.Context(), owner)
	owner.Unlock()
	if !handled || err != nil {
		t.Fatalf("completion=%t/%v", handled, err)
	}
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	data, err := fs.shadowStore.ReadAllIfGeneration("/file.bin", shadow)
	if !ok || meta.Generation != pending || meta.Kind != PendingOverwrite || err != nil || !bytes.Equal(data, newData) {
		t.Fatal("old receipt cleaned successor staging")
	}
	wantReleaseReceiptRemote(t, remote)
}

func TestReleaseCommitResetRejectsChangedOwner(t *testing.T) {
	for _, changed := range []string{"image", "staging"} {
		t.Run(changed, func(t *testing.T) {
			fs, _, owner, remote := newRetainedReleaseReceipt(t, nil)
			(&SSEWatcher{fs: fs}).handleReset()
			owner.Lock()
			defer owner.Unlock()
			if changed == "image" {
				if _, err := owner.Dirty.Write(0, []byte("H")); err != nil {
					t.Fatal(err)
				}
			} else if err := fs.stageShadowLocked(owner, true); err != nil {
				t.Fatal(err)
			}
			gens, seq, version := fs.captureHandleStagingGensLocked(owner), owner.DirtySeq, owner.Dirty.contentVersion
			handled, err := fs.consumeReleaseCommitLocked(t.Context(), owner)
			if (handled && err == nil) || !owner.Dirty.HasDirtyParts() || owner.DirtySeq != seq || owner.Dirty.contentVersion != version || fs.captureHandleStagingGensLocked(owner) != gens {
				t.Fatal("receipt consumed or changed a different owner image/staging")
			}
			wantReleaseReceiptRemote(t, remote)
		})
	}
}

func TestReleaseCommitResetAfterStagingCleanup(t *testing.T) {
	fs, ino, id, remote := newFtruncateReleaseSpill(t)
	addTruncateHandoffAlias(t, fs, ino, remote)
	owner, _ := fs.fileHandles.Get(id)
	var once sync.Once
	verified := false
	testHookNotifyInodeSync = func(observed uint64) {
		if observed != ino {
			return
		}
		once.Do(func() {
			r := owner.releaseCommit
			if r == nil || r.revision.Load() != 2 || r.gens == (StagingGens{}) ||
				owner.Dirty != r.buffer || owner.Dirty.contentVersion != r.version ||
				owner.DirtySeq != 0 || owner.Dirty.HasDirtyParts() {
				t.Error("normal shadow completion boundary prerequisite failed")
				return
			}
			verified = true
			(&SSEWatcher{fs: fs}).handleReset()
		})
	}
	t.Cleanup(func() { testHookNotifyInodeSync = nil })
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	if !verified {
		t.Fatal("normal shadow completion did not reach notification boundary")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if response := fs.Drain(ctx); !response.OK {
		t.Fatalf("clean completion after staging retirement: %+v", response)
	}
	if _, exists := fs.fileHandles.Get(id); exists {
		t.Fatal("clean completed owner was not finalized")
	}
	data, revision, puts := remote.snapshot()
	if !bytes.Equal(data, bytes.Repeat([]byte("p"), releaseSpillTestSize)) || revision != 2 || puts != 1 {
		t.Fatal("clean completion republished or changed remote data")
	}
}
