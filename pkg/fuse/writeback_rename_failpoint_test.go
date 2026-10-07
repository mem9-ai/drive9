//go:build failpoint

package fuse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func newOwnedRenameBoundaryFS(t *testing.T) (*Dat9FS, *WriteBackUploader, *atomic.Int32) {
	t.Helper()
	puts := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			puts.Add(1)
			w.Header().Set("X-Dat9-Revision", "1")
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	var err error
	fs.writeBack, err = NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	const path = "/old/file"
	if err := fs.shadowStore.WriteFull(path, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.pendingIndex.PutWithBaseRev(path, 7, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fs.writeBack.putHandleSnapshot(path, []byte("payload"), 7, PendingNew, 0, 0, false, "root", "", true, true, nil, 0, 1, fs.snapshotStagingGens(path), nil); err != nil {
		t.Fatal(err)
	}
	u := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", uploadCh: make(chan string, 8)}
	u.SnapshotStagingGens, u.OnDataCommitted, u.OnSuccess = fs.snapshotStagingGens, fs.onWriteBackDataCommitted, fs.onWriteBackUploadSuccess
	fs.uploader = u
	return fs, u, puts
}

func TestOwnedRenameBoundaryExcludesStaging(t *testing.T) {
	fs, u, puts := newOwnedRenameBoundaryFS(t)
	doneShadow, donePending := make(chan error, 1), make(chan error, 1)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ownedWriteBackRenameRekeyed"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, _, newPath string) {
		if observed != fs {
			return
		}
		go func() { doneShadow <- fs.shadowStore.WriteFull(newPath, []byte("newer"), 0) }()
		go func() { _, err := fs.pendingIndex.PutWithBaseRev(newPath, 5, PendingNew, 0); donePending <- err }()
		deadline := time.Now().Add(3 * time.Second)
		for {
			fs.pendingIndex.mu.RLock()
			pendingBlocked := fs.pendingIndex.pathLocks[newPath].waiters > 1
			fs.pendingIndex.mu.RUnlock()
			fs.shadowStore.mu.RLock()
			shadowBlocked := fs.shadowStore.pathLocks[newPath].waiters > 1
			fs.shadowStore.mu.RUnlock()
			if pendingBlocked && shadowBlocked {
				break
			}
			if time.Now().After(deadline) {
				t.Error("staging writers did not reach held locks")
				break
			}
			runtime.Gosched()
		}
		select {
		case <-doneShadow:
			t.Error("shadow crossed migration window")
		default:
		}
		select {
		case <-donePending:
			t.Error("pending crossed migration window")
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	if moved, err := fs.renameOwnedWriteBack("/old/file", "/new/file"); err != nil || !moved {
		t.Fatalf("rename=%t/%v", moved, err)
	}
	for _, done := range []chan error{doneShadow, donePending} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("staging deadlock")
		}
	}
	pendingGen, shadowGen := fs.pendingIndex.Generation("/new/file"), fs.shadowStore.ActiveGeneration("/new/file")
	u.uploadOne("/new/file")
	if puts.Load() != 1 || fs.pendingIndex.Generation("/new/file") != pendingGen || fs.shadowStore.ActiveGeneration("/new/file") != shadowGen {
		t.Fatal("old upload removed newer staging")
	}
}

func TestOwnedRenameBoundaryQuarantinesQueuedUpload(t *testing.T) {
	fs, u, puts := newOwnedRenameBoundaryFS(t)
	u.uploadCh <- "/new/file" // Already queued before the failing rename.
	syncDone := make(chan error, 1)
	started := make(chan struct{})
	originalDir := fs.pendingIndex.dir
	point := "github.com/mem9-ai/drive9/pkg/fuse/ownedWriteBackRenameRekeyed"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, _, newPath string) {
		if observed != fs {
			return
		}
		u.wg.Add(1)
		go u.worker()
		go func() {
			close(started)
			_, err := u.UploadSyncWithRevision(context.Background(), newPath)
			syncDone <- err
		}()
		<-started
		fs.pendingIndex.dir = filepath.Join(originalDir, "missing")
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	if _, err := fs.renameOwnedWriteBack("/old/file", "/new/file"); err == nil {
		t.Fatal("expected injected failure")
	}
	fs.pendingIndex.dir = originalDir
	u.DrainAll()
	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("sync accepted conflict")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sync deadlock")
	}
	meta, ok := fs.writeBack.GetMeta("/new/file")
	if puts.Load() != 0 || !ok || meta.Kind != PendingConflict || !fs.shadowStore.Has("/old/file") {
		t.Fatal("failed migration was consumed or lost")
	}
}

func TestReleasedFtruncateRevalidatesEvent(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	fs.commitQueue.DrainAll()
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	reader, _ := fs.fileHandles.Get(b)
	event := reader.pendingFtruncate.Load()
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, _ string) {
		if observed != fs || fh != reader {
			return
		}
		changed := *event
		changed.seq++
		fh.pendingFtruncate.Store(&changed)
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	if data, st, _ := readDat9FSTestRange(fs, ino, b, 0, 5); st != gofuse.EAGAIN {
		t.Fatalf("served invalidated event %q/%v", data, st)
	}
	reader.pendingFtruncate.Store(event)
}
