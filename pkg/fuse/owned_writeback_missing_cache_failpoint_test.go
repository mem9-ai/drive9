//go:build failpoint

package fuse

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestOwnedWriteBackMissingCachePendingMigration(t *testing.T) {
	for _, mode := range []string{"success", "prepare_failure", "shadow_failure"} {
		t.Run(mode, func(t *testing.T) { runOwnedWriteBackMissingCachePendingMigration(t, mode) })
	}
}

func runOwnedWriteBackMissingCachePendingMigration(t *testing.T, mode string) {
	t.Helper()
	const oldDir, newDir = "/old", "/new"
	const oldPath, newPath = oldDir + "/file.bin", newDir + "/file.bin"
	var mu sync.Mutex
	var remotePath string
	var remoteData []byte
	var revision int64
	var renamed bool
	accepted, resume, uploadDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	fs := newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/fs"), "/")
		if p == "" {
			p = "/"
		}
		if r.Method == http.MethodPut {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			if r.Header.Get("X-Dat9-Expected-Revision") != fmt.Sprint(revision) || remotePath != "" && p != remotePath {
				mu.Unlock()
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			remotePath, remoteData, revision = p, append([]byte(nil), body...), revision+1
			committed := revision
			first := committed == 1
			mu.Unlock()
			if first {
				close(accepted)
				<-resume
			}
			_, _ = fmt.Fprintf(w, `{"revision":%d}`, committed)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if p == "/" || p == oldDir && !renamed || p == newDir && renamed {
				w.Header().Set("X-Dat9-IsDir", "true")
				w.Header().Set("Content-Length", "0")
				return
			}
			if p != remotePath || remotePath == "" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Resource-ID", "late-created-child")
			w.Header().Set("X-Dat9-Revision", fmt.Sprint(revision))
			w.Header().Set("Content-Length", fmt.Sprint(len(remoteData)))
		case http.MethodGet:
			if r.URL.Query().Has("list") {
				_, _ = io.WriteString(w, `{"entries":[]}`)
				return
			}
			if p != remotePath {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(remoteData)
		case http.MethodPost:
			if p != newDir || r.Header.Get("X-Dat9-Rename-Source") != oldDir || remotePath != oldPath {
				t.Errorf("unexpected rename p=%s source=%s child=%s", p, r.Header.Get("X-Dat9-Rename-Source"), remotePath)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			renamed, remotePath = true, newPath
		case http.MethodPatch:
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
		}
	}, false)
	fs.client.SetSmallFileThresholdForTests(64 << 20)
	fs.syncMode = SyncStrict
	dir := fs.inodes.Lookup(oldDir, true, 0, time.Now())
	var opened gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: dir}, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: defaultRegularFileMode}, "file.bin", &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	fh, _ := fs.fileHandles.Get(opened.Fh)
	t.Cleanup(func() {
		unblock()
		fh.Lock()
		fs.releaseHandleRemoteCommitPathLocked(fh)
		fh.Unlock()
	})
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: opened.NodeId}, Fh: opened.Fh}, []byte("first")); st != gofuse.OK {
		t.Fatal(st)
	}
	var firstGeneration uint64
	var later *WriteBackMeta
	large := bytes.Repeat([]byte("B"), writeBackThreshold+1)
	phase := "github.com/mem9-ai/drive9/pkg/fuse/ownedDirectoryRenamePhase"
	if err := failpoint.EnableCall(phase, func(observed *Dat9FS, at string) {
		if observed != fs || at != "before_remote" {
			return
		}
		if fs.pendingIndex.HasPending(oldPath) {
			t.Fatal("child already published before late-publication boundary")
		}
		if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: opened.NodeId}, Fh: opened.Fh}); st != gofuse.OK {
			t.Fatal(st)
		}
		meta, ok := fs.writeBack.GetMeta(oldPath)
		if !ok || !meta.ownedStagingKnown {
			t.Fatal("Flush did not publish an owned writeback snapshot")
		}
		firstGeneration = meta.Generation
		go func() { fs.uploader.uploadOne(oldPath); close(uploadDone) }()
		select {
		case <-accepted:
		case <-time.After(5 * time.Second):
			t.Fatal("first upload did not commit before rename")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(phase) })
	point := "github.com/mem9-ai/drive9/pkg/fuse/ownedWriteBackRenameBeforeAcquire"
	listed := false
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, old, next string) {
		if observed != fs || old != oldPath || next != newPath {
			return
		}
		listed = true
		if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: opened.NodeId}, Fh: opened.Fh}, large); st != gofuse.OK {
			t.Fatal(st)
		}
		if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: opened.NodeId}, Fh: opened.Fh}); st != gofuse.OK {
			t.Fatal(st)
		}
		cached, ok := fs.writeBack.GetMeta(oldPath)
		if !ok || cached.Generation != firstGeneration || !fh.ShadowCommitReady {
			t.Fatal("large shadow-spill Flush unexpectedly replaced small writeback snapshot")
		}
		later, ok = fs.pendingIndex.GetMeta(oldPath)
		if !ok || later.Size != int64(len(large)) {
			t.Fatal("large Flush did not publish its newer pending generation")
		}
		unblock()
		select {
		case <-uploadDone:
		case <-time.After(5 * time.Second):
			t.Fatal("old upload did not finish")
		}
		if _, exists := fs.writeBack.GetMeta(oldPath); exists {
			t.Fatal("actual uploader did not remove its old cache generation")
		}
		current, ok := fs.pendingIndex.GetMeta(oldPath)
		if !ok || current.Generation != later.Generation || current.Kind == PendingConflict {
			t.Fatal("old upload did not preserve newer pending generation")
		}
		t.Logf("REACHABLE listed_cache=%d removed_by_upload=true surviving_pending=%d size=%d", firstGeneration, later.Generation, later.Size)
		if mode != "success" {
			blocker := filepath.Join(fs.pendingIndex.dir, hashPath(newPath)+".meta")
			if mode == "shadow_failure" {
				blocker = fs.shadowStore.shadowPath(newPath)
			}
			if err := os.Mkdir(blocker, 0755); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Remove(blocker) })
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	status := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
	if !listed || later == nil {
		t.Fatal("rename did not reach listed owned cache race")
	}
	if fh.Path != newPath {
		t.Fatalf("handle not reconciled after remote rename: %s", fh.Path)
	}
	if mode != "success" {
		if status != gofuse.EIO {
			t.Fatalf("migration failure returned %v, want EIO", status)
		}
		retained, ok := fs.pendingIndex.GetMeta(oldPath)
		if !ok || retained.Generation != later.Generation || retained.Kind != PendingConflict || retained.SnapshotID != later.SnapshotID || retained.Size != later.Size {
			t.Fatalf("failed migration left recoverable old path: %+v", retained)
		}
		if _, ok := fs.pendingIndex.GetMeta(newPath); ok {
			t.Fatal("failed migration published target pending metadata")
		}
		data, err := fs.shadowStore.ReadAll(oldPath)
		if err != nil || !bytes.Equal(data, large) {
			t.Fatalf("accepted late bytes not preserved: size=%d error=%v", len(data), err)
		}
		return
	}
	if status != gofuse.OK {
		t.Fatal(status)
	}
	if old, ok := fs.pendingIndex.GetMeta(oldPath); ok {
		t.Fatalf("surviving pending left under renamed-away directory: %+v", old)
	}
	moved, ok := fs.pendingIndex.GetMeta(newPath)
	if !ok || moved.Size != later.Size || moved.Kind != later.Kind || moved.BaseRev != later.BaseRev || moved.SnapshotID != later.SnapshotID || moved.ParentSnapshotID != later.ParentSnapshotID || !fs.shadowStore.Has(newPath) {
		t.Fatalf("pending/shadow did not move: %+v", moved)
	}
	if fs.shadowStore.Has(oldPath) {
		t.Fatal("old shadow survived successful migration")
	}
	data, err := fs.shadowStore.ReadAll(newPath)
	if err != nil || !bytes.Equal(data, large) {
		t.Fatalf("accepted late bytes not preserved at new path: size=%d error=%v", len(data), err)
	}
	mu.Lock()
	defer mu.Unlock()
	if remotePath != newPath || !bytes.Equal(remoteData, []byte("first")) {
		t.Fatalf("remote child not moved with committed snapshot: path=%s size=%d", remotePath, len(remoteData))
	}
}
