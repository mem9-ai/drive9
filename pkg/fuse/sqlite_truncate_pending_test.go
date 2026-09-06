package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestSQLiteZeroTruncateWaitingAppendLogFsync(t *testing.T) {
	for _, route := range []string{"tryAppendLogLocked", "tryAppendLogGenerationResetLocked", "tryAppendLogFullRewriteLocked"} {
		t.Run(route, func(t *testing.T) {
			var uploads [][]byte
			fs, original, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				uploads = append(uploads, data)
				if r.Method == http.MethodPost {
					_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
				}
			})
			t.Cleanup(closeServer)
			original.appendLogObserveLayout(client.ContentLayoutAppendLog, original.BaseRev, original.OrigSize)
			switch route {
			case "tryAppendLogGenerationResetLocked":
				h0, _ := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
				setGenerationResetDirty(t, original, h0, makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4), 128)
			case "tryAppendLogFullRewriteLocked":
				original.appendLogRecordUserWrite(7, 0, 1)
			}
			ino := fs.inodes.Lookup(original.Path, false, original.OrigSize, time.Now())
			fs.inodes.UpdateRevision(ino, 5)
			original.Ino = ino
			original.DirtySeq = fs.markDirtySize(ino, original.Dirty.Size())
			originalID := fs.allocateFileHandle(original)
			t.Cleanup(func() { fs.deleteFileHandle(originalID, original) })
			source := &FileHandle{Ino: ino, Path: original.Path, Flags: syscall.O_RDWR, BaseRev: 5,
				OrigSize: original.OrigSize, Dirty: fs.newWriteBuffer(original.Path, maxPreloadSize, 0)}
			sourceID := fs.allocateFileHandle(source)
			t.Cleanup(func() { fs.deleteFileHandle(sourceID, source) })

			// SetAttr owns the path fence but cannot publish until we release
			// its FD. The Fsync stack proves its initial consumer has already
			// run and the selected upload route is waiting on that fence.
			fs.lockRemoteCommitPath(original.Path)()
			pathLock := fs.remoteCommitLocks[original.Path]
			source.Lock()
			unlockSource := sync.OnceFunc(source.Unlock)
			t.Cleanup(unlockSource)
			truncateDone := make(chan gofuse.Status, 1)
			go func() { truncateDone <- sqliteZeroTruncateForTest(fs, ino, sourceID) }()
			deadline := time.Now().Add(3 * time.Second)
			for pathLock.TryLock() {
				pathLock.Unlock()
				if time.Now().After(deadline) {
					t.Fatal("SetAttr did not acquire the path fence")
				}
				runtime.Gosched()
			}
			fsyncDone := make(chan gofuse.Status, 1)
			go func() { fsyncDone <- fs.Fsync(nil, &gofuse.FsyncIn{Fh: originalID}) }()
			stack := make([]byte, 1<<20)
			for {
				n := runtime.Stack(stack, true)
				waiting := false
				for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
					if strings.Contains(goroutine, "(*Dat9FS)."+route+"(") && strings.Contains(goroutine, "(*Dat9FS).takeHandleRemoteCommitPathLocked(") {
						waiting = true
					}
				}
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("Fsync did not wait in %s", route)
				}
				runtime.Gosched()
			}
			unlockSource()
			for name, done := range map[string]<-chan gofuse.Status{"truncate": truncateDone, "fsync": fsyncDone} {
				select {
				case st := <-done:
					if st != gofuse.OK {
						t.Fatalf("%s: %v", name, st)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("%s did not finish", name)
				}
			}
			if len(uploads) != 0 {
				t.Fatalf("stale Fsync uploaded %d old payload(s): %q", len(uploads), uploads)
			}
			got, st, err := readDat9FSTestRange(fs, ino, originalID, 0, 1)
			if err != nil || st != gofuse.OK || len(got) != 0 {
				t.Fatalf("read after truncate = %q/%v/%v, want EOF", got, st, err)
			}
		})
	}
}

func TestSQLiteZeroTruncateDelayedLayerCommit(t *testing.T) {
	for _, suffix := range []string{"-wal", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			filePath := "/layer.db" + suffix
			var committed []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-test/objects" && r.URL.Query().Get("path") == filePath {
					_, _ = w.Write(committed)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-test/entries" {
					t.Errorf("unexpected layer request %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected request", http.StatusInternalServerError)
					return
				}
				var req client.FSLayerEntryRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.Op != "upsert" || req.Path != filePath || req.BaseRevision != 7 {
					t.Errorf("layer upsert = %+v", req)
				}
				committed = cloneBytes(req.Content)
				_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "layer-test", "path": filePath, "op": "upsert", "kind": "file"})
			}))
			t.Cleanup(ts.Close)
			opts := &MountOptions{GVisorCompat: true, LayerRef: "layer-test"}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			ino := fs.inodes.Lookup(filePath, false, 24, time.Now())
			fs.inodes.UpdateRevision(ino, 7)
			open := func() (uint64, *FileHandle) {
				fh := &FileHandle{Ino: ino, Path: filePath, Flags: syscall.O_RDWR, BaseRev: 7, OrigSize: 24,
					Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0)}
				_, _ = fh.Dirty.Write(0, []byte("old layer journal bytes!"))
				fh.Dirty.ClearDirty()
				id := fs.allocateFileHandle(fh)
				t.Cleanup(func() { fs.deleteFileHandle(id, fh) })
				return id, fh
			}
			busyID, busy := open()
			sourceID, _ := open()
			busy.Lock()
			unlockBusy := sync.OnceFunc(busy.Unlock)
			t.Cleanup(unlockBusy)
			if st := sqliteZeroTruncateForTest(fs, ino, sourceID); st != gofuse.OK {
				t.Fatal(st)
			}
			want := []byte("new layer")
			sqliteWriteSyncForTest(t, fs, ino, sourceID, 0, want)
			state, ok := fs.committedMutation(ino)
			if !ok || state.committedRevision != 0 || state.committedSeq <= busy.pendingSQLiteTruncate.Load().seq || !bytes.Equal(committed, want) {
				t.Fatalf("revisionless layer commit = %+v/%t/%q", state, ok, committed)
			}
			fs.readCache.Invalidate(filePath)
			unlockBusy()
			got, st, err := readDat9FSTestRange(fs, ino, busyID, 0, 30)
			if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
				t.Errorf("delayed read = %q/%v/%v, want %q", got, st, err, want)
			}
			got, st, err = readDat9FSTestRange(fs, ino, busyID, int64(len(want)), 1)
			if err != nil || st != gofuse.OK || len(got) != 0 {
				t.Errorf("EOF = %q/%v/%v", got, st, err)
			}
			sqliteWriteSyncForTest(t, fs, ino, busyID, uint64(len(want)), []byte("!"))
			if !bytes.Equal(committed, append(cloneBytes(want), '!')) {
				t.Fatalf("reused layer bytes = %q", committed)
			}
		})
	}
}

func TestSQLiteZeroTruncateDelayedPendingCreate(t *testing.T) {
	for _, gvisor := range []bool{false, true} {
		for _, appendLog := range []bool{false, true} {
			t.Run(fmt.Sprintf("gvisor=%t/append-log=%t", gvisor, appendLog), func(t *testing.T) {
				const filePath = "/pending.db-wal"
				remote := &casFileServer{t: t, path: filePath}
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/status" {
						_ = json.NewEncoder(w).Encode(map[string]any{"storage_capabilities": map[string]bool{"append_log_v1": true}})
						return
					}
					if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
						data, _ := io.ReadAll(r.Body)
						remote.mu.Lock()
						defer remote.mu.Unlock()
						if r.Header.Get("X-Dat9-Expected-Revision") != fmt.Sprint(remote.revision) || r.Header.Get("X-Dat9-Expected-Size") != fmt.Sprint(len(remote.body)) {
							http.Error(w, "append baseline mismatch", http.StatusConflict)
							return
						}
						remote.body = append(remote.body, data...)
						remote.revision++
						_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: remote.revision, Size: int64(len(remote.body))})
						return
					}
					remote.serveHTTP(w, r)
				}))
				t.Cleanup(ts.Close)
				c := newTestClient(ts.URL)
				c.Warm(context.Background())
				opts := &MountOptions{GVisorCompat: gvisor}
				if appendLog {
					opts.AppendLogPatterns = []string{"**/pending.db-wal"}
				}
				opts.setDefaults()
				fs := NewDat9FS(c, opts)
				ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
				creator := &FileHandle{Ino: ino, Path: filePath, Flags: syscall.O_RDWR, IsNew: true,
					Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0)}
				creatorID := fs.allocateFileHandle(creator)
				t.Cleanup(func() { fs.deleteFileHandle(creatorID, creator) })
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: syscall.O_RDWR}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				busy, _ := fs.fileHandles.Get(out.Fh)
				t.Cleanup(func() { fs.deleteFileHandle(out.Fh, busy) })
				if !busy.IsNew || busy.BaseRev != 0 {
					t.Fatal("second Open did not adopt pending sidecar creation")
				}
				busy.Lock()
				unlockBusy := sync.OnceFunc(busy.Unlock)
				t.Cleanup(unlockBusy)
				if st := sqliteZeroTruncateForTest(fs, ino, creatorID); st != gofuse.OK {
					t.Fatal(st)
				}
				sqliteWriteSyncForTest(t, fs, ino, creatorID, 0, []byte("new"))
				unlockBusy()
				sqliteWriteSyncForTest(t, fs, ino, out.Fh, 3, []byte("!"))
				_, body, _ := remote.snapshot()
				if !bytes.Equal(body, []byte("new!")) {
					t.Fatalf("reused pending handle bytes = %q, want new!", body)
				}
			})
		}
	}
}

func sqliteZeroTruncateForTest(fs *Dat9FS, ino, id uint64) gofuse.Status {
	return fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE | gofuse.FATTR_FH, Fh: id,
	}}, &gofuse.AttrOut{})
}

func sqliteWriteSyncForTest(t *testing.T, fs *Dat9FS, ino, id, offset uint64, data []byte) {
	t.Helper()
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Offset: offset}, data); st != gofuse.OK || int(n) != len(data) {
		t.Fatalf("Write = %d/%v", n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
		t.Fatalf("Fsync = %v", st)
	}
}
