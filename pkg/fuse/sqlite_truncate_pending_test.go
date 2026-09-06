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
	testSQLiteZeroTruncateWaitingAppendLogFsync(t, false)
}

func TestSQLiteZeroTruncateWaitingAppendLogFsyncPendingMode(t *testing.T) {
	testSQLiteZeroTruncateWaitingAppendLogFsync(t, true)
}

func testSQLiteZeroTruncateWaitingAppendLogFsync(t *testing.T, pendingMode bool) {
	t.Helper()
	for _, route := range []string{"tryAppendLogLocked", "tryAppendLogGenerationResetLocked", "tryAppendLogFullRewriteLocked"} {
		for _, failMode := range []bool{false, true} {
			if !pendingMode && failMode {
				continue
			}
			t.Run(fmt.Sprintf("%s/mode=%t/fail=%t", route, pendingMode, failMode), func(t *testing.T) {
				var uploads [][]byte
				var chmodCalls int
				fs, original, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Has("chmod") {
						chmodCalls++
						if failMode {
							w.WriteHeader(http.StatusForbidden)
						}
						return
					}
					data, _ := io.ReadAll(r.Body)
					uploads = append(uploads, data)
					if r.Method == http.MethodPost {
						_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
					} else {
						_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
					}
				})
				t.Cleanup(closeServer)
				if pendingMode {
					fs.setPendingModeLocked(original, 0o600, 1)
				}
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
						wantStatus := gofuse.OK
						if name == "fsync" && failMode {
							wantStatus = gofuse.EACCES
						}
						if st != wantStatus {
							t.Errorf("%s: %v, want %v", name, st, wantStatus)
						}
					case <-time.After(3 * time.Second):
						t.Fatalf("%s did not finish", name)
					}
				}
				if pendingMode && (chmodCalls != 1 || original.HasPendingMode != failMode) {
					t.Errorf("chmod calls/pending = %d/%t, want 1/%t", chmodCalls, original.HasPendingMode, failMode)
				}
				entry, _ := fs.inodes.GetEntry(ino)
				if entry.Size != 0 {
					t.Errorf("inode size = %d, want zero", entry.Size)
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

func TestSQLiteZeroTruncateWaitingAppendLogPendingCreateMode(t *testing.T) {
	for _, gvisor := range []bool{false, true} {
		for _, failMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("gvisor=%t/fail-mode=%t", gvisor, failMode), func(t *testing.T) {
				const filePath = "/pending.db-wal"
				var mu sync.Mutex
				var body []byte
				var revision int64
				var mode uint32 = 0o644
				var contentCalls, chmodCalls int
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/status" {
						_ = json.NewEncoder(w).Encode(map[string]any{"storage_capabilities": map[string]bool{"append_log_v1": true}})
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if r.URL.Query().Has("chmod") {
						chmodCalls++
						if revision == 0 {
							http.NotFound(w, r)
							return
						}
						if failMode && chmodCalls == 1 {
							w.WriteHeader(http.StatusForbidden)
							return
						}
						var payload struct{ Mode uint32 }
						if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
							t.Error(err)
						}
						mode = payload.Mode
						return
					}
					if r.Method == http.MethodPost || r.Method == http.MethodPut {
						contentCalls++
						if !r.URL.Query().Has("append-log") || r.Header.Get("X-Dat9-Expected-Revision") != fmt.Sprint(revision) ||
							r.Header.Get("X-Dat9-Expected-Size") != fmt.Sprint(len(body)) {
							http.Error(w, "unexpected content request or baseline", http.StatusConflict)
							return
						}
						data, _ := io.ReadAll(r.Body)
						body = append(body, data...)
						revision++
						_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: revision, Size: int64(len(body))})
						return
					}
					if revision == 0 {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					w.Header().Set("X-Dat9-Revision", fmt.Sprint(revision))
					w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
					if r.Method == http.MethodGet {
						_, _ = w.Write(body)
					}
				}))
				t.Cleanup(ts.Close)
				c := newTestClient(ts.URL)
				c.Warm(context.Background())
				opts := &MountOptions{GVisorCompat: gvisor, AppendLogPatterns: []string{"**/pending.db-wal"}}
				opts.setDefaults()
				fs := NewDat9FS(c, opts)
				var created gofuse.CreateOut
				if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1},
					Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o600}, "pending.db-wal", &created); st != gofuse.OK {
					t.Fatal(st)
				}
				creator, _ := fs.fileHandles.Get(created.Fh)
				t.Cleanup(func() { fs.deleteFileHandle(created.Fh, creator) })
				var opened gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Flags: syscall.O_RDWR}, &opened); st != gofuse.OK {
					t.Fatal(st)
				}
				source, _ := fs.fileHandles.Get(opened.Fh)
				t.Cleanup(func() { fs.deleteFileHandle(opened.Fh, source) })
				if !creator.IsNew || !source.IsNew || creator.BaseRev != 0 || source.BaseRev != 0 || !creator.HasPendingMode {
					t.Fatal("Create/Open did not produce a pending private-mode WAL")
				}
				a := makeSQLiteTruncateWALForTest(t, 1, 2, 'A')
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}, a); st != gofuse.OK {
					t.Fatal(st)
				}

				// SetAttr holds the path fence while waiting for its FD. Wait for
				// the creator to reach the append fence before publishing truncate.
				fs.lockRemoteCommitPath(filePath)()
				pathLock := fs.remoteCommitLocks[filePath]
				source.Lock()
				unlockSource := sync.OnceFunc(source.Unlock)
				t.Cleanup(unlockSource)
				truncateDone := make(chan gofuse.Status, 1)
				go func() { truncateDone <- sqliteZeroTruncateForTest(fs, created.NodeId, opened.Fh) }()
				deadline := time.Now().Add(3 * time.Second)
				for pathLock.TryLock() {
					pathLock.Unlock()
					if time.Now().After(deadline) {
						t.Fatal("SetAttr did not acquire the path fence")
					}
					runtime.Gosched()
				}
				fsyncDone := make(chan gofuse.Status, 1)
				go func() { fsyncDone <- fs.Fsync(nil, &gofuse.FsyncIn{Fh: created.Fh}) }()
				stack := make([]byte, 1<<20)
				for {
					n := runtime.Stack(stack, true)
					waiting := false
					for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
						if strings.Contains(goroutine, "(*Dat9FS).tryAppendLogLocked(") && strings.Contains(goroutine, "(*Dat9FS).takeHandleRemoteCommitPathLocked(") {
							waiting = true
						}
					}
					if waiting {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("Fsync did not wait on append fence")
					}
					runtime.Gosched()
				}
				unlockSource()
				for name, done := range map[string]<-chan gofuse.Status{"truncate": truncateDone, "fsync": fsyncDone} {
					select {
					case st := <-done:
						if st != gofuse.OK {
							t.Errorf("%s = %v, want OK", name, st)
						}
					case <-time.After(3 * time.Second):
						t.Fatalf("%s did not finish", name)
					}
				}
				assertUncreated := func() {
					t.Helper()
					mu.Lock()
					defer mu.Unlock()
					if chmodCalls != 0 || contentCalls != 0 || revision != 0 {
						t.Fatalf("premature chmod/content/revision = %d/%d/%d, want 0/0/0", chmodCalls, contentCalls, revision)
					}
				}
				assertUncreated()
				if creator.DirtySeq != 0 || creator.Dirty.HasDirtyParts() || source.DirtySeq == 0 || !source.Dirty.HasDirtyParts() {
					t.Fatal("zero truncate did not retain sole content ownership on the truncating FD")
				}
				if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: created.Fh}); st != gofuse.OK {
					t.Fatalf("superseded Fsync retry = %v", st)
				}
				assertUncreated()
				if !source.HasPendingMode || source.PendingMode != 0o600 || source.PendingModeGen != creator.PendingModeGen || !creator.HasPendingMode {
					t.Fatal("pending create mode was not retained for the committing FD")
				}
				b := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: opened.Fh}, b); st != gofuse.OK {
					t.Fatal(st)
				}
				wantStatus := gofuse.OK
				if failMode {
					wantStatus = gofuse.EACCES
				}
				if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: opened.Fh}); st != wantStatus {
					t.Fatalf("committing Fsync = %v, want %v", st, wantStatus)
				}
				if failMode {
					if !creator.HasPendingMode || !source.HasPendingMode {
						t.Fatal("failed chmod lost pending mode")
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: opened.Fh}); st != gofuse.OK {
						t.Fatalf("committed mode retry = %v", st)
					}
				}
				if creator.HasPendingMode || source.HasPendingMode {
					t.Fatal("successful chmod did not clear the shared mode generation")
				}
				mu.Lock()
				wantChmodCalls := 1
				if failMode {
					wantChmodCalls = 2
				}
				if !bytes.Equal(body, b) || mode != 0o600 || revision != 1 || contentCalls != 1 || chmodCalls != wantChmodCalls {
					t.Errorf("committed bytes/mode/revision/content/chmod = %x/%o/%d/%d/%d", body, mode, revision, contentCalls, chmodCalls)
				}
				mu.Unlock()
				got, st, err := readDat9FSTestRange(fs, created.NodeId, created.Fh, 0, len(a))
				if err != nil || st != gofuse.OK || !bytes.Equal(got, b) {
					t.Fatalf("creator read = %x/%v/%v, want new header", got, st, err)
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
