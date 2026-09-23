package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type closeSyncTruncateTest struct {
	fs        *Dat9FS
	header    gofuse.InHeader
	old       uint64
	mu        sync.Mutex
	revision  int64
	content   []byte
	layout    string
	beforePut func(http.ResponseWriter, []byte) bool
}

func newCloseSyncTruncateTest(t *testing.T) *closeSyncTruncateTest {
	t.Helper()
	test := &closeSyncTruncateTest{}
	test.fs = newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		test.mu.Lock()
		defer test.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read upload: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if test.beforePut != nil && test.beforePut(w, body) {
				return
			}
			want := r.Header.Get("X-Dat9-Expected-Revision")
			if want != "" && want != strconv.FormatInt(test.revision, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			test.content = body
			test.layout = "single"
			test.revision++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": test.revision})
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(test.content)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(test.revision, 10))
			w.Header().Set("X-Dat9-Content-Layout", test.layout)
		case http.MethodGet:
			http.ServeContent(w, r, "race.txt", time.Time{}, bytes.NewReader(test.content))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	var created gofuse.CreateOut
	const pid = 1234
	if st := test.fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Pid: pid}},
		Flags:    syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC, Mode: 0o644,
	}, "race.txt", &created); st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	test.header = gofuse.InHeader{NodeId: created.NodeId, Caller: gofuse.Caller{Pid: pid}}
	test.old = created.Fh
	test.write(t, test.old, "seed")
	test.flush(t, test.old)
	// Successful Flush acknowledges close(2), but Release may arrive later.
	return test
}

func (test *closeSyncTruncateTest) write(t *testing.T, handle uint64, data string) {
	t.Helper()
	if n, st := test.fs.Write(nil, &gofuse.WriteIn{InHeader: test.header, Fh: handle}, []byte(data)); st != gofuse.OK || int(n) != len(data) {
		t.Fatalf("Write: n=%d, status=%v", n, st)
	}
}

func (test *closeSyncTruncateTest) flush(t *testing.T, handle uint64) {
	t.Helper()
	if st := test.fs.Flush(nil, &gofuse.FlushIn{InHeader: test.header, Fh: handle}); st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
}

func (test *closeSyncTruncateTest) release(handle uint64) {
	test.fs.Release(nil, &gofuse.ReleaseIn{InHeader: test.header, Fh: handle})
}

func (test *closeSyncTruncateTest) open(t *testing.T, pid uint32) uint64 {
	t.Helper()
	header := test.header
	header.Pid = pid
	var out gofuse.OpenOut
	if st := test.fs.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: syscall.O_WRONLY}, &out); st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}
	return out.Fh
}

func (test *closeSyncTruncateTest) truncate(size uint64) gofuse.Status {
	var out gofuse.AttrOut
	return test.fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: test.header, Valid: gofuse.FATTR_SIZE, Size: size,
	}}, &out)
}

func (test *closeSyncTruncateTest) assertRemote(t *testing.T, content string, revision int64) {
	t.Helper()
	test.mu.Lock()
	defer test.mu.Unlock()
	if string(test.content) != content || test.revision != revision {
		t.Fatalf("remote content=%q revision=%d, want %q revision=%d", test.content, test.revision, content, revision)
	}
}

func TestCloseSyncPathTruncateBeforeDelayedRelease(t *testing.T) {
	for _, size := range []uint64{0, 2, 8} {
		for _, order := range []string{"release-before-open", "release-before-flush", "release-after-flush", "reuse-live-handle"} {
			t.Run(fmt.Sprintf("size-%d/%s", size, order), func(t *testing.T) {
				test := newCloseSyncTruncateTest(t)
				if st := test.truncate(size); st != gofuse.OK {
					t.Fatalf("truncate: %v", st)
				}
				truncated := make([]byte, size)
				copy(truncated, "seed")
				test.assertRemote(t, string(truncated), 2)
				if order == "release-before-open" {
					test.release(test.old)
					test.assertRemote(t, string(truncated), 2)
				}
				writer := test.old
				if order != "reuse-live-handle" {
					writer = test.open(t, test.header.Pid)
				}
				wantRevision := int64(3)
				if size == 0 {
					// Linux may issue another path SetAttr after writable Open.
					if st := test.truncate(0); st != gofuse.OK {
						t.Fatalf("second truncate: %v", st)
					}
					wantRevision++
				}
				test.write(t, writer, "final")
				if order == "release-before-flush" {
					test.release(test.old)
					test.assertRemote(t, string(truncated), wantRevision-1)
				}
				test.flush(t, writer)
				if order == "release-after-flush" {
					test.release(test.old)
				}
				test.release(writer)
				want := make([]byte, max(size, 5))
				copy(want, "final")
				test.assertRemote(t, string(want), wantRevision)
			})
		}
	}
}

func TestCloseSyncPathTruncateKeepsDirtyOwnerAndCleanSibling(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(fmt.Sprintf("alias-%t", alias), func(t *testing.T) {
			test := newCloseSyncTruncateTest(t)
			if alias && !test.fs.inodes.AddAlias(test.header.NodeId, "/alias.txt", "object-1", 2, false, 4, time.Now()) {
				t.Fatal("add hardlink alias")
			}
			writer := test.open(t, test.header.Pid)
			test.write(t, writer, "pending")
			if st := test.truncate(0); st != gofuse.OK {
				t.Fatalf("truncate: %v", st)
			}
			// A live dirty writer still owns the truncate, including through
			// a hardlink alias. Sizing its clean sibling creates no publisher.
			test.assertRemote(t, "seed", 1)
			test.release(test.old)
			test.assertRemote(t, "seed", 1)
			test.write(t, writer, "final")
			test.flush(t, writer)
			test.release(writer)
			test.assertRemote(t, "final", 2)
		})
	}
}

func TestCloseSyncPathTruncateRefreshesCleanHardlinkHandle(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	if !test.fs.inodes.AddAlias(test.header.NodeId, "/alias.txt", "object-1", 2, false, 4, time.Now()) {
		t.Fatal("add hardlink alias")
	}
	if st := test.truncate(0); st != gofuse.OK {
		t.Fatalf("truncate alias: %v", st)
	}
	test.assertRemote(t, "", 2)
	test.write(t, test.old, "final")
	test.flush(t, test.old)
	test.release(test.old)
	test.assertRemote(t, "final", 3)
}

func TestCloseSyncPathTruncatePreservesIndependentWriterConflict(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	writer := test.open(t, test.header.Pid+1)
	test.write(t, writer, "stale")
	if st := test.truncate(0); st != gofuse.OK {
		t.Fatalf("truncate: %v", st)
	}
	test.assertRemote(t, "", 2)
	if st := test.fs.Flush(nil, &gofuse.FlushIn{InHeader: test.header, Fh: writer}); st != gofuse.EIO {
		t.Fatalf("independent stale writer Flush: %v, want EIO", st)
	}
	test.release(writer)
	test.release(test.old)
	test.assertRemote(t, "", 2)
}

func TestCloseSyncPathTruncateWaitsForRemoteWithCleanHandle(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail-%t", fail), func(t *testing.T) {
			test := newCloseSyncTruncateTest(t)
			entered := make(chan struct{})
			proceed := make(chan struct{})
			var once, enteredOnce sync.Once
			var publications atomic.Int32
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			test.mu.Lock()
			test.beforePut = func(w http.ResponseWriter, body []byte) bool {
				if len(body) != 0 {
					t.Errorf("truncate uploaded %q", body)
				}
				publications.Add(1)
				enteredOnce.Do(func() { close(entered) })
				<-proceed
				if fail {
					w.WriteHeader(http.StatusForbidden)
				}
				return fail
			}
			test.mu.Unlock()
			done := make(chan gofuse.Status, 1)
			go func() { done <- test.truncate(0) }()
			select {
			case <-entered:
			case status := <-done:
				t.Fatalf("truncate returned before remote commit: %v", status)
			case <-time.After(5 * time.Second):
				t.Fatal("truncate did not start remote commit")
			}
			select {
			case status := <-done:
				t.Fatalf("truncate returned while commit blocked: %v", status)
			default:
			}
			unblock()
			select {
			case status := <-done:
				if fail && status == gofuse.OK || !fail && status != gofuse.OK {
					t.Fatalf("truncate status=%v, fail=%t", status, fail)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("truncate did not finish")
			}
			test.release(test.old)
			if got := publications.Load(); got != 1 {
				t.Fatalf("truncate publications=%d, want 1", got)
			}
			if fail {
				test.assertRemote(t, "seed", 1)
			} else {
				test.assertRemote(t, "", 2)
			}
		})
	}
}

func TestCloseSyncPathTruncateRetiresRotatedShadow(t *testing.T) {
	for _, size := range []int64{16, 48} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			test := newCloseSyncTruncateTest(t)
			fs := test.fs
			headerBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
			header, ok := parseSQLiteWALHeader(headerBytes)
			if !ok {
				t.Fatal("invalid test WAL header")
			}
			test.write(t, test.old, string(headerBytes))
			test.flush(t, test.old)
			fh, _ := fs.fileHandles.Get(test.old)
			if err := fs.shadowStore.WriteFull(fh.Path, headerBytes, 2); err != nil {
				t.Fatal(err)
			}
			fh.Lock()
			fh.ShadowReady, fh.ShadowSpill = true, true
			fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
			// Use the actual generation-reset transition that leaves a clean
			// handle with a superseded shadow claim and a new live shadow.
			fs.rotateAppendLogGenerationShadowLocked(fh, fh.Path, header, 2)
			fh.Unlock()
			fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/race.txt"})
			test.mu.Lock()
			test.layout = "append_log"
			test.mu.Unlock()
			entry, _ := fs.inodes.GetEntry(fh.Ino)
			handled, status := fs.rewriteAppendLogPathTruncate(context.Background(), entry, fh.Ino, test.header.Pid, size, nil)
			if !handled || status != gofuse.OK {
				t.Fatalf("path truncate: handled=%t status=%v", handled, status)
			}
			want := make([]byte, size)
			copy(want, headerBytes)
			test.assertRemote(t, string(want), 3)
			if rev, gotSize := fh.appendLogCommittedBaseline(); rev != 3 || gotSize != size {
				t.Errorf("append baseline=%d/%d, want 3/%d", rev, gotSize, size)
			}
			test.write(t, test.old, "x")
			test.flush(t, test.old)
			want[0] = 'x'
			test.assertRemote(t, string(want), 4)
		})
	}
}

func TestCloseSyncPathTruncateDoesNotMutateNewerShadow(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	fs := test.fs
	fh, _ := fs.fileHandles.Get(test.old)
	if err := fs.shadowStore.WriteFull(fh.Path, []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
	fh.ShadowReady, fh.ShadowSpill = true, true
	if err := fs.shadowStore.WriteFull(fh.Path, []byte("newer writer"), 2); err != nil {
		t.Fatal(err)
	}
	generation := fs.shadowStore.ActiveGeneration(fh.Path)
	// Model an acknowledged path truncate while the shared store contains
	// another writer's later generation. Passive adoption must not Ensure,
	// Truncate, Remove or write through that generation.
	test.mu.Lock()
	test.content, test.revision = []byte("se"), 2
	test.mu.Unlock()
	fs.updateOpenHandleBaseRevision(fh.Path, 2, test.header.Pid, 2)
	test.write(t, test.old, "x")
	data, err := fs.shadowStore.ReadAllIfGeneration(fh.Path, generation)
	if err != nil || string(data) != "newer writer" {
		t.Fatalf("passive handle mutated newer shadow: %q, %v", data, err)
	}
}

func TestCloseSyncPathTruncateRechecksOwnerAfterConcurrentFlush(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	test.write(t, test.old, "updated")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var enteredOnce, proceedOnce sync.Once
	unblock := func() { proceedOnce.Do(func() { close(proceed) }) }

	testHookAfterCallerOwnedTruncateScan = func(path string) {
		if path == "/race.txt" {
			enteredOnce.Do(func() { close(entered) })
			<-proceed
		}
	}
	done := make(chan bool, 1)
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("truncate owner worker did not stop")
		}
		testHookAfterCallerOwnedTruncateScan = nil
	})
	go func() {
		defer close(done)
		done <- test.fs.adoptCallerOwnedInodeTruncate(test.header.NodeId, test.header.Pid, 0)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("truncate did not select an owner")
	}
	test.flush(t, test.old)
	unblock()
	select {
	case adopted := <-done:
		if adopted {
			t.Fatal("truncate reused an owner whose Flush already completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ownership recheck did not finish")
	}
	test.assertRemote(t, "updated", 2)
	fh, _ := test.fs.fileHandles.Get(test.old)
	if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || fh.Dirty.Size() != 7 {
		t.Fatal("ownership recheck dirtied or truncated the acknowledged handle")
	}
}

func TestCloseSyncPathTruncateRebasesAppendLogState(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	fh, _ := test.fs.fileHandles.Get(test.old)
	fh.appendLogRecordTruncate()
	test.fs.updateOpenHandleBaseRevision(fh.Path, 2, test.header.Pid, 2)
	if revision, size := fh.appendLogCommittedBaseline(); revision != 2 || size != 2 || fh.appendLog.hasRewriteBase {
		t.Fatalf("append baseline=%d/%d rewriteBase=%t", revision, size, fh.appendLog.hasRewriteBase)
	}
}

func TestCloseSyncPathTruncateClearsCleanSiblingRestoreCallbacks(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	fh, _ := test.fs.fileHandles.Get(test.old)
	fh.Dirty.RestorePart = func(int) ([]byte, error) { t.Error("restored obsolete shadow"); return nil, nil }
	fh.Dirty.OnPartFull = func(int, []byte) { t.Error("evicted to obsolete shadow") }
	test.fs.syncOpenHandlesAfterPathTruncate(fh.Ino, 2)
	if fh.Dirty.RestorePart != nil || fh.Dirty.OnPartFull != nil || fh.Dirty.HasDirtyParts() || fh.DirtySeq != 0 {
		t.Fatal("clean sibling retained staging callbacks or became dirty")
	}
}
