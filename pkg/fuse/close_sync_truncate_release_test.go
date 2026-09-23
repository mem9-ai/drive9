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

	"github.com/mem9-ai/drive9/pkg/client"
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
			test, headerBytes := newCloseSyncRotatedShadowTest(t)
			fs := test.fs
			fh, _ := fs.fileHandles.Get(test.old)
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
			test.assertRemote(t, string(want), 4)
			if rev, gotSize := fh.appendLogCommittedBaseline(); rev != 4 || gotSize != size {
				t.Errorf("append baseline=%d/%d, want 4/%d", rev, gotSize, size)
			}
			test.write(t, test.old, "x")
			test.flush(t, test.old)
			want[0] = 'x'
			test.assertRemote(t, string(want), 5)
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
	fh.Lock()
	fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
	fh.ShadowReady, fh.ShadowSpill = true, true
	fh.Unlock()
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

// newCloseSyncRotatedShadowTest drives the actual header-write/reset commit,
// including the remote CAS and shadow rotation, rather than setting the clean
// post-reset claim fields directly.
func newCloseSyncRotatedShadowTest(t *testing.T) (*closeSyncTruncateTest, []byte) {
	t.Helper()
	test := newCloseSyncTruncateTest(t)
	fs := test.fs
	oldHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	oldHeader, ok := parseSQLiteWALHeader(oldHeaderBytes)
	if !ok {
		t.Fatal("invalid old WAL header")
	}
	oldImage := append(append([]byte(nil), oldHeaderBytes...), bytes.Repeat([]byte("o"), 32)...)
	test.write(t, test.old, string(oldImage))
	test.flush(t, test.old)
	fh, _ := fs.fileHandles.Get(test.old)
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/race.txt"})
	fh.Lock()
	if err := fs.shadowStore.WriteFull(fh.Path, oldImage, 2); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.ShadowReady, fh.ShadowSpill = true, true
	fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 2, int64(len(oldImage)))
	fh.appendLogObserveCommittedSQLiteWALHeader(oldHeader)
	fh.Unlock()

	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	test.write(t, test.old, string(newHeader))
	fh.Lock()
	reset := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	passive := fs.isPassiveCloseSyncHandleLocked(fh)
	rotated := fh.ShadowReady && fh.ShadowSpill && fh.ShadowStageGen != fs.shadowStore.ActiveGeneration(fh.Path)
	fh.Unlock()
	if reset.route != appendLogRouteCommitted || reset.status != gofuse.OK || !passive || !rotated {
		t.Fatalf("reset=%+v passive=%t rotated=%t", reset, passive, rotated)
	}
	test.assertRemote(t, string(newHeader), 3)
	return test, newHeader
}

func TestCloseSyncRefreshCommittedRevisionRetiresRotatedShadow(t *testing.T) {
	for _, explicitSize := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit-size-%t", explicitSize), func(t *testing.T) {
			test, header := newCloseSyncRotatedShadowTest(t)
			fs := test.fs
			fh, _ := fs.fileHandles.Get(test.old)
			generation := fs.shadowStore.ActiveGeneration(fh.Path)
			want := append(append([]byte(nil), header...), []byte("new tail")...)
			test.mu.Lock()
			test.content, test.revision = want, 4
			test.mu.Unlock()
			fs.inodes.UpdateSize(fh.Ino, int64(len(want)))
			fs.inodes.UpdateRevision(fh.Ino, 4)
			fs.recordCommittedRevisionWithSize(fh.Path, 4, int64(len(want)))
			if explicitSize {
				fs.refreshCommittedRevisionForOpenHandlesWithSize(fh.Path, 4, nil, int64(len(want)))
			} else {
				fs.refreshCommittedRevisionForOpenHandles(fh.Path, 4, nil)
			}
			// Once BaseRev advances, Read cannot rely on the lazy revision
			// refresh to notice and repair a stale 32-byte shadow source.
			result, status := fs.Read(nil, &gofuse.ReadIn{InHeader: test.header, Fh: test.old, Size: uint32(len(want))}, nil)
			if status != gofuse.OK {
				t.Fatalf("Read: %v", status)
			}
			data, status := result.Bytes(make([]byte, len(want)))
			result.Done()
			if status != gofuse.OK || !bytes.Equal(data, want) {
				t.Errorf("read=%q status=%v, want %q", data, status, want)
			}
			if fh.BaseRev != 4 || fh.Dirty.Size() != int64(len(want)) || fh.ShadowReady || fh.ShadowSpill || fh.ShadowStageGen != 0 || fh.ShadowStageSeq != 0 {
				t.Errorf("refresh retained an obsolete shadow claim or baseline: revision=%d size=%d ready=%t spill=%t gen=%d seq=%d", fh.BaseRev, fh.Dirty.Size(), fh.ShadowReady, fh.ShadowSpill, fh.ShadowStageGen, fh.ShadowStageSeq)
			}
			if data, err := fs.shadowStore.ReadAllIfGeneration(fh.Path, generation); err != nil || !bytes.Equal(data, header) {
				t.Fatalf("refresh mutated the shared shadow: %x, %v", data, err)
			}
		})
	}
}

func TestCloseSyncPathTruncateAdoptsConfirmedAppendLogBaseline(t *testing.T) {
	for _, size := range []int64{16, 40} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			test, header := newCloseSyncRotatedShadowTest(t)
			fs := test.fs
			fh, _ := fs.fileHandles.Get(test.old)
			generation := fs.shadowStore.ActiveGeneration(fh.Path)
			fs.recordCommittedRevisionWithSize(fh.Path, 4, size)
			fs.syncOpenHandlesAfterPathTruncate(fh.Ino, size)
			fh.Lock()
			defer fh.Unlock()
			if fh.BaseRev != 4 || fh.OrigSize != size || fh.Dirty.Size() != size || fh.ZeroBase || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
				t.Fatalf("passive truncate revision=%d original=%d size=%d zero=%t seq=%d dirty=%t", fh.BaseRev, fh.OrigSize, fh.Dirty.Size(), fh.ZeroBase, fh.DirtySeq, fh.Dirty.HasDirtyParts())
			}
			if fh.ShadowReady || fh.ShadowSpill || fh.ShadowStageGen != 0 || fh.ShadowStageSeq != 0 || fh.ShadowCommitReady || fh.ShadowCommitSeq != 0 || fh.Dirty.RestorePart != nil || fh.Dirty.OnPartFull != nil {
				t.Fatal("confirmed truncate retained shadow claim or callbacks")
			}
			if rev, gotSize := fh.appendLogCommittedBaseline(); rev != 4 || gotSize != size || fh.appendLogLayoutAt(4, size) != client.ContentLayoutAppendLog || !fh.appendLogCanUseTail() || fh.appendLog.hasRewriteBase || fh.appendLog.sqliteWALTruncated {
				t.Fatalf("confirmed append baseline=%d/%d state=%+v", rev, gotSize, fh.appendLog)
			}
			if data, err := fs.shadowStore.ReadAllIfGeneration(fh.Path, generation); err != nil || !bytes.Equal(data, header) {
				t.Fatalf("passive truncate mutated shared shadow: %x, %v", data, err)
			}
		})
	}
}

func TestCloseSyncPathTruncateWithoutConfirmedSizePreservesAppendState(t *testing.T) {
	test := newCloseSyncTruncateTest(t)
	fs := test.fs
	fh, _ := fs.fileHandles.Get(test.old)
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/race.txt"})
	fh.Lock()
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 1, 4)
	fh.appendLogAdoptCommittedBaseline(1, 4)
	before := fh.appendLog
	fh.Unlock()
	// A revision-only notification invalidates the cached size, so this
	// sibling must not adopt it or claim to own a pending truncate rewrite.
	fs.recordCommittedRevision(fh.Path, 2)
	fs.syncOpenHandlesAfterPathTruncate(fh.Ino, 2)
	fh.Lock()
	defer fh.Unlock()
	if fh.BaseRev != 1 || fh.Dirty.Size() != 2 || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		t.Fatalf("unconfirmed truncate revision=%d size=%d seq=%d dirty=%t", fh.BaseRev, fh.Dirty.Size(), fh.DirtySeq, fh.Dirty.HasDirtyParts())
	}
	if fh.appendLog != before {
		t.Fatalf("passive sizing recorded a pending append-log mutation: before=%+v after=%+v", before, fh.appendLog)
	}
}

func TestPassiveCloseSyncHandleExcludesUncommittedCreate(t *testing.T) {
	fs := &Dat9FS{}
	for _, isNew := range []bool{false, true} {
		fh := &FileHandle{Path: "/new", WritePolicy: WritePolicyCloseSync, IsNew: isNew, Dirty: NewWriteBuffer("/new", 1024, 0)}
		fh.Lock()
		eligible := fs.handleCanAdoptCommittedRevisionLocked(fh)
		passive := fs.isPassiveCloseSyncHandleLocked(fh)
		fh.Unlock()
		if !eligible || passive == isNew {
			t.Fatalf("IsNew=%t clean=%t passive=%t", isNew, eligible, passive)
		}
	}
}
