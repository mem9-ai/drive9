package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// newExtentMetaOKServer is JuiceFS Meta.Create/Unlink for tests: create
// allocates a new inode id, unlink drops the name, recreate must not reuse.
func newExtentMetaOKServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	return newExtentMetaOKServerMode(t, false)
}

func newExtentMetaOKServerMode(t *testing.T, commitOK bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var mu sync.Mutex
	idByPath := map[string]string{}
	revByPath := map[string]int64{}
	var n, creates atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			creates.Add(1)
			if _, ok := idByPath[path]; ok {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "path already exists"})
				return
			}
			id := fmt.Sprintf("inode-%d", n.Add(1))
			idByPath[path] = id
			revByPath[path] = 1
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodDelete:
			delete(idByPath, path)
			delete(revByPath, path)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case r.Method == http.MethodHead:
			id, ok := idByPath[path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Resource-ID", id)
			w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", revByPath[path]))
			w.Header().Set("Content-Length", "0")
		case commitOK && r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			revByPath[path]++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": revByPath[path], "slice_generation": 0})
		case commitOK && r.Method == http.MethodPost && (r.URL.RawQuery == "prepare-blocks=1" || r.URL.RawQuery == "presign-put=1"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": []any{}, "put_url": "http://127.0.0.1/obj", "headers": map[string]string{}})
		case commitOK && r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &creates
}

func TestAttachExtentWriterClearsZeroBaseOnceCommitted(t *testing.T) {
	// JuiceFS has no ZeroBase. After VACUUM rewrite commits, a handle that
	// still has ZeroBase must adopt the open-file committed size or Read
	// clips to 0 and btreeInitPage sees zeros.
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	of := newExtentOpenFile(4096)
	of.committed = 4096
	of.rev = 3
	fs.extentOpens = map[uint64]*extentOpenFile{9: of}
	fh := &FileHandle{
		Ino: 9, Path: "/vac.db", OrigSize: 0, ZeroBase: true,
		ContentLayout: client.ContentLayoutExtent,
		Dirty:         NewWriteBuffer("/vac.db", 1<<20, 0),
	}
	fs.attachExtentWriter(fh)
	if fh.ZeroBase {
		t.Fatal("ZeroBase still set after sibling commit size")
	}
	if fh.OrigSize != 4096 {
		t.Fatalf("OrigSize=%d want 4096", fh.OrigSize)
	}
}

func TestOpenSecondHandleSeesUnflushedExtentWrites(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "app.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	header := []byte("SQLite format 3\x00")
	if written, wst := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}, header); wst != gofuse.OK || int(written) != len(header) {
		t.Fatalf("Write written=%d status=%v", written, wst)
	}

	var opened gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Flags:    uint32(syscall.O_RDWR),
	}, &opened)
	if st != gofuse.OK {
		t.Fatalf("second Open status=%v (unflushed extent must not ENOENT)", st)
	}
	second, ok := fs.fileHandles.Get(opened.Fh)
	if !ok {
		t.Fatal("missing second handle")
	}
	if !second.isExtent() {
		t.Fatal("second handle must inherit extent layout from the live writer")
	}
	if second.extentWriter == nil {
		t.Fatal("second handle must share the extent writer")
	}

	got, rst, err := readDat9FSTestRange(fs, created.NodeId, opened.Fh, 0, len(header))
	if err != nil {
		t.Fatal(err)
	}
	if rst != gofuse.OK {
		t.Fatalf("Read status=%v", rst)
	}
	if string(got) != string(header) {
		t.Fatalf("second open read %q, want %q (not zeros from size-only Dirty)", got, header)
	}
}

func TestReadExtentIgnoresStaleInodeEOF(t *testing.T) {
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	fs.inodes.Lookup("/", true, 0, time.Now())

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "stale-size.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	payload := []byte("SQLite format 3\x00more")
	if written, wst := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}, payload); wst != gofuse.OK || int(written) != len(payload) {
		t.Fatalf("Write written=%d status=%v", written, wst)
	}
	fs.inodes.UpdateSize(created.NodeId, 0)

	got, rst, err := readDat9FSTestRange(fs, created.NodeId, created.Fh, 0, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if rst != gofuse.OK {
		t.Fatalf("Read status=%v", rst)
	}
	if string(got) != string(payload) {
		t.Fatalf("read %q, want %q (stale inode size must not EOF extent overlay)", got, payload)
	}
}

func TestSQLiteWALIndexOpenFlagsKeepCache(t *testing.T) {
	fh := &FileHandle{
		Path:          "/data/app.db-shm",
		ContentLayout: client.ContentLayoutExtent,
	}
	if got := remoteOpenFlagsForHandle(fh); got != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("open flags=%d, want KEEP_CACHE for WAL index mmap", got)
	}
}

func TestExtentCompactGenerationDoesNotDropKeepCache(t *testing.T) {
	// JuiceFS OpenCache: while the open-file lives, later Opens keep cache
	// even if compact gen or write revision moved (mmap -shm coherence).
	of := newExtentOpenFile(4096)
	of.rev = 1
	of.gen = 1
	if of.takeKeepCache() {
		t.Fatal("first takeKeepCache = true, want false")
	}
	of.gen++
	of.rev++
	if !of.takeKeepCache() {
		t.Fatal("live open-file dropped KeepCache after gen/rev bump")
	}
}

func TestExtentOpenKeepCacheMatchesJuiceFS(t *testing.T) {
	// JuiceFS Open: KeepCache follows mtime (Drive9 revision), not compact
	// generation. First Open after a revision change does not KEEP_CACHE
	// and InodeNotify; compact InvalidateChunk is userspace only.
	fs := &Dat9FS{fileHandles: NewHandleTable[*FileHandle](), inodes: NewInodeToPath()}

	fh := &FileHandle{
		Ino: 7, Path: "/keep.db", ContentLayout: client.ContentLayoutExtent,
		OrigSize: 4096, BaseRev: 1, SliceGeneration: 1,
	}
	fs.attachExtentWriter(fh)
	before := fs.notifyCount.Load()
	if got := fs.openFlagsForHandle(fh); got != 0 {
		t.Fatalf("first open flags=%d, want 0 (JuiceFS !KeepCache + InodeNotify)", got)
	}
	if fs.notifyCount.Load() != before+1 {
		t.Fatalf("first open notifies=%d want %d", fs.notifyCount.Load()-before, 1)
	}
	if got := fs.openFlagsForHandle(fh); got != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("second open flags=%d, want KEEP_CACHE", got)
	}
	fh.extentOpen.mu.Lock()
	fh.extentOpen.gen++
	fh.extentOpen.rev++
	fh.extentOpen.mu.Unlock()
	if got := fs.openFlagsForHandle(fh); got != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("open of live inode after gen/rev bump flags=%d, want KEEP_CACHE (JuiceFS OpenCache)", got)
	}
	fs.detachExtentWriter(fh)
	fs.attachExtentWriter(fh)
	if got := fs.openFlagsForHandle(fh); got != 0 {
		t.Fatalf("open after last close flags=%d, want 0 (new open-file)", got)
	}
}

func TestGetAttrAfterExtentTruncateReportsNewSize(t *testing.T) {
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "trunc.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	payload := []byte("1234567-not-the-truncated-size")
	if written, wst := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}, payload); wst != gofuse.OK || int(written) != len(payload) {
		t.Fatalf("Write written=%d status=%v", written, wst)
	}

	var attr gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: created.NodeId},
			Size:     7,
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       created.Fh,
		},
	}, &attr)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status=%v", st)
	}
	if attr.Size != 7 {
		t.Fatalf("SetAttr size=%d want 7", attr.Size)
	}

	var out gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status=%v", st)
	}
	if out.Size != 7 {
		t.Fatalf("GetAttr size=%d want 7 (must not keep pre-truncate extentOpen size)", out.Size)
	}
}

func TestGetAttrPrefersLiveExtentSize(t *testing.T) {
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "size.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	payload := []byte("hello-extent-size")
	if written, wst := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}, payload); wst != gofuse.OK || int(written) != len(payload) {
		t.Fatalf("Write written=%d status=%v", written, wst)
	}
	fs.inodes.UpdateSize(created.NodeId, 1)

	var out gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status=%v", st)
	}
	if out.Size != uint64(len(payload)) {
		t.Fatalf("GetAttr size=%d want %d", out.Size, len(payload))
	}
	if out.AttrValid != 0 {
		t.Fatalf("AttrValid=%d want 0 while extent writes are live (JuiceFS ModifiedSince)", out.AttrValid)
	}
}

func TestExtentReadDoesNotShortReadWhenLiveSizeGrew(t *testing.T) {
	// JuiceFS VFS.Read uses writer length after Flush. A handle whose
	// OrigSize/Dirty lag GetAttr size must still return a full page
	// (SQLITE_IOERR_SHORT_READ is 522).
	page := bytes.Repeat([]byte{0x0d, 0x05, 0x00, 0x00}, 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write(page)
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 3, Generation: 1, SizeBytes: 8192,
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: 4096, BlockKey: "blocks/p", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)
	ino := fs.inodes.Lookup("/short.db", false, 8192, time.Now())
	fs.markDirtySize(ino, 8192)
	stale := &FileHandle{
		Ino:           ino,
		Path:          "/short.db",
		Dirty:         NewWriteBuffer("/short.db", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
		OrigSize:      0,
		BaseRev:       3,
	}
	fs.attachExtentWriter(stale)
	stale.extentOpen.clipSize(0)
	got, err := fs.readExtentRange(context.Background(), stale, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4096 {
		t.Fatalf("read %d bytes, want 4096 (short read is SQLITE_IOERR_SHORT_READ)", len(got))
	}
}

func TestReadExtentStaleHandleFetchesAfterSiblingCommit(t *testing.T) {
	header := []byte("SQLite format 3\x00")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write(header)
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 2, Generation: 1, SizeBytes: int64(len(header)),
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: int64(len(header)), BlockKey: "blocks/a", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		fileHandles: NewHandleTable[*FileHandle](),
		openHandles: NewOpenHandleIndex(),
		extentOpens: make(map[uint64]*extentOpenFile),
		extentCache: newExtentReadCache(1 << 20),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	writer := &FileHandle{
		Ino:           9,
		Path:          "/multi.db",
		Dirty:         NewWriteBuffer("/multi.db", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
		IsNew:         true,
	}
	fs.attachExtentWriter(writer)
	writer.extentWriter.writeAt(0, header)
	_ = writer.Dirty.SetSizeOnly(int64(len(header)))
	writer.extentOpen.noteCommit(2, 1, int64(len(header)))
	writer.extentWriter.dropAllSlices()

	stale := &FileHandle{
		Ino:           9,
		Path:          "/multi.db",
		Dirty:         NewWriteBuffer("/multi.db", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
		IsNew:         true,
		OrigSize:      0,
		BaseRev:       0,
	}
	fs.attachExtentWriter(stale)
	if stale.OrigSize != int64(len(header)) {
		t.Fatalf("stale OrigSize=%d, attach must adopt sibling committed size", stale.OrigSize)
	}
	got, err := fs.readExtentRange(context.Background(), stale, 0, int64(len(header)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(header) {
		t.Fatalf("read %q, want header after sibling commit dropped overlay", got)
	}
}

func TestRefreshExtentHandlesAfterCommitUpdatesSibling(t *testing.T) {
	fs := &Dat9FS{
		openHandles: NewOpenHandleIndex(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	a := &FileHandle{Ino: 4, Path: "/t.db", ContentLayout: client.ContentLayoutExtent, BaseRev: 1, OrigSize: 8, IsNew: true}
	b := &FileHandle{Ino: 4, Path: "/t.db", ContentLayout: client.ContentLayoutExtent, BaseRev: 1, OrigSize: 8, IsNew: true}
	fs.openHandles.Add(a)
	fs.openHandles.Add(b)
	fs.refreshExtentHandlesAfterCommit("/t.db", a, 3, 2, 64)
	if b.BaseRev != 3 || b.OrigSize != 64 || b.SliceGeneration != 2 || b.IsNew {
		t.Fatalf("sibling after commit = rev=%d size=%d gen=%d isNew=%t", b.BaseRev, b.OrigSize, b.SliceGeneration, b.IsNew)
	}
	if a.BaseRev != 1 {
		t.Fatalf("skip handle must stay at rev %d", a.BaseRev)
	}
}

func TestExpandExtentReadaheadDoesNotBranchOnSQLiteName(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*"}}}
	db := &FileHandle{Path: "/data/app.db"}
	other := &FileHandle{Path: "/fio/rand_rw"}
	offDB, nDB := fs.expandExtentReadahead(db, 4096, 4096, 1<<20)
	offOther, nOther := fs.expandExtentReadahead(other, 4096, 4096, 1<<20)
	if offDB != offOther || nDB != nOther {
		t.Fatalf("readahead must not depend on sqlite filename: db=%d+%d other=%d+%d", offDB, nDB, offOther, nOther)
	}
	if offDB != 4096 {
		t.Fatalf("JuiceFS readahead is forward-only, start=%d want 4096", offDB)
	}
}

func TestUnlinkFuseTimeoutSQLiteJournal(t *testing.T) {
	if got := unlinkFuseTimeout("/data/app.db-wal"); got != releaseTimeout(0) {
		t.Fatalf("wal unlink timeout = %s, want %s", got, releaseTimeout(0))
	}
	if got := unlinkFuseTimeout("/data/app.db-journal"); got != releaseTimeout(0) {
		t.Fatalf("journal unlink timeout = %s, want %s", got, releaseTimeout(0))
	}
	if got := unlinkFuseTimeout("/data/app.db"); got != fuseTimeout {
		t.Fatalf("main db unlink timeout = %s, want %s", got, fuseTimeout)
	}
}

func TestWriteExtentAttachesSharedOpenWriter(t *testing.T) {
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "share.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	first, ok := fs.fileHandles.Get(created.Fh)
	if !ok || first.extentWriter == nil {
		t.Fatal("create must attach an extent writer")
	}
	shared := first.extentWriter

	second := &FileHandle{
		Ino:           first.Ino,
		Path:          first.Path,
		Dirty:         NewWriteBuffer(first.Path, 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
	}
	fh := fs.fileHandles.Allocate(second)
	if written, wst := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       fh,
	}, []byte("x")); wst != gofuse.OK || written != 1 {
		t.Fatalf("Write written=%d status=%v", written, wst)
	}
	if second.extentWriter != shared {
		t.Fatal("Write must attach the per-inode shared writer, not a private buffer")
	}
}

func TestExtentCreateDoesNotBlockOnMetaCreate(t *testing.T) {
	// Drive9 meta is HTTP. Putting CreateFileWithLayout on VFS.Create made
	// wal-multiwrite miss --wait all. JuiceFS Meta.Create is local; Drive9
	// Create stays local-first and name-creates in ensureRemote.
	ts, creates := newExtentMetaOKServer(t)
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "journal.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	if creates.Load() != 0 {
		t.Fatalf("CreateFileWithLayout count=%d want 0 at VFS.Create", creates.Load())
	}
	fh, ok := fs.fileHandles.Get(created.Fh)
	if !ok || fh.extentWriter == nil {
		t.Fatal("missing extent writer")
	}
	if err := fh.extentWriter.ensureRemote(context.Background()); err != nil {
		t.Fatalf("ensureRemote: %v", err)
	}
	if creates.Load() != 1 {
		t.Fatalf("CreateFileWithLayout count=%d want 1 at ensureRemote", creates.Load())
	}
	if fh.extentWriter.fileID != "inode-1" {
		t.Fatalf("fileID=%q want inode-1", fh.extentWriter.fileID)
	}
}

func TestExtentEnsureRemoteDoesNotHoldRemoteCommitLock(t *testing.T) {
	// JuiceFS Meta.Create is a local txn. Holding remoteCommitLock across
	// HTTP CreateFileWithLayout deadlocked speedtest1-truncate (Write
	// flushwaiting vs SetAttr on the same path).
	createStarted := make(chan struct{})
	createRelease := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.RawQuery == "create=1" {
			once.Do(func() { close(createStarted) })
			<-createRelease
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Resource-ID", "inode-1")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "trunc.db", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	fh, ok := fs.fileHandles.Get(created.Fh)
	if !ok || fh.extentWriter == nil {
		t.Fatal("missing writer")
	}
	errCh := make(chan error, 1)
	go func() { errCh <- fh.extentWriter.ensureRemote(context.Background()) }()
	select {
	case <-createStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("CreateFileWithLayout did not start")
	}
	start := time.Now()
	unlock := fs.lockRemoteCommitPath(fh.Path)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("ensureRemote held remoteCommitLock across HTTP Meta.Create")
	}
	unlock()
	close(createRelease)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ensureRemote: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ensureRemote did not finish")
	}
}

func TestExtentUnlinkedWriterDoesNotRecreateName(t *testing.T) {
	// JuiceFS Unlink is name/nlink. A later flush of the unlinked fd must
	// not Meta.Create the path again (that 409-Stat leftover journal).
	ts, creates := newExtentMetaOKServer(t)
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "app.db-journal", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	first, ok := fs.fileHandles.Get(created.Fh)
	if !ok || first.extentWriter == nil {
		t.Fatal("missing writer")
	}
	st = fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "app.db-journal")
	if st != gofuse.OK {
		t.Fatalf("Unlink status=%v", st)
	}
	if err := first.extentWriter.ensureRemote(context.Background()); err != nil {
		t.Fatalf("ensureRemote after unlink: %v", err)
	}
	if creates.Load() != 0 {
		t.Fatalf("unlinked writer CreateFileWithLayout count=%d want 0", creates.Load())
	}
}

func TestExtentEnsureRemoteAfterUnlinkAllocatesNewInode(t *testing.T) {
	// JuiceFS Unlink is name/nlink then Create of the same name is a new
	// inode. The unlinked writer must not recreate the edge.
	ts, creates := newExtentMetaOKServer(t)
	opts := &MountOptions{ExtentPaths: []string{"*"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var created gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "app.db-journal", &created)
	if st != gofuse.OK {
		t.Fatalf("Create status=%v", st)
	}
	first, ok := fs.fileHandles.Get(created.Fh)
	if !ok {
		t.Fatal("missing first handle")
	}
	if err := first.extentWriter.ensureRemote(context.Background()); err != nil {
		t.Fatalf("ensureRemote: %v", err)
	}
	oldID := first.extentWriter.fileID
	if oldID == "" {
		t.Fatal("ensureRemote must bind a remote inode id")
	}

	st = fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "app.db-journal")
	if st != gofuse.OK {
		t.Fatalf("Unlink status=%v", st)
	}
	if err := first.extentWriter.ensureRemote(context.Background()); err != nil {
		t.Fatalf("unlinked ensureRemote: %v", err)
	}
	if creates.Load() != 1 {
		t.Fatalf("unlinked writer created again: count=%d", creates.Load())
	}

	var created2 gofuse.CreateOut
	st = fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "app.db-journal", &created2)
	if st != gofuse.OK {
		t.Fatalf("recreate status=%v", st)
	}
	second, ok := fs.fileHandles.Get(created2.Fh)
	if !ok || second.extentWriter == nil {
		t.Fatal("missing second handle")
	}
	if err := second.extentWriter.ensureRemote(context.Background()); err != nil {
		t.Fatalf("recreate ensureRemote: %v", err)
	}
	if second.extentWriter.fileID == "" || second.extentWriter.fileID == oldID {
		t.Fatalf("recreate fileID=%q old=%q; want a new inode after Unlink", second.extentWriter.fileID, oldID)
	}
	if created2.NodeId == created.NodeId {
		t.Fatalf("recreate reused local ino %d", created.NodeId)
	}
	if creates.Load() != 2 {
		t.Fatalf("CreateFileWithLayout count=%d want 2", creates.Load())
	}
}

func TestReadExtentIgnoresStaleHandleRevisionCache(t *testing.T) {
	header := []byte("SQLite format 3\x00")
	stale := []byte("STALE-HEADER!!!!")
	if len(stale) != len(header) {
		t.Fatalf("stale len %d want %d", len(stale), len(header))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write(header)
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 2, Generation: 1, SizeBytes: int64(len(header)),
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: int64(len(header)), BlockKey: "blocks/a", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		fileHandles: NewHandleTable[*FileHandle](),
		openHandles: NewOpenHandleIndex(),
		extentOpens: make(map[uint64]*extentOpenFile),
		extentCache: newExtentReadCache(1 << 20),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	fh := &FileHandle{
		Ino:           9,
		Path:          "/multi.db",
		Dirty:         NewWriteBuffer("/multi.db", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
		BaseRev:       1,
		OrigSize:      int64(len(header)),
	}
	fs.attachExtentWriter(fh)
	fh.extentOpen.noteCommit(2, 1, int64(len(header)))
	remote := fs.remotePath(fh.Path)
	fs.extentCache.putSlices(remote, 1, 1, int64(len(stale)), []client.SliceRow{{
		FileOff: 0, Len: int64(len(stale)), BlockKey: "blocks/stale", BlockOff: 0,
	}})
	fs.extentCache.putBlock("blocks/stale", 0, int64(len(stale)), stale)

	got, err := fs.readExtentRange(context.Background(), fh, 0, int64(len(header)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(header) {
		t.Fatalf("read %q, want live header not stale cache %q", got, stale)
	}
}
