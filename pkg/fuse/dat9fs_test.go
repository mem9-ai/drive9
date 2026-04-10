package fuse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

func newTestDat9FS(t *testing.T, size int64, get func(http.ResponseWriter, *http.Request)) (*Dat9FS, uint64, func()) {
	t.Helper()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			get(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())
	return fs, ino, ts.Close
}

func TestOpenWritableFailsWhenSmallFilePreloadFails(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 16, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.EIO {
		t.Fatalf("Open status = %v, want EIO", st)
	}
}

func TestOpenWritableLargeFileLazyPreload(t *testing.T) {
	// With lazy preload, Open() succeeds for large files even if the server
	// would fail on GET — the actual data loading is deferred to Write time.
	fs, ino, cleanup := newTestDat9FS(t, smallFileThreshold+1, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK (lazy preload defers loading)", st)
	}
}

func TestGetAttrPrefersDirtyHandleSize(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 4, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "data")
	})
	defer cleanup()

	fh := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0, 0),
	}
	if _, err := fh.Dirty.Write(0, []byte("dirty-size")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.fileHandles.Allocate(fh)

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: ino},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got, want := out.Size, uint64(len("dirty-size")); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
}

func TestGetAttrUsesLatestDirtyHandleSize(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 4, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "data")
	})
	defer cleanup()

	fh1 := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0, 0),
	}
	fh2 := &FileHandle{
		Ino:   ino,
		Path:  "/file.bin",
		Dirty: NewWriteBuffer("/file.bin", 0, 0),
	}
	fhID1 := fs.fileHandles.Allocate(fh1)
	fhID2 := fs.fileHandles.Allocate(fh2)

	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID1}, []byte("abc")); st != gofuse.OK {
		t.Fatalf("first write status = %v, want OK", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID2}, []byte("abcdefghi")); st != gofuse.OK {
		t.Fatalf("second write status = %v, want OK", st)
	}

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: ino},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got, want := out.Size, uint64(len("abcdefghi")); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
}

func TestGetAttrDirectoryDoesNotRequireRemoteStat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: dirIno}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("GetAttr mode = %o, want directory mode", out.Mode)
	}
}

func TestLookupFallsBackToParentListWhenDirStatUnsupported(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Path == "/v1/fs/" && r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []map[string]any{{
						"name":  "dir",
						"isDir": true,
						"size":  0,
					}},
				})
				return
			}
			http.NotFound(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "dir", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Lookup mode = %o, want directory mode", out.Mode)
	}
}

func TestOpenReadOnlyLargeFileGetsPrefetcher(t *testing.T) {
	size := int64(1024 * 1024) // 1MB — above smallFileThreshold
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	fs, ino, cleanup := newTestDat9FS(t, size, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data)
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.Prefetch == nil {
		t.Fatal("expected Prefetcher to be set for large read-only file")
	}
}

func TestOpenWritableLargeFileGetsLazyPreload(t *testing.T) {
	size := int64(1024 * 1024) // 1MB — above smallFileThreshold
	getCalled := false

	fs, ino, cleanup := newTestDat9FS(t, size, func(w http.ResponseWriter, r *http.Request) {
		getCalled = true
		_, _ = w.Write(make([]byte, size))
	})
	defer cleanup()

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	// With lazy preload, Open should NOT have fetched the file content
	if getCalled {
		t.Fatal("expected lazy preload — GET should not be called during Open")
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.Dirty == nil {
		t.Fatal("expected Dirty buffer to be set")
	}
	if fh.Dirty.LoadPart == nil {
		t.Fatal("expected LoadPart callback to be set for lazy preload")
	}
}

func TestDefaultTTLIs60Seconds(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	if opts.AttrTTL != 60*time.Second {
		t.Fatalf("default AttrTTL = %v, want 60s", opts.AttrTTL)
	}
	if opts.EntryTTL != 60*time.Second {
		t.Fatalf("default EntryTTL = %v, want 60s", opts.EntryTTL)
	}
}

func TestLookupReturnsTTLInEntryOut(t *testing.T) {
	fs, _, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "file.bin", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	// The entry timeout should match the configured TTL (60s default).
	// go-fuse stores timeouts in seconds + nanoseconds.
	if out.EntryValid < 59 {
		t.Fatalf("EntryValid = %d, want >= 59 (60s TTL)", out.EntryValid)
	}
	if out.AttrValid < 59 {
		t.Fatalf("AttrValid = %d, want >= 59 (60s TTL)", out.AttrValid)
	}
}

func TestInitStoresServer(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)
	if fs.server != nil {
		t.Fatal("server should be nil before Init")
	}
	// We can't easily create a real gofuse.Server in tests,
	// but we can verify that notifyEntry/notifyInode are safe
	// to call with a nil server (no panic).
	fs.notifyEntry(1, "test")
	fs.notifyInode(1)
}

func TestCreateFileGetsStreamUploader(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	// First we need to list root so inodes are populated
	_, _ = fs.client.List("/")

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "newfile.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.Streamer == nil {
		t.Fatal("expected StreamUploader to be set for new file")
	}
}

func TestStatFs_ReportsVirtualCapacity(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)

	var out gofuse.StatfsOut
	st := fs.StatFs(nil, &gofuse.InHeader{NodeId: 1}, &out)
	if st != gofuse.OK {
		t.Fatalf("StatFs status = %v, want OK", st)
	}
	if out.Bsize != 4096 {
		t.Fatalf("Bsize = %d, want 4096", out.Bsize)
	}
	if out.Frsize != 4096 {
		t.Fatalf("Frsize = %d, want 4096", out.Frsize)
	}
	if out.NameLen != 255 {
		t.Fatalf("NameLen = %d, want 255", out.NameLen)
	}
	// 1 TiB in 4K blocks
	const expectedBlocks = (1 << 40) / 4096
	if out.Blocks != expectedBlocks {
		t.Fatalf("Blocks = %d, want %d", out.Blocks, expectedBlocks)
	}
	if out.Bavail != expectedBlocks-1 {
		t.Fatalf("Bavail = %d, want %d", out.Bavail, expectedBlocks-1)
	}
}

func TestXAttr_GetReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)

	_, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: 1}, "user.test", nil)
	if st != gofuse.ENOATTR {
		t.Fatalf("GetXAttr status = %v, want ENOATTR", st)
	}
}

func TestXAttr_ListReturnsEmpty(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)

	n, st := fs.ListXAttr(nil, &gofuse.InHeader{NodeId: 1}, nil)
	if st != gofuse.OK {
		t.Fatalf("ListXAttr status = %v, want OK", st)
	}
	if n != 0 {
		t.Fatalf("ListXAttr size = %d, want 0", n)
	}
}

func TestXAttr_SetDiscardsSilently(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)

	st := fs.SetXAttr(nil, &gofuse.SetXAttrIn{}, "user.test", []byte("val"))
	if st != gofuse.OK {
		t.Fatalf("SetXAttr status = %v, want OK", st)
	}
}

func TestXAttr_RemoveReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)

	st := fs.RemoveXAttr(nil, &gofuse.InHeader{NodeId: 1}, "user.test")
	if st != gofuse.ENOATTR {
		t.Fatalf("RemoveXAttr status = %v, want ENOATTR", st)
	}
}

func TestSetAttr_MtimeUpdate(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	mtime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MTIME,
			Mtime:    uint64(mtime.Unix()),
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	// Verify the inode was updated
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if !entry.Mtime.Equal(mtime) {
		t.Fatalf("Mtime = %v, want %v", entry.Mtime, mtime)
	}

	// Verify the attr output has the correct mtime
	if out.Mtime != uint64(mtime.Unix()) {
		t.Fatalf("out.Mtime = %d, want %d", out.Mtime, mtime.Unix())
	}
}

func TestSetAttr_MtimeNow(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	before := time.Now()

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MTIME | gofuse.FATTR_MTIME_NOW,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	after := time.Now()

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Mtime.Before(before) || entry.Mtime.After(after) {
		t.Fatalf("Mtime = %v, expected between %v and %v", entry.Mtime, before, after)
	}
}

func TestLookup_UsesMtimeFromStat(t *testing.T) {
	mtime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(mtime.Unix(), 10))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "file.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mtime != uint64(mtime.Unix()) {
		t.Fatalf("Lookup mtime = %d, want %d", out.Mtime, mtime.Unix())
	}
}

// TestDebounce_ReleaseAfterFlush_NoDataLoss verifies that data is not lost
// when Release fires after a debounced Flush. This is a regression test for
// the scenario: Flush debounces → ClearDirty → Release cancels timer →
// flushHandle sees no dirty data → data never uploaded.
func TestDebounce_ReleaseAfterFlush_NoDataLoss(t *testing.T) {
	uploadedCh := make(chan []byte, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			uploadedCh <- append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 10 * time.Second} // long debounce — won't fire naturally
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	// Create a file
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "test.md", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	// Write data
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("important data"))
	if st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	// Flush (will debounce since file < smallFileThreshold and debounce > 0)
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}

	// Release (should cancel debounce and flush immediately)
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	// Verify data was uploaded (use channel to avoid race)
	select {
	case uploaded := <-uploadedCh:
		if string(uploaded) != "important data" {
			t.Fatalf("uploaded = %q, want %q", uploaded, "important data")
		}
	case <-time.After(time.Second):
		t.Fatal("data was never uploaded — data loss!")
	}
}
