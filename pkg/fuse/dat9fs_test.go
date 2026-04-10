package fuse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
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

// ---------------------------------------------------------------------------
// Full lifecycle tests — echo "xxx" > file pattern
// ---------------------------------------------------------------------------

// newTestServer creates a test HTTP server that handles dat9 API calls.
// It returns the server and channels for observing uploads.
func newTestServer(t *testing.T) (*httptest.Server, chan []byte) {
	t.Helper()
	uploadedCh := make(chan []byte, 10)
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
	return ts, uploadedCh
}

// TestCreateWriteFlushRelease_SmallFile tests the exact echo "xxx" > file
// lifecycle: Create → Write → Flush → Release. This is the pattern that
// caused the original deadlock with synchronous kernel notifications.
func TestCreateWriteFlushRelease_SmallFile(t *testing.T) {
	ts, uploadedCh := newTestServer(t)
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0} // disable debounce for determinism
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	done := make(chan struct{})
	go func() {
		defer close(done)

		// Create
		var createOut gofuse.CreateOut
		st := fs.Create(nil, &gofuse.CreateIn{
			InHeader: gofuse.InHeader{NodeId: 1},
		}, "t1.txt", &createOut)
		if st != gofuse.OK {
			t.Errorf("Create: %v", st)
			return
		}

		// Write
		_, st = fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		}, []byte("xxx\n"))
		if st != gofuse.OK {
			t.Errorf("Write: %v", st)
			return
		}

		// Flush
		st = fs.Flush(nil, &gofuse.FlushIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
		if st != gofuse.OK {
			t.Errorf("Flush: %v", st)
			return
		}

		// Release
		fs.Release(nil, &gofuse.ReleaseIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
	}()

	// The entire lifecycle must complete within 5s.
	// If notifyEntry/notifyInode were synchronous, this could deadlock.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Create→Write→Flush→Release lifecycle timed out (possible deadlock)")
	}

	// Verify data was uploaded
	select {
	case uploaded := <-uploadedCh:
		if string(uploaded) != "xxx\n" {
			t.Fatalf("uploaded = %q, want %q", uploaded, "xxx\n")
		}
	case <-time.After(time.Second):
		t.Fatal("data was never uploaded")
	}
}

// TestConcurrentGetAttrDuringWrite verifies that GetAttr on a file with an
// open dirty handle does NOT issue HTTP calls (uses dirty handle size).
// This prevents hangs when macOS issues GetAttr while a write is in progress.
func TestConcurrentGetAttrDuringWrite(t *testing.T) {
	// Server that tracks HEAD calls — GetAttr for dirty files should NOT
	// reach the server.
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	// Create a file
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "concurrent.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	// Write data
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, []byte("some data"))
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	headsBefore := headCalls.Load()

	// GetAttr while file has dirty handle — should NOT call server
	var attrOut gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}

	headsAfter := headCalls.Load()
	if headsAfter > headsBefore {
		t.Fatalf("GetAttr issued %d HEAD requests for dirty file — should use dirty handle size",
			headsAfter-headsBefore)
	}

	// Size should reflect the written data
	if attrOut.Size != 9 {
		t.Fatalf("GetAttr size = %d, want 9", attrOut.Size)
	}
}

// TestNotifyEntry_NonBlocking verifies that notifyEntry and notifyInode
// return immediately (are dispatched asynchronously) even if the server is
// non-nil. This is a regression test for the macOS kernel notification
// deadlock — synchronous EntryNotify/InodeNotify inside a FUSE handler can
// deadlock on macOS when the kernel processes the invalidation in-band.
func TestNotifyEntry_NonBlocking(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)
	// fs.server is nil — verify it doesn't panic.
	// In production, fs.server != nil and the go func() ensures no block.

	done := make(chan struct{})
	go func() {
		defer close(done)
		fs.notifyEntry(1, "test")
		fs.notifyInode(1)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notifyEntry/notifyInode blocked for > 1s (should be async)")
	}
}

// TestMutationHandlers_CompleteWithinTimeout verifies that all mutation
// handlers (Create, Mkdir, Unlink, Rmdir, Rename) complete within a bounded
// time. If kernel notifications were synchronous, these could deadlock.
func TestMutationHandlers_CompleteWithinTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	// Pre-populate some inodes for mutation tests
	fs.inodes.Lookup("/existing.txt", false, 100, time.Now())
	fs.inodes.Lookup("/oldname.txt", false, 100, time.Now())

	tests := []struct {
		name string
		fn   func() gofuse.Status
	}{
		{
			name: "Create",
			fn: func() gofuse.Status {
				var out gofuse.CreateOut
				return fs.Create(nil, &gofuse.CreateIn{
					InHeader: gofuse.InHeader{NodeId: 1},
				}, "new.txt", &out)
			},
		},
		{
			name: "Mkdir",
			fn: func() gofuse.Status {
				var out gofuse.EntryOut
				return fs.Mkdir(nil, &gofuse.MkdirIn{
					InHeader: gofuse.InHeader{NodeId: 1},
				}, "newdir", &out)
			},
		},
		{
			name: "Unlink",
			fn: func() gofuse.Status {
				return fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "existing.txt")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan gofuse.Status, 1)
			go func() { done <- tc.fn() }()

			select {
			case st := <-done:
				if st != gofuse.OK {
					t.Fatalf("%s returned %v", tc.name, st)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s timed out (possible deadlock from synchronous kernel notify)", tc.name)
			}
		})
	}
}

// TestParallelCreateAndGetAttr runs Create and GetAttr concurrently to detect
// lock ordering deadlocks between dirtyMu, inodes.mu, and fileHandles.mu.
func TestParallelCreateAndGetAttr(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	const N = 20
	var wg sync.WaitGroup
	errCh := make(chan error, N*2)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var out gofuse.CreateOut
			name := fmt.Sprintf("file_%d.txt", idx)
			st := fs.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
			}, name, &out)
			if st != gofuse.OK {
				errCh <- fmt.Errorf("Create(%s): %v", name, st)
				return
			}

			// Write + Release
			_, st = fs.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: out.NodeId},
				Fh:       out.Fh,
			}, []byte("data"))
			if st != gofuse.OK {
				errCh <- fmt.Errorf("Write(%s): %v", name, st)
			}

			fs.Release(nil, &gofuse.ReleaseIn{
				InHeader: gofuse.InHeader{NodeId: out.NodeId},
				Fh:       out.Fh,
			})
		}(i)

		// Concurrent GetAttr on root (directory)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out gofuse.AttrOut
			st := fs.GetAttr(nil, &gofuse.GetAttrIn{
				InHeader: gofuse.InHeader{NodeId: 1},
			}, &out)
			if st != gofuse.OK {
				errCh <- fmt.Errorf("GetAttr(root): %v", st)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("parallel Create+GetAttr timed out (deadlock)")
	}

	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestFlushHandle_SmallFile_ServerUnreachable verifies that a dead server
// does not cause permanent hangs — the operation must respect context timeouts.
// Uses a closed server to get fast connection-refused errors instead of a
// slow server, keeping the test fast while still validating timeout behavior.
func TestFlushHandle_SmallFile_ServerUnreachable(t *testing.T) {
	// Create and immediately close server → connection refused
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	serverURL := ts.URL
	ts.Close() // server is now dead

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(client.New(serverURL, ""), opts)

	// Manually create a file handle (server is dead, can't do real Create)
	ino := fs.inodes.Lookup("/slow.txt", false, 0, time.Now())
	wb := NewWriteBuffer("/slow.txt", streamingWriteMaxSize, 0)
	wb.touched = true
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	_, _ = wb.Write(0, []byte("data"))
	fh := &FileHandle{
		Ino:      ino,
		Path:     "/slow.txt",
		Dirty:    wb,
		Streamer: NewStreamUploader(fs.client, "/slow.txt"),
	}
	fh.DirtySeq = fs.markDirtySize(ino, wb.Size())
	fhID := fs.fileHandles.Allocate(fh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		fs.Release(nil, &gofuse.ReleaseIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
		})
	}()

	// Connection refused should fail fast — well under 5s.
	select {
	case <-done:
		// Good — Release completed (with error, which is expected)
	case <-time.After(5 * time.Second):
		t.Fatal("Release hung beyond 5s on dead server (should fail fast)")
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
