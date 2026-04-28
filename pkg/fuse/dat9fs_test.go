package fuse

import (
	"context"
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

func TestOpenWritableSmallFileLazyPreload(t *testing.T) {
	// With unified lazy preload, Open() succeeds for small files even if the
	// server would fail on GET — the actual data loading is deferred to first
	// Read/Write via LoadPart (same behavior as large files).
	fs, ino, cleanup := newTestDat9FS(t, 16, func(w http.ResponseWriter, r *http.Request) {
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

func TestCreateWriteThroughShadow(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR),
	}, "shadow.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Fh:       out.Fh,
		Offset:   0,
	}, []byte("hello")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	buf := make([]byte, 5)
	n, err := shadow.ReadAt("/shadow.txt", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 || string(buf) != "hello" {
		t.Fatalf("shadow read = %q (%d), want hello (5)", buf[:n], n)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("expected file handle to exist")
	}
	if !fh.ShadowReady {
		t.Fatal("expected created handle to remain shadow-backed")
	}
}

func TestOpenTruncateWriteThroughShadow(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 32, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "existing remote content that should be truncated")
	})
	defer cleanup()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   0,
	}, []byte("bye")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	if sz := shadow.Size("/file.bin"); sz != 3 {
		t.Fatalf("shadow size = %d, want 3", sz)
	}
	buf := make([]byte, 3)
	n, err := shadow.ReadAt("/file.bin", 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || string(buf) != "bye" {
		t.Fatalf("shadow data = %q (%d), want bye (3)", buf[:n], n)
	}
}

func TestFlushSkipsAsyncShadowForPartialExistingSnapshot(t *testing.T) {
	const size = int64(9 << 20) // > 8MB part size and < 10MB write-back threshold

	var mutateCalls atomic.Int32
	data := make([]byte, size)
	for i := range data {
		data[i] = 'a'
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(data)
		default:
			mutateCalls.Add(1)
			http.Error(w, "mutation not supported in test", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())

	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = writeBack
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   0,
	}, []byte("Z")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
	})
	if st == gofuse.OK {
		t.Fatal("Flush unexpectedly succeeded; partial existing snapshot should not use async local fast path")
	}
	if mutateCalls.Load() == 0 {
		t.Fatal("expected Flush to attempt a remote mutation after skipping async local staging")
	}
	if pending.Count() != 0 {
		t.Fatalf("pending index count = %d, want 0", pending.Count())
	}
	if shadow.Has("/file.bin") {
		t.Fatal("partial existing overwrite should not create a shadow fast-path snapshot")
	}
	if _, ok := writeBack.Get("/file.bin"); ok {
		t.Fatal("partial existing overwrite should not create a write-back snapshot")
	}
}

func TestOpenWritablePrefersPendingShadowSnapshot(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 5, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "stale")
	})
	defer cleanup()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	if err := shadow.WriteFull("/file.bin", []byte("fresh"), 9); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/file.bin", 5, PendingOverwrite, 9); err != nil {
		t.Fatal(err)
	}

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("expected file handle to exist")
	}
	if !fh.ShadowReady {
		t.Fatal("expected open to reuse pending shadow snapshot")
	}
	if fh.BaseRev != 9 {
		t.Fatalf("BaseRev = %d, want 9", fh.BaseRev)
	}

	buf := make([]byte, 16)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	data, _ := result.Bytes(buf)
	if string(data) != "fresh" {
		t.Fatalf("Read = %q, want fresh", data)
	}
}

func TestFlushLargeOverwritePatchCarriesExpectedRevision(t *testing.T) {
	const (
		fileSize = smallFileThreshold + 1024
		partSize = 5 << 20
	)

	var gotExpected atomic.Int64
	gotExpected.Store(-1)
	var uploadedBytes int
	var completeCalled bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/file.bin":
			var req struct {
				NewSize          int64  `json:"new_size"`
				DirtyParts       []int  `json:"dirty_parts"`
				PartSize         int64  `json:"part_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad patch body", http.StatusBadRequest)
				return
			}
			if req.ExpectedRevision == nil {
				http.Error(w, "missing expected_revision", http.StatusBadRequest)
				return
			}
			gotExpected.Store(*req.ExpectedRevision)
			if req.NewSize != fileSize {
				http.Error(w, "bad new_size", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.PatchPlan{
				UploadID: "patch-1",
				PartSize: partSize,
				UploadParts: []*client.PatchPartURL{{
					Number:    1,
					URL:       "http://" + r.Host + "/upload/1",
					Size:      fileSize,
					ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/upload/1":
			body, _ := io.ReadAll(r.Body)
			uploadedBytes = len(body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-1/complete":
			completeCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	ino := fs.inodes.Lookup("/file.bin", false, fileSize, time.Now())
	wb := NewWriteBuffer("/file.bin", 0, partSize)
	_, err := wb.Write(0, make([]byte, fileSize))
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:      ino,
		Path:     "/file.bin",
		Dirty:    wb,
		OrigSize: fileSize,
		BaseRev:  17,
	}
	fh.DirtySeq = fs.markDirtySize(ino, wb.Size())
	fhID := fs.fileHandles.Allocate(fh)

	st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	if gotExpected.Load() != 17 {
		t.Fatalf("expected_revision = %d, want 17", gotExpected.Load())
	}
	if uploadedBytes != int(fileSize) {
		t.Fatalf("uploaded bytes = %d, want %d", uploadedBytes, fileSize)
	}
	if !completeCalled {
		t.Fatal("patch complete was not called")
	}
}

func TestFlushNewLargeWriteStreamCarriesCreateIfAbsentRevision(t *testing.T) {
	const fileSize = smallFileThreshold + 2048

	var gotExpected atomic.Int64
	gotExpected.Store(-1)
	var completeParts int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad initiate body", http.StatusBadRequest)
				return
			}
			if req.ExpectedRevision == nil {
				http.Error(w, "missing expected_revision", http.StatusBadRequest)
				return
			}
			gotExpected.Store(*req.ExpectedRevision)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "up-1",
				"key":         "blobs/up-1",
				"part_size":   8 << 20,
				"total_parts": 1,
				"expires_at":  time.Now().Add(time.Minute).Format(time.RFC3339Nano),
				"resumable":   false,
				"checksum_contract": map[string]any{
					"supported": []string{"SHA-256"},
					"required":  false,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/up-1/presign-batch":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number":     1,
					"url":        "http://" + r.Host + "/upload/1",
					"size":       fileSize,
					"headers":    map[string]string{},
					"expires_at": time.Now().Add(time.Minute).Format(time.RFC3339Nano),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/upload/1":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"etag-1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/up-1/complete":
			var req struct {
				Parts []struct {
					Number int    `json:"number"`
					ETag   string `json:"etag"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad complete body", http.StatusBadRequest)
				return
			}
			completeParts = len(req.Parts)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	ino := fs.inodes.Lookup("/new.bin", false, 0, time.Now())
	wb := NewWriteBuffer("/new.bin", 0, 0)
	_, err := wb.Write(0, make([]byte, fileSize))
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:      ino,
		Path:     "/new.bin",
		Dirty:    wb,
		OrigSize: 0,
		IsNew:    true,
	}
	fh.DirtySeq = fs.markDirtySize(ino, wb.Size())
	fhID := fs.fileHandles.Allocate(fh)

	st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	if gotExpected.Load() != 0 {
		t.Fatalf("expected_revision = %d, want 0", gotExpected.Load())
	}
	if completeParts != 1 {
		t.Fatalf("complete parts = %d, want 1", completeParts)
	}
}

func TestWriteBufferCanMaterializeFull(t *testing.T) {
	wb := NewWriteBuffer("/file.bin", 0, 8)
	wb.totalSize = 16
	wb.remoteSize = 16
	wb.parts[0] = []byte("12345678")
	if wb.CanMaterializeFull() {
		t.Fatal("expected partial remote coverage to be unsafe for full materialization")
	}

	wb.parts[1] = []byte("abcdefgh")
	if !wb.CanMaterializeFull() {
		t.Fatal("expected complete remote coverage to be safe for full materialization")
	}

	if err := wb.Truncate(4); err != nil {
		t.Fatal(err)
	}
	delete(wb.parts, 1)
	if !wb.CanMaterializeFull() {
		t.Fatal("truncate should drop the need to materialize removed remote ranges")
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

func TestGetAttrRetriesTransientCanceledStat(t *testing.T) {
	var headCalls atomic.Int32
	firstHeadStarted := make(chan struct{}, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if headCalls.Add(1) == 1 {
				firstHeadStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			w.Header().Set("Content-Length", "99")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "3")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/retry-getattr.bin", false, 0, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	var out gofuse.AttrOut
	go func() {
		done <- fs.GetAttr(cancel, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	}()

	select {
	case <-firstHeadStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first HEAD request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("GetAttr status = %v, want OK", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetAttr timed out")
	}

	if got, want := out.Size, uint64(99); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
	if headCalls.Load() < 2 {
		t.Fatalf("HEAD calls = %d, want at least 2", headCalls.Load())
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

func TestLookupRetriesTransientCanceledStat(t *testing.T) {
	var headCalls atomic.Int32
	firstHeadStarted := make(chan struct{}, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if headCalls.Add(1) == 1 {
				firstHeadStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	cancel := make(chan struct{})
	var out gofuse.EntryOut
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "file.bin", &out)
	}()

	select {
	case <-firstHeadStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first HEAD request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Lookup status = %v, want OK", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup timed out")
	}

	if out.NodeId == 0 {
		t.Fatal("Lookup returned zero NodeId")
	}
	if headCalls.Load() < 2 {
		t.Fatalf("HEAD calls = %d, want at least 2", headCalls.Load())
	}
	if total, success, exhausted := fs.lookupRetryStats(); total != 1 || success != 1 || exhausted != 0 {
		t.Fatalf("lookup retry stats = (%d,%d,%d), want (1,1,0)", total, success, exhausted)
	}
}

func TestLookupReturnsENOENTAfterTransientCanceledStat(t *testing.T) {
	var headCalls atomic.Int32
	firstHeadStarted := make(chan struct{}, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if headCalls.Add(1) == 1 {
				firstHeadStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Path == "/v1/fs/" && r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
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

	cancel := make(chan struct{})
	var out gofuse.EntryOut
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "missing.bin", &out)
	}()

	select {
	case <-firstHeadStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first HEAD request")
	}

	select {
	case st := <-done:
		if st != gofuse.ENOENT {
			t.Fatalf("Lookup status = %v, want ENOENT", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup timed out")
	}

	if headCalls.Load() < 2 {
		t.Fatalf("HEAD calls = %d, want at least 2", headCalls.Load())
	}
	if total, success, exhausted := fs.lookupRetryStats(); total != 1 || success != 0 || exhausted != 0 {
		t.Fatalf("lookup retry stats = (%d,%d,%d), want (1,0,0)", total, success, exhausted)
	}
}

func TestLookupTransientRetryExhaustedReturnsEAGAIN(t *testing.T) {
	var headCalls atomic.Int32
	firstHeadStarted := make(chan struct{}, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if headCalls.Add(1) == 1 {
				firstHeadStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			http.Error(w, "temporary unavailable", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	cancel := make(chan struct{})
	var out gofuse.EntryOut
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "retry.bin", &out)
	}()

	select {
	case <-firstHeadStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first HEAD request")
	}

	select {
	case st := <-done:
		want := gofuse.Status(syscall.EAGAIN)
		if st != want {
			t.Fatalf("Lookup status = %v, want %v", st, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup timed out")
	}

	wantCalls := int32(1 + lookupTransientRetryCount)
	if got := headCalls.Load(); got != wantCalls {
		t.Fatalf("HEAD calls = %d, want %d", got, wantCalls)
	}
	if total, success, exhausted := fs.lookupRetryStats(); total != 1 || success != 0 || exhausted != 1 {
		t.Fatalf("lookup retry stats = (%d,%d,%d), want (1,0,1)", total, success, exhausted)
	}
}

func TestHTTPToFuseStatus_ContextErrorsMapToEAGAIN(t *testing.T) {
	want := gofuse.Status(syscall.EAGAIN)
	if got := httpToFuseStatus(context.Canceled); got != want {
		t.Fatalf("context canceled status = %v, want %v", got, want)
	}
	if got := httpToFuseStatus(context.DeadlineExceeded); got != want {
		t.Fatalf("context deadline status = %v, want %v", got, want)
	}
}

func TestHTTPToFuseStatus_MapsGatewayTimeoutToEAGAIN(t *testing.T) {
	want := gofuse.Status(syscall.EAGAIN)
	if got := httpToFuseStatus(&client.StatusError{StatusCode: http.StatusGatewayTimeout, Message: "gateway timeout"}); got != want {
		t.Fatalf("status error 504 = %v, want %v", got, want)
	}
	if got := httpToFuseStatus(fmt.Errorf("HTTP 504: upstream timeout")); got != want {
		t.Fatalf("string error 504 = %v, want %v", got, want)
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

func TestDefaultTTLIs10Seconds(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	if opts.AttrTTL != 10*time.Second {
		t.Fatalf("default AttrTTL = %v, want 10s", opts.AttrTTL)
	}
	if opts.EntryTTL != 10*time.Second {
		t.Fatalf("default EntryTTL = %v, want 10s", opts.EntryTTL)
	}
	if opts.NegativeEntryTTL != 10*time.Second {
		t.Fatalf("default NegativeEntryTTL = %v, want 10s", opts.NegativeEntryTTL)
	}
}

func TestMountOptionsLookupRetryDefaultsAndDisableSentinel(t *testing.T) {
	defaults := &MountOptions{}
	defaults.setDefaults()
	if defaults.LookupRetryCount != lookupTransientRetryCount {
		t.Fatalf("default LookupRetryCount = %d, want %d", defaults.LookupRetryCount, lookupTransientRetryCount)
	}
	if defaults.LookupRetryTimeout != lookupTransientRetryTimeout {
		t.Fatalf("default LookupRetryTimeout = %v, want %v", defaults.LookupRetryTimeout, lookupTransientRetryTimeout)
	}

	disabled := &MountOptions{LookupRetryCount: -1, LookupRetryTimeout: 250 * time.Millisecond}
	disabled.setDefaults()
	if disabled.LookupRetryCount != 0 {
		t.Fatalf("disabled LookupRetryCount = %d, want 0", disabled.LookupRetryCount)
	}
	if disabled.LookupRetryTimeout != 250*time.Millisecond {
		t.Fatalf("disabled LookupRetryTimeout = %v, want 250ms", disabled.LookupRetryTimeout)
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
	// The entry timeout should match the configured TTL (10s default).
	// go-fuse stores timeouts in seconds + nanoseconds.
	if out.EntryValid < 10 || out.EntryValid > 11 {
		t.Fatalf("EntryValid = %d, want ~10 (10s TTL)", out.EntryValid)
	}
	if out.AttrValid < 10 || out.AttrValid > 11 {
		t.Fatalf("AttrValid = %d, want ~10 (10s TTL)", out.AttrValid)
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

func TestSetAttr_TruncateWithoutHandleRefreshesRevision(t *testing.T) {
	var currentRevision atomic.Int64
	currentRevision.Store(1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(currentRevision.Load(), 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if len(body) != 0 {
				t.Fatalf("truncate write body = %q, want empty", string(body))
			}
			currentRevision.Store(2)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
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
	ino := fs.inodes.Lookup("/file.bin", false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	var attrOut gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Revision != 2 {
		t.Fatalf("inode revision = %d, want 2", entry.Revision)
	}

	var openOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.BaseRev != 2 {
		t.Fatalf("open base revision = %d, want 2", fh.BaseRev)
	}
}

func TestFlushHandle_UsesCommittedRevisionWithoutPostFlushStat(t *testing.T) {
	var (
		mu         sync.Mutex
		handlerErr error
		putCalls   atomic.Int32
		headCalls  atomic.Int32
	)
	recordHandlerErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "7" {
				recordHandlerErr(fmt.Errorf("X-Dat9-Expected-Revision = %q, want %q", got, "7"))
				http.Error(w, "bad expected revision", http.StatusBadRequest)
				return
			}
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 8})
		case http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "unexpected post-flush HEAD", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/flush.bin", false, 4, time.Now())
	fs.inodes.UpdateRevision(ino, 7)

	fh := &FileHandle{
		Ino:     ino,
		Path:    "/flush.bin",
		Dirty:   NewWriteBuffer("/flush.bin", maxPreloadSize, 0),
		BaseRev: 7,
	}
	if _, err := fh.Dirty.Write(0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	mu.Lock()
	err := handlerErr
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if fh.BaseRev != 8 {
		t.Fatalf("fh.BaseRev = %d, want 8", fh.BaseRev)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Revision != 8 {
		t.Fatalf("inode revision = %d, want 8", entry.Revision)
	}
}

func TestFlushHandle_SmallFile_SeedsReadCache(t *testing.T) {
	var (
		mu         sync.Mutex
		handlerErr error
		putCalls   atomic.Int32
		getCalls   atomic.Int32
		headCalls  atomic.Int32
	)
	recordHandlerErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	content := []byte("hello freshness")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 2})
		case http.MethodGet:
			getCalls.Add(1)
			recordHandlerErr(fmt.Errorf("unexpected GET after flush — should hit cache"))
			_, _ = w.Write(content)
		case http.MethodHead:
			headCalls.Add(1)
			recordHandlerErr(fmt.Errorf("unexpected HEAD after flush — should hit cache"))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	ino := fs.inodes.Lookup("/fresh.txt", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.inodes.UpdateSize(ino, int64(len(content)))

	fh := &FileHandle{
		Ino:     ino,
		Path:    "/fresh.txt",
		Dirty:   NewWriteBuffer("/fresh.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	if _, err := fh.Dirty.Write(0, content); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}

	// Verify readCache was seeded with committed revision.
	data, ok := fs.readCache.Get("/fresh.txt", 2)
	if !ok {
		t.Fatal("readCache miss after flush — freshness not seeded")
	}
	if string(data) != string(content) {
		t.Fatalf("readCache content = %q, want %q", data, content)
	}

	// Verify no GET or HEAD was made (cache should serve reads).
	mu.Lock()
	err := handlerErr
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}

	// Verify inode revision updated to committed value.
	if fh.BaseRev != 2 {
		t.Fatalf("fh.BaseRev = %d, want 2", fh.BaseRev)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Revision != 2 {
		t.Fatalf("inode revision = %d, want 2", entry.Revision)
	}
}

func TestFinalizeHandleFlushLocked_ResetsStreamerToCommittedRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)
	ino := fs.inodes.Lookup("/stream.bin", false, 0, time.Now())

	fh := &FileHandle{
		Ino:      ino,
		Path:     "/stream.bin",
		Dirty:    NewWriteBuffer("/stream.bin", maxPreloadSize, 0),
		BaseRev:  11,
		Streamer: NewStreamUploader(nil, "/stream.bin", 11),
	}
	fh.Streamer.started = true
	fh.Streamer.streamedParts[1] = true

	fh.Lock()
	fs.finalizeHandleFlushLocked(fh, 11)
	fh.Unlock()

	if fh.BaseRev != 12 {
		t.Fatalf("fh.BaseRev = %d, want 12", fh.BaseRev)
	}
	if got := fh.Streamer.ExpectedRevision(); got != 12 {
		t.Fatalf("streamer expected revision = %d, want 12", got)
	}
	if fh.Streamer.Started() {
		t.Fatal("streamer should be reset to not-started after successful flush")
	}
	if fh.Streamer.HasStreamedParts() {
		t.Fatal("streamer should clear prior streamed parts after successful flush")
	}
}

func TestSetAttr_PathTruncateRefreshesOpenHandleBaseRevision(t *testing.T) {
	const callerPID = 4242

	var (
		mu         sync.Mutex
		revision   int64 = 1
		content          = []byte("orig")
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				recordHandlerErr(fmt.Errorf("read body: %w", err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expected := r.Header.Get("X-Dat9-Expected-Revision")
			if expected != "" && expected != strconv.FormatInt(revision, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			content = append([]byte(nil), body...)
			revision++
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write(content)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, revision)
	fs.inodes.UpdateSize(ino, int64(len(content)))

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: callerPID}},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.BaseRev != 1 {
		t.Fatalf("open base revision = %d, want 1", fh.BaseRev)
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: callerPID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	if fh.BaseRev != 2 {
		t.Fatalf("open handle base revision after path truncate = %d, want 2", fh.BaseRev)
	}
	if fh.Streamer == nil {
		t.Fatal("expected stream uploader on O_TRUNC handle")
	}
	fh.Streamer.mu.Lock()
	streamerRevision := fh.Streamer.expectedRevision
	fh.Streamer.mu.Unlock()
	if streamerRevision != 2 {
		t.Fatalf("streamer expected revision after path truncate = %d, want 2", streamerRevision)
	}

	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
		Offset:   0,
	}, []byte("overwrite")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	mu.Lock()
	if handlerErr != nil {
		mu.Unlock()
		t.Fatal(handlerErr)
	}
	defer mu.Unlock()
	if got := string(content); got != "overwrite" {
		t.Fatalf("remote content = %q, want %q", got, "overwrite")
	}
	if revision != 3 {
		t.Fatalf("remote revision = %d, want 3", revision)
	}
}

func TestSetAttr_PathTruncateSingleCallerWriterAdoptsZeroBase(t *testing.T) {
	const callerPID = 5151

	var (
		mu         sync.Mutex
		revision   int64 = 1
		content          = []byte("orig")
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				recordHandlerErr(fmt.Errorf("read body: %w", err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expected := r.Header.Get("X-Dat9-Expected-Revision")
			if expected != "" && expected != strconv.FormatInt(revision, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			content = append([]byte(nil), body...)
			revision++
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write(content)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, revision)
	fs.inodes.UpdateSize(ino, int64(len(content)))

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: callerPID}},
		Flags:    uint32(syscall.O_WRONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fh, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: callerPID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	if fh.BaseRev != 2 {
		t.Fatalf("open handle base revision after path truncate = %d, want 2", fh.BaseRev)
	}
	if !fh.ZeroBase {
		t.Fatal("expected same-caller writer handle to adopt zero base")
	}
	if got := fh.Dirty.Size(); got != 0 {
		t.Fatalf("dirty size after path truncate = %d, want 0", got)
	}

	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
		Offset:   0,
	}, []byte("overwrite")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	fh.Lock()
	flushStatus := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if flushStatus != gofuse.OK {
		t.Fatalf("flush status = %v, want %v", flushStatus, gofuse.OK)
	}

	mu.Lock()
	if handlerErr != nil {
		mu.Unlock()
		t.Fatal(handlerErr)
	}
	defer mu.Unlock()
	if got := string(content); got != "overwrite" {
		t.Fatalf("remote content = %q, want %q", got, "overwrite")
	}
	if revision != 3 {
		t.Fatalf("remote revision = %d, want 3", revision)
	}
}

func TestSetAttr_PathTruncateDoesNotRefreshStaleWriterHandle(t *testing.T) {
	const (
		stalePID    = 7001
		truncatePID = 7002
	)

	var (
		mu         sync.Mutex
		revision   int64 = 1
		content          = []byte("orig")
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				recordHandlerErr(fmt.Errorf("read body: %w", err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expected := r.Header.Get("X-Dat9-Expected-Revision")
			if expected != "" && expected != strconv.FormatInt(revision, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			content = append([]byte(nil), body...)
			revision++
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write(content)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, revision)
	fs.inodes.UpdateSize(ino, int64(len(content)))

	var staleOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: stalePID}},
		Flags:    uint32(syscall.O_WRONLY),
	}, &staleOut)
	if st != gofuse.OK {
		t.Fatalf("stale writer Open status = %v, want OK", st)
	}
	staleFH, ok := fs.fileHandles.Get(staleOut.Fh)
	if !ok {
		t.Fatal("stale writer handle not found")
	}

	var truncOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: truncatePID}},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &truncOut)
	if st != gofuse.OK {
		t.Fatalf("truncate handle Open status = %v, want OK", st)
	}
	truncFH, ok := fs.fileHandles.Get(truncOut.Fh)
	if !ok {
		t.Fatal("truncate handle not found")
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: truncatePID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	if staleFH.BaseRev != 1 {
		t.Fatalf("stale writer base revision = %d, want 1", staleFH.BaseRev)
	}
	if truncFH.BaseRev != 2 {
		t.Fatalf("truncate handle base revision = %d, want 2", truncFH.BaseRev)
	}

	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       staleOut.Fh,
		Offset:   0,
	}, []byte("stale")); st != gofuse.OK {
		t.Fatalf("stale writer Write status = %v, want OK", st)
	}

	staleFH.Lock()
	flushStatus := fs.flushHandle(context.Background(), staleFH)
	staleFH.Unlock()
	if flushStatus != gofuse.EIO {
		t.Fatalf("stale writer flush status = %v, want %v", flushStatus, gofuse.EIO)
	}

	mu.Lock()
	if handlerErr != nil {
		mu.Unlock()
		t.Fatal(handlerErr)
	}
	defer mu.Unlock()
	if got := string(content); got != "" {
		t.Fatalf("remote content after stale writer conflict = %q, want empty", got)
	}
	if revision != 2 {
		t.Fatalf("remote revision after stale writer conflict = %d, want 2", revision)
	}
}

func TestSetAttr_PathTruncateSingleStaleWriterHandleKeepsOriginalRevision(t *testing.T) {
	const (
		stalePID    = 8001
		truncatePID = 8002
	)

	var (
		mu         sync.Mutex
		revision   int64 = 1
		content          = []byte("orig")
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				recordHandlerErr(fmt.Errorf("read body: %w", err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			expected := r.Header.Get("X-Dat9-Expected-Revision")
			if expected != "" && expected != strconv.FormatInt(revision, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			content = append([]byte(nil), body...)
			revision++
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				return
			}
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write(content)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, revision)
	fs.inodes.UpdateSize(ino, int64(len(content)))

	var staleOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: stalePID}},
		Flags:    uint32(syscall.O_WRONLY),
	}, &staleOut)
	if st != gofuse.OK {
		t.Fatalf("stale writer Open status = %v, want OK", st)
	}
	staleFH, ok := fs.fileHandles.Get(staleOut.Fh)
	if !ok {
		t.Fatal("stale writer handle not found")
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: truncatePID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	if staleFH.BaseRev != 1 {
		t.Fatalf("single stale writer base revision = %d, want 1", staleFH.BaseRev)
	}

	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       staleOut.Fh,
		Offset:   0,
	}, []byte("stale")); st != gofuse.OK {
		t.Fatalf("single stale writer Write status = %v, want OK", st)
	}

	staleFH.Lock()
	flushStatus := fs.flushHandle(context.Background(), staleFH)
	staleFH.Unlock()
	if flushStatus != gofuse.EIO {
		t.Fatalf("single stale writer flush status = %v, want %v", flushStatus, gofuse.EIO)
	}

	mu.Lock()
	if handlerErr != nil {
		mu.Unlock()
		t.Fatal(handlerErr)
	}
	defer mu.Unlock()
	if got := string(content); got != "" {
		t.Fatalf("remote content after single stale writer conflict = %q, want empty", got)
	}
	if revision != 2 {
		t.Fatalf("remote revision after single stale writer conflict = %d, want 2", revision)
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

	// Set a non-nil server so notifyEntry/notifyInode actually enter the
	// async goroutine path (a zero-value Server returns ENOSYS immediately
	// from EntryNotify/InodeNotify, which is fine — we just need the
	// goroutine to be spawned and complete).
	fs.Init(&gofuse.Server{})

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

	// Wait for the async goroutines to complete and verify WaitGroup drains.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		fs.notifyWg.Wait()
	}()
	select {
	case <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("notifyWg.Wait() blocked — async notifications not completing")
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
	fs.inodes.Lookup("/existingdir", true, 0, time.Now())

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
		{
			name: "Rmdir",
			fn: func() gofuse.Status {
				return fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "existingdir")
			},
		},
		{
			name: "Rename",
			fn: func() gofuse.Status {
				return fs.Rename(nil, &gofuse.RenameIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Newdir:   1,
				}, "oldname.txt", "renamed.txt")
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
		Streamer: NewStreamUploader(fs.client, "/slow.txt", -1),
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

func TestReleaseTimeoutScaling(t *testing.T) {
	tests := []struct {
		size    int64
		wantMin time.Duration
		wantMax time.Duration
	}{
		{0, 60 * time.Second, 60 * time.Second},                   // small file: floor
		{10 << 20, 60 * time.Second, 60 * time.Second},            // 10 MB: still floor
		{1 << 30, 200 * time.Second, 220 * time.Second},           // 1 GiB: ~205s
		{100 << 30, 15 * time.Minute, 15 * time.Minute},           // 100 GiB: capped at 15min
	}
	for _, tt := range tests {
		got := releaseTimeout(tt.size)
		if got < tt.wantMin || got > tt.wantMax {
			t.Errorf("releaseTimeout(%d) = %v, want [%v, %v]", tt.size, got, tt.wantMin, tt.wantMax)
		}
	}
}

// largeFlushStreamingMock returns an httptest server that accepts a full
// streaming upload flow (initiate → presign-batch → PUT parts → complete) and
// answers HEAD requests so subsequent Lookup/Stat calls can verify visibility.
// completeCh fires once /complete is observed (one event per upload).
func largeFlushStreamingMock(t *testing.T, fileSize int64, completeCh chan<- struct{}) *httptest.Server {
	t.Helper()
	var (
		mu        sync.Mutex
		uploaded  bool // set true after /complete
		etagSeq   int
	)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			mu.Lock()
			done := uploaded
			mu.Unlock()
			if !done {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "up-large",
				"key":         "blobs/up-large",
				"part_size":   8 << 20,
				"total_parts": (fileSize + (8 << 20) - 1) / (8 << 20),
				"expires_at":  time.Now().Add(time.Minute).Format(time.RFC3339Nano),
				"resumable":   false,
				"checksum_contract": map[string]any{
					"supported": []string{"SHA-256"},
					"required":  false,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/up-large/presign":
			var req struct {
				PartNumber int `json:"part_number"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":     req.PartNumber,
				"url":        "http://" + r.Host + "/upload/" + strconv.Itoa(req.PartNumber),
				"size":       8 << 20,
				"headers":    map[string]string{},
				"expires_at": time.Now().Add(time.Minute).Format(time.RFC3339Nano),
			})
		case r.Method == http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			etagSeq++
			etag := fmt.Sprintf(`"etag-%d"`, etagSeq)
			mu.Unlock()
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/up-large/complete":
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			uploaded = true
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			select {
			case completeCh <- struct{}{}:
			default:
			}
		default:
			http.NotFound(w, r)
		}
	}))
}

// TestFlushLargeFile_StrictUploadsBeforeReturning is the primary regression
// guard for the juicefs bench failure (#337 follow-up).
//
// In strict mode, Flush MUST upload the file to the server before returning.
// Otherwise, applications that close()→drop_caches→open() (juicefs bench, fio,
// some sync tools) see ENOENT because the kernel re-issues Lookup, the dentry
// cache is empty, and the remote stat has not yet observed the upload.
//
// Repro path that previously failed:
//   1. Create + Write 10 MiB
//   2. Flush (was: returned OK without uploading)
//   3. Lookup the path → must succeed
func TestFlushLargeFile_StrictUploadsBeforeReturning(t *testing.T) {
	const fileSize = int64(writeBackThreshold) // 10 MiB — minimal trigger of the large-file path

	completeCh := make(chan struct{}, 1)
	ts := largeFlushStreamingMock(t, fileSize, completeCh)
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.syncMode = SyncStrict

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR),
	}, "big.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	// Write the file in 1 MiB chunks (sequential writes drive the streaming uploader).
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for off := int64(0); off < fileSize; off += int64(len(chunk)) {
		_, st = fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   uint64(off),
		}, chunk)
		if st != gofuse.OK {
			t.Fatalf("Write @%d: %v", off, st)
		}
	}

	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}

	// Strict mode must have completed the upload by the time Flush returns.
	select {
	case <-completeCh:
	default:
		t.Fatal("Flush returned OK but /complete was never received — close→drop→open will see ENOENT")
	}

	// Lookup must now resolve via remote stat (no pendingIndex configured here).
	var entryOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "big.bin", &entryOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup after Flush: %v, want OK", st)
	}
	if entryOut.Size != uint64(fileSize) {
		t.Fatalf("Lookup size = %d, want %d", entryOut.Size, fileSize)
	}
}

// TestFlushLargeFile_InteractiveStagesShadowAndPendingIndex verifies that in
// interactive mode the same close→drop→open sequence is served from the local
// shadow + pendingIndex, without blocking Flush on a network upload. The
// CommitQueue takes over the actual server write asynchronously.
//
// We assert two invariants:
//   1. Flush returns quickly without contacting the upload endpoints.
//   2. A subsequent Lookup hits pendingIndex (no remote stat needed).
func TestFlushLargeFile_InteractiveStagesShadowAndPendingIndex(t *testing.T) {
	const fileSize = int64(writeBackThreshold) // 10 MiB

	var uploadAttempted atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Any /v2/uploads/* hit during Flush would mean we accidentally went
		// through the synchronous path. HEAD is allowed (Lookup may still
		// fall through if pendingIndex misses, which is the bug we're guarding).
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound) // server has nothing yet
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate" {
			uploadAttempted.Store(true)
		}
		// Keep the CommitQueue worker happy enough that it doesn't spam errors,
		// but we don't actually care if the background upload "succeeds" here —
		// the test asserts on the local overlay.
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := client.New(ts.URL, "")
	fs := NewDat9FS(c, opts)
	fs.syncMode = SyncInteractive

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	// Wire a CommitQueue so the Release path enqueues asynchronously instead
	// of falling back to a synchronous uploader (which would defeat the test).
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer cq.DrainAll()
	fs.commitQueue = cq

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR),
	}, "interactive.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for off := int64(0); off < fileSize; off += int64(len(chunk)) {
		_, st = fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   uint64(off),
		}, chunk)
		if st != gofuse.OK {
			t.Fatalf("Write @%d: %v", off, st)
		}
	}

	flushStart := time.Now()
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	flushDur := time.Since(flushStart)
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
	// Interactive Flush should be local-only — no upload initiate during Flush.
	if uploadAttempted.Load() {
		t.Fatal("interactive Flush hit upload endpoints — expected stage-only fast path")
	}
	// Sanity: even on slow CI a 10 MiB shadow stage should be well under 5s.
	if flushDur > 5*time.Second {
		t.Fatalf("interactive Flush took %v — should be local fsync only", flushDur)
	}

	// pendingIndex must contain the file with the correct size.
	meta, ok := pending.GetMeta("/interactive.bin")
	if !ok {
		t.Fatal("pendingIndex missing entry after interactive Flush — Lookup will ENOENT")
	}
	if meta.Size != fileSize {
		t.Fatalf("pendingIndex size = %d, want %d", meta.Size, fileSize)
	}

	// Lookup must hit the in-memory overlay without remote stat.
	var entryOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "interactive.bin", &entryOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v, want OK", st)
	}
	if entryOut.Size != uint64(fileSize) {
		t.Fatalf("Lookup size = %d, want %d", entryOut.Size, fileSize)
	}
}

// ---------------------------------------------------------------------------
// Read detached retry tests
// ---------------------------------------------------------------------------

// TestReadSmallFileRetryOnTransient verifies that a transient error on the
// first ReadCtx attempt triggers a detached retry that succeeds, returning
// data instead of EAGAIN.
func TestReadSmallFileRetryOnTransient(t *testing.T) {
	var getCalls atomic.Int32
	fileData := []byte("hello retry")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(fileData)))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			n := getCalls.Add(1)
			if n == 1 {
				// First GET: simulate transient failure
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			// Retry succeeds
			w.Header().Set("Content-Length", strconv.Itoa(len(fileData)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fileData)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	// Register root inode and do a lookup to populate inode entry
	cancel := make(chan struct{})
	var entryOut2 gofuse.EntryOut
	st := fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "small.txt", &entryOut2)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}

	// Open the file
	var openOut gofuse.OpenOut
	st = fs.Open(cancel, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entryOut2.NodeId}, Flags: syscall.O_RDONLY}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	// Read — first GET fails 503, retry succeeds
	result, st := fs.Read(cancel, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0, Size: uint32(len(fileData))}, nil)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK (must not return EAGAIN)", st)
	}
	if result == nil {
		t.Fatal("Read returned nil result")
	}
	got := getCalls.Load()
	if got < 2 {
		t.Fatalf("GET calls = %d, want >= 2 (initial + retry)", got)
	}
}

// TestReadRangeRetryExhaustedReturnsEIO verifies that when all read retries
// are exhausted, the Read returns EIO instead of EAGAIN.
func TestReadRangeRetryExhaustedReturnsEIO(t *testing.T) {
	var getCalls atomic.Int32
	fileSize := int64(1 << 20) // 1 MiB — above smallFileThreshold

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			// All GETs fail with 503
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	cancel := make(chan struct{})
	var entryOut2 gofuse.EntryOut
	st := fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "large.bin", &entryOut2)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}

	var openOut gofuse.OpenOut
	st = fs.Open(cancel, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entryOut2.NodeId}, Flags: syscall.O_RDONLY}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	// Read — all attempts fail 503
	_, st = fs.Read(cancel, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0, Size: 4096}, nil)
	if st != gofuse.EIO {
		t.Fatalf("Read status = %v, want EIO (must not return EAGAIN)", st)
	}

	// Verify retries happened: 1 initial + readTransientRetryCount retries
	wantCalls := int32(1 + readTransientRetryCount)
	if got := getCalls.Load(); got != wantCalls {
		t.Fatalf("GET calls = %d, want %d", got, wantCalls)
	}
}

// TestReadSmallFileRetryExhaustedReturnsEIO verifies that small file read
// retries that are all exhausted return EIO, not EAGAIN.
func TestReadSmallFileRetryExhaustedReturnsEIO(t *testing.T) {
	var getCalls atomic.Int32
	fileSize := 100 // small file

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(fileSize))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	cancel := make(chan struct{})
	var entryOut2 gofuse.EntryOut
	st := fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "tiny.txt", &entryOut2)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}

	var openOut gofuse.OpenOut
	st = fs.Open(cancel, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entryOut2.NodeId}, Flags: syscall.O_RDONLY}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	_, st = fs.Read(cancel, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0, Size: uint32(fileSize)}, nil)
	if st != gofuse.EIO {
		t.Fatalf("Read status = %v, want EIO", st)
	}

	wantCalls := int32(1 + readTransientRetryCount)
	if got := getCalls.Load(); got != wantCalls {
		t.Fatalf("GET calls = %d, want %d", got, wantCalls)
	}
}

// TestIsTransientReadErr verifies classification of transient read errors.
func TestIsTransientReadErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context.Canceled", context.Canceled, true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"HTTP 503", &client.StatusError{StatusCode: http.StatusServiceUnavailable, Message: "unavailable"}, true},
		{"HTTP 504", &client.StatusError{StatusCode: http.StatusGatewayTimeout, Message: "timeout"}, true},
		{"HTTP 500", &client.StatusError{StatusCode: http.StatusInternalServerError, Message: "error"}, true},
		{"HTTP 404", &client.StatusError{StatusCode: http.StatusNotFound, Message: "not found"}, false},
		{"HTTP 403", &client.StatusError{StatusCode: http.StatusForbidden, Message: "forbidden"}, false},
		{"generic error", fmt.Errorf("something broke"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientReadErr(tt.err); got != tt.want {
				t.Fatalf("isTransientReadErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
