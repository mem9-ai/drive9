package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

var nativeEndian = func() binary.ByteOrder {
	// Detect host endianness so the dirent wire-format parser in tests
	// matches the platform go-fuse serialized the buffer with.
	var probe uint16 = 1
	b := (*[2]byte)(unsafe.Pointer(&probe))
	if b[0] == 1 {
		return binary.LittleEndian
	}
	return binary.BigEndian
}()

type testErrorRecorder struct {
	mu  sync.Mutex
	err error
}

func (r *testErrorRecorder) Recordf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *testErrorRecorder) Check(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		t.Fatal(r.err)
	}
}

func trustedProcessLocalEventsOptions() *MountOptions {
	opts := &MountOptions{TrustLocalEvents: true}
	opts.setDefaults()
	return opts
}

func newTestDat9FS(tb testing.TB, size int64, get func(http.ResponseWriter, *http.Request)) (*Dat9FS, uint64, func()) {
	tb.Helper()

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
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())
	return fs, ino, ts.Close
}

func newTestDat9FSWithRangeObject(tb testing.TB, size int64, object func(http.ResponseWriter, *http.Request)) (*Dat9FS, uint64, func()) {
	tb.Helper()

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			object(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	opts := trustedProcessLocalEventsOptions()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())
	return fs, ino, ts.Close
}

func newTestDat9FSWithStatAndRangeObject(tb testing.TB, size int64, stat func(http.ResponseWriter, *http.Request), object func(http.ResponseWriter, *http.Request)) (*Dat9FS, uint64, func()) {
	tb.Helper()

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			stat(w, r)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			object(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/file.bin", false, size, time.Now())
	return fs, ino, ts.Close
}

func TestInitSynchronouslyWarmsInlineThreshold(t *testing.T) {
	// Pre-fix this was a goroutine, so the very first FUSE Create/Flush
	// after mount could see the 50KB fallback even when the server
	// advertised something larger. Init must block on the warm fetch.
	const advertised = int64(262144)
	statusHits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			statusHits++
			_, _ = w.Write([]byte(`{"status":"active","inline_threshold":262144}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	// Use the raw client (no test pin) so Init's warm actually hits the
	// fake /v1/status — newTestClient pins a static threshold and would
	// short-circuit the fetch.
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.Init(nil)

	if statusHits != 1 {
		t.Fatalf("Init issued %d status fetches, want 1", statusHits)
	}
	if got := fs.inlineThreshold(); got != advertised {
		t.Fatalf("inlineThreshold after Init = %d, want %d", got, advertised)
	}
}

func TestInitWarmTimeoutFallsBackToDefault(t *testing.T) {
	// Server is unreachable / hung; Init must not block longer than the
	// warm timeout and must fall back to the local default so mount stays
	// usable. The handler waits on the request context so the server can
	// shut down cleanly when the client cancels.
	prev := inlineThresholdWarmTimeout
	inlineThresholdWarmTimeout = 100 * time.Millisecond
	t.Cleanup(func() { inlineThresholdWarmTimeout = prev })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	// Raw client so Init actually issues the warm fetch and we observe the
	// timeout-fallback path; newTestClient would short-circuit via the
	// pinned threshold.
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	start := time.Now()
	fs.Init(nil)
	elapsed := time.Since(start)
	// Allow a generous slack for slow CI: ~5x the configured timeout.
	if elapsed > 5*inlineThresholdWarmTimeout {
		t.Fatalf("Init took %v, want <= %v", elapsed, 5*inlineThresholdWarmTimeout)
	}
	if got := fs.inlineThreshold(); got != defaultSmallFileThreshold {
		t.Fatalf("inlineThreshold after timeout = %d, want %d", got, defaultSmallFileThreshold)
	}
	// negotiatedInlineThreshold returns 0 since /v1/status never succeeded;
	// hot-path callers must use this to force multipart for non-empty
	// uploads instead of the heuristic-only inlineThreshold().
	if got := fs.negotiatedInlineThreshold(); got != 0 {
		t.Fatalf("negotiatedInlineThreshold after timeout = %d, want 0", got)
	}
}

func TestNegotiatedInlineThresholdSeparatesProtocolFromHeuristic(t *testing.T) {
	// inlineThreshold() is the heuristic value (falls back to 50KB) used
	// for things like read prefetch sizing where 50KB is harmless.
	// negotiatedInlineThreshold() returns 0 until /v1/status succeeds —
	// flushHandle and commit_queue use the latter so a missing server
	// value forces multipart instead of risking a server-side reject when
	// the operator configured the inline threshold below 50KB.
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://127.0.0.1:1", ""), opts) // unreachable server

	if got := fs.negotiatedInlineThreshold(); got != 0 {
		t.Fatalf("pre-warm negotiatedInlineThreshold = %d, want 0", got)
	}
	if got := fs.inlineThreshold(); got != defaultSmallFileThreshold {
		t.Fatalf("pre-warm inlineThreshold = %d, want %d (heuristic fallback)", got, defaultSmallFileThreshold)
	}
}

func TestUnknownInlineThresholdNonEmptyHandleUploadUsesMultipart(t *testing.T) {
	const filePath = "/unknown-threshold.bin"
	data := []byte("non-empty write before inline threshold warmup")

	for _, tc := range []struct {
		name string
		run  func(*Dat9FS, *FileHandle) gofuse.Status
	}{
		{
			name: "flush",
			run: func(fs *Dat9FS, fh *FileHandle) gofuse.Status {
				return fs.flushHandle(context.Background(), fh)
			},
		},
		{
			name: "write-sync",
			run: func(fs *Dat9FS, fh *FileHandle) gofuse.Status {
				return fs.syncWriteHandleToRemoteLocked(context.Background(), fh)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedRevision := int64(0)
			rec := newMultipartUploadRecorder(t, filePath, int64(len(data)), &expectedRevision)

			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(client.New(rec.server.URL, ""), opts)
			if got := fs.negotiatedInlineThreshold(); got != 0 {
				t.Fatalf("negotiatedInlineThreshold = %d, want 0", got)
			}

			ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
			fh := &FileHandle{
				Ino:      ino,
				Path:     filePath,
				Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
				IsNew:    true,
				OrigSize: 0,
			}
			if _, err := fh.Dirty.Write(0, data); err != nil {
				t.Fatal(err)
			}
			fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

			fh.Lock()
			st := tc.run(fs, fh)
			fh.Unlock()
			if st != gofuse.OK {
				t.Fatalf("upload status = %v, want OK", st)
			}
			if got := rec.directFilePuts.Load(); got != 0 {
				t.Fatalf("direct PUT calls = %d, want 0", got)
			}
			if got := rec.initiateCalls.Load(); got != 1 {
				t.Fatalf("multipart initiate calls = %d, want 1", got)
			}
			if got := rec.completeCalls.Load(); got != 1 {
				t.Fatalf("multipart complete calls = %d, want 1", got)
			}
			rec.mu.Lock()
			uploadedBytes := rec.gotUploadedBytes
			rec.mu.Unlock()
			if uploadedBytes != int64(len(data)) {
				t.Fatalf("uploaded bytes = %d, want %d", uploadedBytes, len(data))
			}
		})
	}
}

func TestFlushHandlePatchAfterLazyTruncatePreservesRemotePrefix(t *testing.T) {
	const (
		filePath = "/patch-truncate.bin"
		origSize = 300_000
		newSize  = 202_324
		baseRev  = 7
	)

	remote := make([]byte, origSize)
	for i := range remote {
		remote[i] = byte((i*31)%251 + 1)
	}

	var (
		mu          sync.Mutex
		patchCalls  atomic.Int32
		uploadCalls atomic.Int32
		uploaded    []byte
	)

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			_, _ = w.Write(remote)
			return
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs"+filePath:
			patchCalls.Add(1)
			var req struct {
				NewSize          int64 `json:"new_size"`
				DirtyParts       []int `json:"dirty_parts"`
				PartSize         int64 `json:"part_size"`
				ExpectedRevision int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode patch request: %v", err)
			}
			if req.NewSize != newSize {
				t.Fatalf("patch new_size = %d, want %d", req.NewSize, newSize)
			}
			if !reflect.DeepEqual(req.DirtyParts, []int{1}) {
				t.Fatalf("patch dirty_parts = %v, want [1]", req.DirtyParts)
			}
			if req.PartSize != DefaultPartSize {
				t.Fatalf("patch part_size = %d, want %d", req.PartSize, DefaultPartSize)
			}
			if req.ExpectedRevision != baseRev {
				t.Fatalf("patch expected_revision = %d, want %d", req.ExpectedRevision, baseRev)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id": "patch-upload-1",
				"part_size": int64(DefaultPartSize),
				"upload_parts": []map[string]any{
					{
						"number": 1,
						"url":    ts.URL + "/s3/patch-upload-1/1",
						"size":   int64(newSize),
					},
				},
				"copied_parts": []int{},
			})
			return
		case r.Method == http.MethodPut && r.URL.Path == "/s3/patch-upload-1/1":
			uploadCalls.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read patch upload: %v", err)
			}
			mu.Lock()
			uploaded = append(uploaded[:0], body...)
			mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-upload-1/complete":
			w.WriteHeader(http.StatusOK)
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, origSize, time.Now())
	fs.inodes.UpdateRevision(ino, baseRev)
	fh := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: origSize,
		BaseRev:  baseRev,
	}
	fh.Lock()
	fs.rebindCleanWriteBufferToRemoteLocked(fh, origSize)
	fh.Unlock()

	fh.Lock()
	abortStreamer, err := fs.truncateWritableHandleLocked(fh, newSize)
	if err != nil {
		fh.Unlock()
		t.Fatalf("truncateWritableHandleLocked: %v", err)
	}
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if abortStreamer != nil {
		abortStreamer()
	}
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if got := patchCalls.Load(); got != 1 {
		t.Fatalf("patch calls = %d, want 1", got)
	}
	if got := uploadCalls.Load(); got != 1 {
		t.Fatalf("patch upload calls = %d, want 1", got)
	}
	mu.Lock()
	got := append([]byte(nil), uploaded...)
	mu.Unlock()
	if !bytes.Equal(got, remote[:newSize]) {
		gotHash := sha256.Sum256(got)
		wantHash := sha256.Sum256(remote[:newSize])
		t.Fatalf("patch upload did not preserve prefix: got_sha256=%x want_sha256=%x", gotHash, wantHash)
	}
}

func BenchmarkDat9FS_SmallFileOpen(b *testing.B) {
	const (
		filePath = "/file.bin"
		fileRev  = 7
	)
	data := []byte("fresh-data")

	b.Run("read-cache-hit", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("stale-data"))
		})
		defer cleanup()

		fs.inodes.UpdateRevision(ino, fileRev)
		fs.readCache.Put(filePath, data, fileRev)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out gofuse.OpenOut
			st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out)
			if st != gofuse.OK {
				b.Fatalf("Open status = %v, want OK", st)
			}
			if fh, ok := fs.fileHandles.Get(out.Fh); ok {
				benchmarkIntSink += int(fh.BaseRev)
				fs.clearDirtySize(ino, fh.DirtySeq)
			}
			fs.fileHandles.Delete(out.Fh)
		}
	})

	b.Run("pending-shadow", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("stale-data"))
		})
		defer cleanup()

		shadow, err := NewShadowStore(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		defer shadow.Close()
		pending, err := NewPendingIndex(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		fs.shadowStore = shadow
		fs.pendingIndex = pending

		if err := shadow.WriteFull(filePath, data, 9); err != nil {
			b.Fatal(err)
		}
		if _, err := pending.PutWithBaseRev(filePath, int64(len(data)), PendingOverwrite, 9); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out gofuse.OpenOut
			st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out)
			if st != gofuse.OK {
				b.Fatalf("Open status = %v, want OK", st)
			}
			if fh, ok := fs.fileHandles.Get(out.Fh); ok {
				benchmarkIntSink += int(fh.BaseRev)
				fs.clearDirtySize(ino, fh.DirtySeq)
			}
			fs.fileHandles.Delete(out.Fh)
		}
	})

	b.Run("pending-writeback-overwrite", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("stale-data"))
		})
		defer cleanup()

		cache, err := NewWriteBackCache(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		fs.SetWriteBack(cache, nil)
		if err := cache.PutWithBaseRev(filePath, data, int64(len(data)), PendingOverwrite, 11); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out gofuse.OpenOut
			st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out)
			if st != gofuse.OK {
				b.Fatalf("Open status = %v, want OK", st)
			}
			if fh, ok := fs.fileHandles.Get(out.Fh); ok {
				benchmarkIntSink += int(fh.BaseRev)
				fs.clearDirtySize(ino, fh.DirtySeq)
			}
			fs.fileHandles.Delete(out.Fh)
		}
	})
}

func BenchmarkDat9FS_GetAttr(b *testing.B) {
	const filePath = "/cached.txt"

	b.Run("remote-head", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, 12, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("data"))
		})
		defer cleanup()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out gofuse.AttrOut
			st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
			if st != gofuse.OK {
				b.Fatalf("GetAttr status = %v, want OK", st)
			}
			benchmarkIntSink += int(out.Size)
		}
	})

	b.Run("dir-cache-hit", func(b *testing.B) {
		var remoteCalls atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			remoteCalls.Add(1)
			http.Error(w, "unexpected remote call", http.StatusInternalServerError)
		}))
		defer ts.Close()

		fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
		ino := fs.inodes.Lookup(filePath, false, 1, time.Unix(1, 0))
		fs.dirCache.Upsert("/", CachedFileInfo{
			Name:     "cached.txt",
			Size:     12,
			IsDir:    false,
			Mtime:    time.Unix(123, 0),
			Revision: 7,
			Mode:     0o600,
			HasMode:  true,
		})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out gofuse.AttrOut
			st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
			if st != gofuse.OK {
				b.Fatalf("GetAttr status = %v, want OK", st)
			}
			benchmarkIntSink += int(out.Size)
		}
		b.StopTimer()
		if got := remoteCalls.Load(); got != 0 {
			b.Fatalf("remote calls = %d, want 0", got)
		}
	})
}

func BenchmarkDat9FS_SmallFileRead(b *testing.B) {
	const filePath = "/file.bin"
	data := make([]byte, 32*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	const readSize = 4096

	b.Run("read-cache-hit", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(data)
		})
		defer cleanup()

		fs.inodes.UpdateRevision(ino, 3)
		fs.readCache.Put(filePath, data, 3)

		var out gofuse.OpenOut
		st := fs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Flags:    uint32(syscall.O_RDONLY),
		}, &out)
		if st != gofuse.OK {
			b.Fatalf("Open status = %v, want OK", st)
		}
		defer fs.fileHandles.Delete(out.Fh)

		buf := make([]byte, readSize)
		b.ReportAllocs()
		b.SetBytes(readSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := uint64((i % 8) * readSize)
			result, st := fs.Read(nil, &gofuse.ReadIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
				Offset:   offset,
				Size:     uint32(readSize),
			}, buf)
			if st != gofuse.OK {
				b.Fatalf("Read status = %v, want OK", st)
			}
			data, _ := result.Bytes(buf)
			benchmarkByteSink ^= data[0]
		}
	})

	b.Run("writable-clean-buffer", func(b *testing.B) {
		fs, ino, cleanup := newTestDat9FS(b, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("stale-data"))
		})
		defer cleanup()

		fs.inodes.UpdateRevision(ino, 5)
		fs.readCache.Put(filePath, data, 5)

		var out gofuse.OpenOut
		st := fs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Flags:    uint32(syscall.O_RDWR),
		}, &out)
		if st != gofuse.OK {
			b.Fatalf("Open status = %v, want OK", st)
		}
		defer func() {
			if fh, ok := fs.fileHandles.Get(out.Fh); ok {
				fs.clearDirtySize(ino, fh.DirtySeq)
			}
			fs.fileHandles.Delete(out.Fh)
		}()

		buf := make([]byte, readSize)
		b.ReportAllocs()
		b.SetBytes(readSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offset := uint64((i % 8) * readSize)
			result, st := fs.Read(nil, &gofuse.ReadIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
				Offset:   offset,
				Size:     uint32(readSize),
			}, buf)
			if st != gofuse.OK {
				b.Fatalf("Read status = %v, want OK", st)
			}
			data, _ := result.Bytes(buf)
			benchmarkByteSink ^= data[0]
		}
	})
}

func BenchmarkDat9FS_SmallFileFlush(b *testing.B) {
	const filePath = "/flush.txt"
	data := make([]byte, 16*1024)
	for i := range data {
		data[i] = byte(i % 253)
	}

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://localhost", ""), opts)
	cache, err := NewWriteBackCache(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	fs.SetWriteBack(cache, nil)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Time{})

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out gofuse.OpenOut
		st := fs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
		}, &out)
		if st != gofuse.OK {
			b.Fatalf("Open status = %v, want OK", st)
		}

		if _, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       out.Fh,
			Offset:   0,
		}, data); st != gofuse.OK {
			b.Fatalf("Write status = %v, want OK", st)
		}

		st = fs.Flush(nil, &gofuse.FlushIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       out.Fh,
		})
		if st != gofuse.OK {
			b.Fatalf("Flush status = %v, want OK", st)
		}

		if fh, ok := fs.fileHandles.Get(out.Fh); ok {
			benchmarkIntSink += int(fh.Dirty.Size())
			fs.clearDirtySize(ino, fh.DirtySeq)
		}
		fs.fileHandles.Delete(out.Fh)
		cache.Remove(filePath)
		fs.readCache.Invalidate(filePath)
	}
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
	fs, ino, cleanup := newTestDat9FS(t, defaultSmallFileThreshold+1, func(w http.ResponseWriter, r *http.Request) {
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

func TestOpenWritableSmallFileUsesReadCacheFastPath(t *testing.T) {
	var headCalls atomic.Int32
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			_, _ = w.Write([]byte("stale-data"))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	const cachedRev = 7
	cached := []byte("fresh-data")
	ino := fs.inodes.Lookup("/file.txt", false, int64(len(cached)), time.Now())
	fs.inodes.UpdateRevision(ino, cachedRev)
	fs.readCache.Put("/file.txt", cached, cachedRev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	buf := make([]byte, 32)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}

	data, _ := result.Bytes(buf)
	if string(data) != string(cached) {
		t.Fatalf("Read data = %q, want %q", string(data), string(cached))
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
	}
}

func TestReadZeroLengthFromWritableCleanBuffer(t *testing.T) {
	var headCalls atomic.Int32
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			_, _ = w.Write([]byte("stale-data"))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	const cachedRev = 7
	cached := []byte("fresh-data")
	ino := fs.inodes.Lookup("/file.txt", false, int64(len(cached)), time.Now())
	fs.inodes.UpdateRevision(ino, cachedRev)
	fs.readCache.Put("/file.txt", cached, cachedRev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	buf := make([]byte, 8)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   4,
		Size:     0,
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	data, _ := result.Bytes(buf)
	if len(data) != 0 {
		t.Fatalf("Read len = %d, want 0", len(data))
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
	}
}

func TestReadCacheCachesMediumSmallFileAfterFirstRead(t *testing.T) {
	data := bytes.Repeat([]byte("x"), defaultSmallFileThreshold+1024)
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	const rev = 11
	ino := fs.inodes.Lookup("/medium.bin", false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, rev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("first Read status = %v, want OK", st)
	}
	first, _ := result.Bytes(buf)
	if string(first) != string(data[:len(buf)]) {
		t.Fatalf("first Read = %q, want prefix", string(first))
	}

	result, st = fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   128,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("second Read status = %v, want OK", st)
	}
	second, _ := result.Bytes(buf)
	if string(second) != string(data[128:128+len(buf)]) {
		t.Fatalf("second Read = %q, want prefix", string(second))
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("GET calls = %d, want 1", got)
	}
}

// A 2MiB file sits above the old 1MiB whole-file threshold and below the
// current default: its cold read must be one whole-file GET (no Range
// header, no prefetcher) instead of block-split range reads.
func TestReadTwoMiBFileUsesSingleWholeFileRequest(t *testing.T) {
	data := bytes.Repeat([]byte("y"), 2<<20)
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls.Add(1)
			if got := r.Header.Get("Range"); got != "" {
				http.Error(w, "unexpected range request: "+got, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)

	const rev = 11
	ino := fs.inodes.Lookup("/bench.bin", false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, rev)

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
	if fh.Prefetch != nil {
		t.Fatal("expected no Prefetcher for a file within the whole-file read threshold")
	}

	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[:128<<10]) {
		t.Fatalf("first read status = %v, len = %d; want OK with 128KiB prefix", st, len(got))
	}
	got, st, err = readDat9FSTestRange(fs, ino, out.Fh, int64(len(data))-(128<<10), 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[len(data)-(128<<10):]) {
		t.Fatalf("second read status = %v, len = %d; want OK with 128KiB suffix", st, len(got))
	}
	if calls := getCalls.Load(); calls != 1 {
		t.Fatalf("GET calls = %d, want 1 whole-file request", calls)
	}
}

// Whole-file reads must persist into the disk read cache so warm reads
// after ReadCache TTL expiry or eviction are served from local disk
// instead of re-fetching from the remote.
func TestReadWholeFilePopulatesDiskReadCacheTier(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(7)
	)
	data := bytes.Repeat([]byte("z"), 2<<20)
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "" {
			http.Error(w, "unexpected range request: "+got, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 8<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	key, ok := fs.diskReadCacheKey(path, mustGetInodeEntry(t, fs, ino), 0, int64(len(data)))
	if !ok {
		t.Fatal("disk read cache key unavailable")
	}

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[:64<<10]) {
		t.Fatalf("first read status = %v, len = %d; want OK", st, len(got))
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, key)

	// Simulate ReadCache TTL expiry/eviction: the next read must be served
	// from the disk tier without another remote fetch.
	fs.readCache.InvalidateAll()

	got, st, err = readDat9FSTestRange(fs, ino, fh, int64(len(data))-(64<<10), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[len(data)-(64<<10):]) {
		t.Fatalf("disk-tier read status = %v, len = %d; want OK", st, len(got))
	}
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1 (second read must come from disk tier)", calls)
	}
}

func readDat9FSTestBytes(fs *Dat9FS, ino, fh uint64, size int) ([]byte, gofuse.Status, error) {
	buf := make([]byte, size)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fh,
		Offset:   0,
		Size:     uint32(size),
	}, buf)
	if st != gofuse.OK {
		return nil, st, nil
	}
	if result == nil {
		return nil, st, errors.New("nil read result")
	}
	data, _ := result.Bytes(buf)
	return append([]byte(nil), data...), st, nil
}

func readDat9FSTestRange(fs *Dat9FS, ino, fh uint64, offset int64, size int) ([]byte, gofuse.Status, error) {
	buf := make([]byte, size)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fh,
		Offset:   uint64(offset),
		Size:     uint32(size),
	}, buf)
	if st != gofuse.OK {
		return nil, st, nil
	}
	if result == nil {
		return nil, st, errors.New("nil read result")
	}
	data, _ := result.Bytes(buf)
	return append([]byte(nil), data...), st, nil
}

func openDat9FSTestHandle(t *testing.T, fs *Dat9FS, ino uint64, path string) uint64 {
	t.Helper()
	fh := &FileHandle{Ino: ino, Path: path}
	return fs.allocateFileHandle(fh)
}

func waitForDiskReadCacheEntry(t *testing.T, cache *DiskReadCache, key DiskReadCacheKey) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if cacheHasPersistedDiskReadCacheEntry(cache, key) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for disk read cache entry")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cacheHasPersistedDiskReadCacheEntry(cache *DiskReadCache, key DiskReadCacheKey) bool {
	if cache == nil {
		return false
	}
	digest := key.digest()
	cache.mu.Lock()
	_, pending := cache.pending[digest]
	cache.mu.Unlock()
	if pending {
		return false
	}
	_, err := readDiskReadCacheFile(cache.pathForDigest(key.digest()), key)
	return err == nil
}

func parseTestBytesRange(header string) (int64, int64, bool) {
	var start, end int64
	n, err := fmt.Sscanf(header, "bytes=%d-%d", &start, &end)
	return start, end, n == 2 && err == nil && start >= 0 && end >= start
}

func observeMaxInt32(max *atomic.Int32, value int32) {
	for {
		old := max.Load()
		if value <= old || max.CompareAndSwap(old, value) {
			return
		}
	}
}

func TestDat9FSDiskReadCacheRangeReadThrough(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(7)
	)
	data := bytes.Repeat([]byte("x"), defaultReadCacheMaxFileSize+1024)
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=128-191" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[128:192])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	key, ok := fs.diskReadCacheKey(path, mustGetInodeEntry(t, fs, ino), 128, 64)
	if !ok {
		t.Fatal("disk read cache key unavailable")
	}

	got, st, err := readDat9FSTestRange(fs, ino, fh, 128, 64)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[128:192]) {
		t.Fatalf("first read = %q, %v; want cached range OK", got, st)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, key)

	got, st, err = readDat9FSTestRange(fs, ino, fh, 128, 64)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[128:192]) {
		t.Fatalf("second read = %q, %v; want cached range OK", got, st)
	}
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1", calls)
	}
}

func TestDat9FSDiskReadCacheSingleRangeFinalEOFRead(t *testing.T) {
	const (
		path        = "/file.bin"
		rev         = int64(7)
		requestSize = 64
	)
	data := bytes.Repeat([]byte("x"), defaultReadCacheMaxFileSize+96)
	for i := 0; i < 16; i++ {
		data[len(data)-16+i] = byte('a' + i)
	}
	offset := int64(len(data) - 16)
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		wantRange := fmt.Sprintf("bytes=%d-%d", offset, len(data)-1)
		if got := r.Header.Get("Range"); got != wantRange {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[offset:])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	entry := mustGetInodeEntry(t, fs, ino)
	key, ok := fs.diskReadCacheKey(path, entry, offset, 16)
	if !ok {
		t.Fatal("disk read cache final key unavailable")
	}

	got, st, err := readDat9FSTestRange(fs, ino, fh, offset, requestSize)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[offset:]) {
		t.Fatalf("final EOF read = %q, %v; want trimmed final bytes OK", got, st)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, key)

	got, st, err = readDat9FSTestRange(fs, ino, fh, offset, requestSize)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data[offset:]) {
		t.Fatalf("cached final EOF read = %q, %v; want trimmed final bytes OK", got, st)
	}
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1 after cached final read", calls)
	}
}

func TestDat9FSParallelDiskReadFetchesBlocksInParallelAndCaches(t *testing.T) {
	const (
		path        = "/file.bin"
		rev         = int64(7)
		blockSize   = int64(64)
		concurrency = 2
	)
	data := make([]byte, 4*blockSize)
	for i := range data {
		data[i] = byte(i)
	}
	var objectReads atomic.Int32
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var rangesMu sync.Mutex
	ranges := make([]string, 0, 4)
	recorder := &testErrorRecorder{}

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		rangeHeader := r.Header.Get("Range")
		rangesMu.Lock()
		ranges = append(ranges, rangeHeader)
		rangesMu.Unlock()
		cur := inFlight.Add(1)
		observeMaxInt32(&maxInFlight, cur)
		defer inFlight.Add(-1)
		time.Sleep(50 * time.Millisecond)

		start, end, ok := parseTestBytesRange(rangeHeader)
		if !ok || end >= int64(len(data)) {
			recorder.Recordf("wrong range: %s", rangeHeader)
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.readCache = NewReadCacheWithMaxFileSize(1<<20, 0, 1)
	fs.opts.ParallelReadBlockSize = blockSize
	fs.opts.ParallelReadConcurrency = concurrency
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	entry := mustGetInodeEntry(t, fs, ino)

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data) {
		t.Fatalf("parallel read = len %d, %v; want full data OK", len(got), st)
	}
	recorder.Check(t)
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("max concurrent object reads = %d, want >= 2", got)
	}
	if got := maxInFlight.Load(); got > concurrency {
		t.Fatalf("max concurrent object reads = %d, want <= %d", got, concurrency)
	}
	for offset := int64(0); offset < int64(len(data)); offset += blockSize {
		key, ok := fs.diskReadCacheKey(path, entry, offset, blockSize)
		if !ok {
			t.Fatalf("disk cache block key unavailable at offset %d", offset)
		}
		waitForDiskReadCacheEntry(t, fs.diskReadCache, key)
	}

	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data) {
		t.Fatalf("cached parallel read = len %d, %v; want full data OK", len(got), st)
	}
	if calls := objectReads.Load(); calls != 4 {
		t.Fatalf("object reads = %d, want 4 after cached second read", calls)
	}

	rangesMu.Lock()
	seen := append([]string(nil), ranges...)
	rangesMu.Unlock()
	sort.Strings(seen)
	want := []string{"bytes=0-63", "bytes=128-191", "bytes=192-255", "bytes=64-127"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("ranges = %v, want %v", seen, want)
	}
}

func TestDat9FSDiskReadCacheRejectsShortBackendResponse(t *testing.T) {
	const (
		path      = "/file.bin"
		rev       = int64(7)
		blockSize = int64(64)
	)
	data := bytes.Repeat([]byte("a"), int(blockSize))

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, blockSize, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=0-63" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:blockSize/2])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.readCache = NewReadCacheWithMaxFileSize(1<<20, 0, 1)
	fs.opts.ParallelReadBlockSize = blockSize
	fs.opts.ParallelReadConcurrency = 2
	fs.inodes.UpdateRevision(ino, rev)
	fhID := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fhID)
	fh, ok := fs.fileHandles.Get(fhID)
	if !ok {
		t.Fatal("file handle missing")
	}
	entry := mustGetInodeEntry(t, fs, ino)
	key, ok := fs.diskReadCacheKey(path, entry, 0, blockSize)
	if !ok {
		t.Fatal("disk cache block key unavailable")
	}

	got, _, err := fs.readDiskCachedRange(context.Background(), path, fh, key)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readDiskCachedRange error = %v, want unexpected EOF", err)
	}
	if len(got) != 0 {
		t.Fatalf("readDiskCachedRange data len = %d, want 0 on short response", len(got))
	}
	if cached, ok := fs.diskReadCache.Get(key); ok {
		t.Fatalf("disk read cache hit after short backend response = len %d, want miss", len(cached))
	}
}

func TestDat9FSParallelDiskReadHandlesPartialFinalBlock(t *testing.T) {
	const (
		path      = "/file.bin"
		rev       = int64(7)
		blockSize = int64(64)
	)
	data := make([]byte, 150)
	for i := range data {
		data[i] = byte(i % 251)
	}
	var rangesMu sync.Mutex
	ranges := make([]string, 0, 3)
	recorder := &testErrorRecorder{}

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		rangesMu.Lock()
		ranges = append(ranges, rangeHeader)
		rangesMu.Unlock()
		start, end, ok := parseTestBytesRange(rangeHeader)
		if !ok || end >= int64(len(data)) {
			recorder.Recordf("wrong range: %s", rangeHeader)
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.readCache = NewReadCacheWithMaxFileSize(1<<20, 0, 1)
	fs.opts.ParallelReadBlockSize = blockSize
	fs.opts.ParallelReadConcurrency = 3
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || !bytes.Equal(got, data) {
		t.Fatalf("partial final block read = len %d, %v; want EOF-trimmed data OK", len(got), st)
	}
	recorder.Check(t)
	rangesMu.Lock()
	seen := append([]string(nil), ranges...)
	rangesMu.Unlock()
	sort.Strings(seen)
	want := []string{"bytes=0-63", "bytes=128-149", "bytes=64-127"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("ranges = %v, want %v", seen, want)
	}
}

func TestDat9FSParallelDiskReadPropagatesBlockError(t *testing.T) {
	const (
		path      = "/file.bin"
		rev       = int64(7)
		blockSize = int64(64)
	)
	data := make([]byte, 3*blockSize)
	for i := range data {
		data[i] = byte(i)
	}

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "bytes=64-127" {
			http.Error(w, "blocked range", http.StatusBadGateway)
			return
		}
		start, end, ok := parseTestBytesRange(rangeHeader)
		if !ok || end >= int64(len(data)) {
			http.Error(w, "wrong range", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.readCache = NewReadCacheWithMaxFileSize(1<<20, 0, 1)
	fs.opts.ParallelReadBlockSize = blockSize
	fs.opts.ParallelReadConcurrency = 3
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	_, st, err := readDat9FSTestRange(fs, ino, fh, 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if st == gofuse.OK {
		t.Fatal("parallel read status = OK, want block error propagated")
	}
}

func TestDat9FSParallelDiskReadStopsQueuedBlocksAfterFirstError(t *testing.T) {
	const (
		path      = "/file.bin"
		rev       = int64(7)
		blockSize = int64(64)
	)
	data := make([]byte, 4*blockSize)
	for i := range data {
		data[i] = byte(i)
	}
	slowStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowOnce sync.Once
	var releaseFirstOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseFirstOnce.Do(func() { close(releaseFirst) })
		releaseOnce.Do(func() { close(releaseSlow) })
	})
	var unexpectedQueuedReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		switch rangeHeader {
		case "bytes=0-63":
			select {
			case <-slowStarted:
			case <-r.Context().Done():
				return
			}
			<-releaseFirst
			http.Error(w, "blocked range", http.StatusBadGateway)
			return
		case "bytes=64-127":
			slowOnce.Do(func() { close(slowStarted) })
			<-releaseSlow
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[64:128])
			return
		default:
			unexpectedQueuedReads.Add(1)
			w.WriteHeader(http.StatusPartialContent)
			start, end, ok := parseTestBytesRange(rangeHeader)
			if ok && end < int64(len(data)) {
				_, _ = w.Write(data[start : end+1])
			}
		}
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.readCache = NewReadCacheWithMaxFileSize(1<<20, 0, 1)
	fs.opts.ParallelReadBlockSize = blockSize
	fs.opts.ParallelReadConcurrency = 2
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	done := make(chan gofuse.Status, 1)
	go func() {
		_, st, err := readDat9FSTestRange(fs, ino, fh, 0, len(data))
		if err != nil {
			t.Errorf("Read: %v", err)
		}
		done <- st
	}()
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("second in-flight block did not start")
	}
	releaseFirstOnce.Do(func() { close(releaseFirst) })
	time.Sleep(50 * time.Millisecond)
	releaseOnce.Do(func() { close(releaseSlow) })
	select {
	case st := <-done:
		if st == gofuse.OK {
			t.Fatal("parallel read status = OK, want first block error propagated")
		}
	case <-time.After(time.Second):
		t.Fatal("parallel read did not finish after releasing in-flight block")
	}
	if got := unexpectedQueuedReads.Load(); got != 0 {
		t.Fatalf("queued block reads after first error = %d, want 0", got)
	}
}

func TestDat9FSSQLitePersistentJournalBypassesDiskReadCacheKey(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)

	regular := &InodeEntry{Path: "/repo/regular.bin", Size: 4096, Revision: 7}
	if _, ok := fs.diskReadCacheKey(regular.Path, regular, 0, 1024); !ok {
		t.Fatal("regular file disk read cache key unavailable")
	}

	for _, path := range []string{"/repo/workload.db-wal", "/repo/workload.db-journal"} {
		entry := &InodeEntry{Path: path, Size: 4096, Revision: 7}
		if key, ok := fs.diskReadCacheKey(path, entry, 0, 1024); ok {
			t.Fatalf("disk read cache key for %s = %+v, want disabled", path, key)
		}
	}
}

func TestDat9FSSQLiteMainDatabaseDiskReadCacheDependsOnOpenSidecar(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)

	const dbPath = "/repo/workload.db"
	entry := &InodeEntry{Path: dbPath, Size: 4096, Revision: 7}
	if _, ok := fs.diskReadCacheKey(dbPath, entry, 0, 1024); !ok {
		t.Fatal("sqlite main database disk read cache key unavailable without sidecar")
	}

	walHandle := &FileHandle{Ino: 11, Path: dbPath + "-wal"}
	walHandleID := fs.allocateFileHandle(walHandle)
	defer fs.deleteFileHandle(walHandleID, walHandle)

	if key, ok := fs.diskReadCacheKey(dbPath, entry, 0, 1024); ok {
		t.Fatalf("sqlite main database disk read cache key with active sidecar = %+v, want disabled", key)
	}
}

func TestDat9FSReadSQLitePersistentJournalBypassesSmallReadCache(t *testing.T) {
	const path = "/repo/workload.db-wal"
	var reads atomic.Int32
	var recorder testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/repo/workload.db-wal" {
			recorder.Recordf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		switch reads.Add(1) {
		case 1:
			_, _ = w.Write([]byte("old-wal"))
		case 2:
			_, _ = w.Write([]byte("new-wal"))
		default:
			recorder.Recordf("unexpected read count")
			http.Error(w, "unexpected read count", http.StatusTooManyRequests)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, 7, time.Now())
	fs.inodes.UpdateRevision(ino, 3)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "old-wal" {
		t.Fatalf("first read = %q, %v; want old-wal OK", got, st)
	}

	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "new-wal" {
		t.Fatalf("second read = %q, %v; want new-wal OK", got, st)
	}
	if calls := reads.Load(); calls != 2 {
		t.Fatalf("server reads = %d, want 2", calls)
	}
	recorder.Check(t)
}

func TestDat9FSReadSQLiteMainDatabaseBypassesSmallReadCacheWithOpenSidecar(t *testing.T) {
	const path = "/repo/workload.db"
	var reads atomic.Int32
	var recorder testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/repo/workload.db" {
			recorder.Recordf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		switch reads.Add(1) {
		case 1:
			_, _ = w.Write([]byte("old-db"))
		case 2:
			_, _ = w.Write([]byte("new-db"))
		default:
			recorder.Recordf("unexpected read count")
			http.Error(w, "unexpected read count", http.StatusTooManyRequests)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 3)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	walHandle := &FileHandle{Ino: ino + 1, Path: path + "-wal"}
	walHandleID := fs.allocateFileHandle(walHandle)
	defer fs.deleteFileHandle(walHandleID, walHandle)

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "old-db" {
		t.Fatalf("first read = %q, %v; want old-db OK", got, st)
	}

	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "new-db" {
		t.Fatalf("second read = %q, %v; want new-db OK", got, st)
	}
	if calls := reads.Load(); calls != 2 {
		t.Fatalf("server reads = %d, want 2", calls)
	}
	recorder.Check(t)
}

func TestDat9FSReadSQLiteMainDatabaseUsesSmallReadCacheWithoutOpenSidecar(t *testing.T) {
	const path = "/repo/workload.db"
	var reads atomic.Int32
	var recorder testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/repo/workload.db" {
			recorder.Recordf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		switch reads.Add(1) {
		case 1:
			_, _ = w.Write([]byte("cached"))
		default:
			recorder.Recordf("unexpected read count")
			http.Error(w, "unexpected read count", http.StatusTooManyRequests)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 3)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "cached" {
		t.Fatalf("first read = %q, %v; want cached OK", got, st)
	}

	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK || string(got) != "cached" {
		t.Fatalf("second read = %q, %v; want cached OK", got, st)
	}
	if calls := reads.Load(); calls != 1 {
		t.Fatalf("server reads = %d, want 1", calls)
	}
	recorder.Check(t)
}

func TestDat9FSDiskReadCacheRevisionInvalidatesByKey(t *testing.T) {
	const path = "/file.bin"
	data := bytes.Repeat([]byte("a"), defaultReadCacheMaxFileSize+1024)
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		call := objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-2" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if call == 1 {
			_, _ = w.Write([]byte("old"))
			return
		}
		_, _ = w.Write([]byte("new"))
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	fs.inodes.UpdateRevision(ino, 1)
	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "old" {
		t.Fatalf("rev1 read = %q, %v, %v; want old OK", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: 1, Offset: 0, Length: 3})

	fs.inodes.UpdateRevision(ino, 2)
	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "new" {
		t.Fatalf("rev2 read = %q, %v, %v; want new OK", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: 2, Offset: 0, Length: 3})
	if calls := objectReads.Load(); calls != 2 {
		t.Fatalf("object reads = %d, want 2 for revision change", calls)
	}
}

func TestDat9FSDiskReadCacheCorruptionRefetches(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(7)
	)
	data := bytes.Repeat([]byte("z"), defaultReadCacheMaxFileSize+1024)
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		call := objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-2" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if call == 1 {
			_, _ = w.Write([]byte("bad"))
			return
		}
		_, _ = w.Write([]byte("ok!"))
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: rev, Offset: 0, Length: 3}

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "bad" {
		t.Fatalf("first read = %q, %v, %v; want bad OK", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, key)
	if err := os.WriteFile(fs.diskReadCache.pathForDigest(key.digest()), []byte(`{"file_id":"path:/file.bin","path":"/file.bin","revision":7,"offset":0,"length":3,"size":3,"crc32":1}`+"\nbad"), 0o644); err != nil {
		t.Fatalf("corrupt disk cache: %v", err)
	}

	got, st, err = readDat9FSTestRange(fs, ino, fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "ok!" {
		t.Fatalf("second read = %q, %v, %v; want ok! after refetch", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, key)
	if calls := objectReads.Load(); calls != 2 {
		t.Fatalf("object reads = %d, want 2 after corruption refetch", calls)
	}
}

func TestDat9FSDiskReadCacheConcurrentSameKeyDedupsBackendRead(t *testing.T) {
	const (
		path    = "/file.bin"
		rev     = int64(7)
		readers = 3
	)
	data := bytes.Repeat([]byte("q"), defaultReadCacheMaxFileSize+1024)
	started := make(chan struct{}, readers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-63" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:64])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)

	var wg sync.WaitGroup
	errs := make(chan error, readers)
	startReaders := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startReaders
			got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 64)
			if err != nil {
				errs <- err
				return
			}
			if st != gofuse.OK || !bytes.Equal(got, data[:64]) {
				errs <- fmt.Errorf("Read = %q, %v; want range OK", got, st)
			}
		}()
	}
	close(startReaders)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("backend GET did not start")
	}
	waitForWaiters(t, fs.readFlight, DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: rev, Offset: 0, Length: 64}.flightKey(), readers-1)
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: rev, Offset: 0, Length: 64})
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1", calls)
	}
}

func TestDat9FSDiskReadCacheUnverifiedRevisionMatchServesCachedAfterHead(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(7)
	)
	var headCalls atomic.Int32
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithStatAndRangeObject(t, defaultReadCacheMaxFileSize+1024, func(w http.ResponseWriter, r *http.Request) {
		headCalls.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(defaultReadCacheMaxFileSize+1024))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
		w.WriteHeader(http.StatusOK)
	}, func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		http.Error(w, "unexpected object read", http.StatusInternalServerError)
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, rev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: rev, Offset: 0, Length: 5}
	fs.diskReadCache.PutOwned(key, []byte("hello"))
	fs.markStatCacheUnverified()

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 5)
	if err != nil || st != gofuse.OK || string(got) != "hello" {
		t.Fatalf("read = %q, %v, %v; want cached hello OK", got, st, err)
	}
	if calls := headCalls.Load(); calls != 1 {
		t.Fatalf("HEAD calls = %d, want 1", calls)
	}
	if calls := objectReads.Load(); calls != 0 {
		t.Fatalf("object reads = %d, want 0", calls)
	}
}

func TestDat9FSDiskReadCacheUnverifiedRevisionMismatchRefetches(t *testing.T) {
	const path = "/file.bin"
	const oldRev = int64(7)
	const newRev = int64(8)
	var headCalls atomic.Int32
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithStatAndRangeObject(t, defaultReadCacheMaxFileSize+1024, func(w http.ResponseWriter, r *http.Request) {
		headCalls.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(defaultReadCacheMaxFileSize+1024))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(newRev, 10))
		w.WriteHeader(http.StatusOK)
	}, func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-4" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("fresh"))
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, oldRev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	oldKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: oldRev, Offset: 0, Length: 5}
	newKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: newRev, Offset: 0, Length: 5}
	fs.diskReadCache.PutOwned(oldKey, []byte("stale"))
	fs.markStatCacheUnverified()

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 5)
	if err != nil || st != gofuse.OK || string(got) != "fresh" {
		t.Fatalf("read = %q, %v, %v; want fresh OK", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, newKey)
	if got, ok := fs.diskReadCache.Get(oldKey); ok {
		t.Fatalf("old revision cache still hit after mismatch: %q", got)
	}
	if calls := headCalls.Load(); calls != 1 {
		t.Fatalf("HEAD calls = %d, want 1", calls)
	}
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1", calls)
	}
}

func TestDat9FSDiskReadCacheRevalidatesWhenLocalEventsUntrusted(t *testing.T) {
	const path = "/file.bin"
	const oldRev = int64(7)
	const newRev = int64(8)
	var headCalls atomic.Int32
	var objectReads atomic.Int32

	fs, ino, cleanup := newTestDat9FSWithStatAndRangeObject(t, defaultReadCacheMaxFileSize+1024, func(w http.ResponseWriter, r *http.Request) {
		headCalls.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(defaultReadCacheMaxFileSize+1024))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(newRev, 10))
		w.WriteHeader(http.StatusOK)
	}, func(w http.ResponseWriter, r *http.Request) {
		objectReads.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-4" {
			http.Error(w, "wrong range: "+got, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("fresh"))
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.markStatCacheVerified()
	fs.inodes.UpdateRevision(ino, oldRev)
	fh := openDat9FSTestHandle(t, fs, ino, path)
	defer fs.fileHandles.Delete(fh)
	oldKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: oldRev, Offset: 0, Length: 5}
	newKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: newRev, Offset: 0, Length: 5}
	fs.diskReadCache.PutOwned(oldKey, []byte("stale"))

	got, st, err := readDat9FSTestRange(fs, ino, fh, 0, 5)
	if err != nil || st != gofuse.OK || string(got) != "fresh" {
		t.Fatalf("read = %q, %v, %v; want fresh OK", got, st, err)
	}
	waitForDiskReadCacheEntry(t, fs.diskReadCache, newKey)
	if got, ok := fs.diskReadCache.Get(oldKey); ok {
		t.Fatalf("old revision cache still hit after untrusted revalidation: %q", got)
	}
	if calls := headCalls.Load(); calls != 1 {
		t.Fatalf("HEAD calls = %d, want 1", calls)
	}
	if calls := objectReads.Load(); calls != 1 {
		t.Fatalf("object reads = %d, want 1", calls)
	}
}

func mustGetInodeEntry(t *testing.T, fs *Dat9FS, ino uint64) *InodeEntry {
	t.Helper()
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatalf("inode %d missing", ino)
	}
	return entry
}

func TestDat9FSReadSingleFlightSameRevisionDedupsBackendRead(t *testing.T) {
	const (
		path    = "/file.bin"
		rev     = int64(7)
		readers = 3
	)
	data := []byte("same-revision-data")
	started := make(chan struct{}, readers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReads := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseReads)
	var getCalls atomic.Int32
	var (
		handlerMu  sync.Mutex
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Path != "/v1/fs/file.bin" {
				recordHandlerErr(fmt.Errorf("GET path = %q, want /v1/fs/file.bin", r.URL.Path))
				http.NotFound(w, r)
				return
			}
			getCalls.Add(1)
			started <- struct{}{}
			<-release
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		default:
			recordHandlerErr(fmt.Errorf("unexpected method %s", r.Method))
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, rev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	var wg sync.WaitGroup
	errs := make(chan error, readers)
	results := make(chan []byte, readers)
	startReaders := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startReaders
			got, st, err := readDat9FSTestBytes(fs, ino, out.Fh, len(data))
			if err != nil {
				errs <- err
				return
			}
			if st != gofuse.OK {
				errs <- fmt.Errorf("Read status = %v, want OK", st)
				return
			}
			results <- got
		}()
	}
	close(startReaders)

	select {
	case <-started:
	case <-time.After(time.Second):
		releaseReads()
		t.Fatal("backend GET did not start")
	}
	waitForWaiters(t, fs.readFlight, fmt.Sprintf("%s@%d", path, rev), readers-1)
	releaseReads()
	wg.Wait()
	close(errs)
	close(results)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for got := range results {
		if !bytes.Equal(got, data) {
			t.Fatalf("Read data = %q, want %q", got, data)
		}
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("GET calls = %d, want 1", got)
	}
	if cached, ok := fs.readCache.Get(path, rev); !ok || !bytes.Equal(cached, data) {
		t.Fatalf("readCache.Get(%q, %d) = %q, %v; want cached data", path, rev, cached, ok)
	}
	handlerMu.Lock()
	err := handlerErr
	handlerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDat9FSReadSingleFlightDifferentRevisionNotShared(t *testing.T) {
	const path = "/file.bin"
	rev1Data := []byte("revision-one")
	rev2Data := []byte("revision-two")
	started := make(chan int32, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReads := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseReads)
	var getCalls atomic.Int32
	var (
		handlerMu  sync.Mutex
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(rev2Data)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Path != "/v1/fs/file.bin" {
				recordHandlerErr(fmt.Errorf("GET path = %q, want /v1/fs/file.bin", r.URL.Path))
				http.NotFound(w, r)
				return
			}
			call := getCalls.Add(1)
			started <- call
			<-release
			switch call {
			case 1:
				w.Header().Set("Content-Length", strconv.Itoa(len(rev1Data)))
				_, _ = w.Write(rev1Data)
			case 2:
				w.Header().Set("Content-Length", strconv.Itoa(len(rev2Data)))
				_, _ = w.Write(rev2Data)
			default:
				recordHandlerErr(fmt.Errorf("GET call count = %d, want <= 2", call))
				http.Error(w, "unexpected extra read", http.StatusInternalServerError)
			}
		default:
			recordHandlerErr(fmt.Errorf("unexpected method %s", r.Method))
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, int64(len(rev2Data)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	firstDone := make(chan []byte, 1)
	firstErr := make(chan error, 1)
	go func() {
		got, st, err := readDat9FSTestBytes(fs, ino, out.Fh, len(rev2Data))
		if err != nil {
			firstErr <- err
			return
		}
		if st != gofuse.OK {
			firstErr <- fmt.Errorf("first Read status = %v, want OK", st)
			return
		}
		firstDone <- got
	}()

	select {
	case call := <-started:
		if call != 1 {
			releaseReads()
			t.Fatalf("first started GET call = %d, want 1", call)
		}
	case <-time.After(time.Second):
		releaseReads()
		t.Fatal("first backend GET did not start")
	}

	// Simulate a revalidated lookup/invalidation advancing the observed
	// revision while the old revision read is still in flight.
	fs.inodes.UpdateRevision(ino, 2)

	secondDone := make(chan []byte, 1)
	secondErr := make(chan error, 1)
	go func() {
		got, st, err := readDat9FSTestBytes(fs, ino, out.Fh, len(rev2Data))
		if err != nil {
			secondErr <- err
			return
		}
		if st != gofuse.OK {
			secondErr <- fmt.Errorf("second Read status = %v, want OK", st)
			return
		}
		secondDone <- got
	}()

	select {
	case call := <-started:
		if call != 2 {
			releaseReads()
			t.Fatalf("second started GET call = %d, want 2", call)
		}
	case <-time.After(time.Second):
		releaseReads()
		t.Fatal("second backend GET did not start; reads for different revisions were likely shared")
	}

	releaseReads()

	var first []byte
	select {
	case err := <-firstErr:
		t.Fatal(err)
	case first = <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Read did not finish")
	}
	var second []byte
	select {
	case err := <-secondErr:
		t.Fatal(err)
	case second = <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second Read did not finish")
	}

	if !bytes.Equal(first, rev1Data) {
		t.Fatalf("first Read data = %q, want %q", first, rev1Data)
	}
	if !bytes.Equal(second, rev2Data) {
		t.Fatalf("second Read data = %q, want %q", second, rev2Data)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("GET calls = %d, want 2", got)
	}
	handlerMu.Lock()
	err := handlerErr
	handlerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// TestDat9FSReadSingleFlightOwnerCancelDoesNotFailPiggybacker verifies that
// cancelling the owner's FUSE request context does not fail piggybacking
// readers. The shared HTTP fetch uses a detached context so it runs to
// completion regardless of the owner's cancellation.
func TestDat9FSReadSingleFlightOwnerCancelDoesNotFailPiggybacker(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(3)
	)
	data := []byte("owner-cancel-data")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReads := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseReads)
	var getCalls atomic.Int32
	var (
		handlerMu  sync.Mutex
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Path != "/v1/fs/file.bin" {
				recordHandlerErr(fmt.Errorf("GET path = %q, want /v1/fs/file.bin", r.URL.Path))
				http.NotFound(w, r)
				return
			}
			getCalls.Add(1)
			started <- struct{}{}
			<-release
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		default:
			recordHandlerErr(fmt.Errorf("unexpected method %s", r.Method))
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(path, false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, rev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	sfKey := fmt.Sprintf("%s@%d", path, rev)

	// Owner read with a cancel channel we control.
	ownerCancel := make(chan struct{})
	ownerDone := make(chan struct{})
	var ownerResult []byte
	var ownerSt gofuse.Status
	go func() {
		defer close(ownerDone)
		buf := make([]byte, len(data))
		var result gofuse.ReadResult
		result, ownerSt = fs.Read(ownerCancel, &gofuse.ReadIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       out.Fh,
			Offset:   0,
			Size:     uint32(len(data)),
		}, buf)
		if result != nil {
			ownerResult, _ = result.Bytes(buf)
			ownerResult = append([]byte(nil), ownerResult...)
		}
	}()

	// Wait for the backend GET to start (owner is in the flight callback).
	select {
	case <-started:
	case <-time.After(time.Second):
		releaseReads()
		t.Fatal("backend GET did not start")
	}

	// Start a piggybacker reader (no cancel).
	piggyDone := make(chan []byte, 1)
	piggyErr := make(chan error, 1)
	go func() {
		got, st, err := readDat9FSTestBytes(fs, ino, out.Fh, len(data))
		if err != nil {
			piggyErr <- err
			return
		}
		if st != gofuse.OK {
			piggyErr <- fmt.Errorf("piggy Read status = %v, want OK", st)
			return
		}
		piggyDone <- got
	}()

	// Wait for the piggybacker to attach to the in-flight entry.
	waitForWaiters(t, fs.readFlight, sfKey, 1)

	// Cancel the owner's FUSE context while the HTTP fetch is in-flight.
	// Because the singleflight callback uses a detached context
	// (context.WithoutCancel), this cancel does NOT fail the shared HTTP
	// fetch. The owner is executing fn() directly so it blocks until the
	// fetch completes — the cancel only affects piggybackers' select.
	close(ownerCancel)

	// Release the HTTP fetch — the detached context is still valid despite
	// the owner cancel.
	releaseReads()

	// Owner should complete with OK (detached fetch succeeded).
	select {
	case <-ownerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("owner Read did not return")
	}
	if ownerSt != gofuse.OK {
		t.Fatalf("owner status = %v, want OK (detached fetch should succeed)", ownerSt)
	}
	if !bytes.Equal(ownerResult, data) {
		t.Fatalf("owner data = %q, want %q", ownerResult, data)
	}

	// Piggybacker must also succeed with correct data.
	select {
	case err := <-piggyErr:
		t.Fatalf("piggybacker failed: %v", err)
	case got := <-piggyDone:
		if !bytes.Equal(got, data) {
			t.Fatalf("piggybacker data = %q, want %q", got, data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("piggybacker Read did not finish")
	}

	// Only one GET should have been made.
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("GET calls = %d, want 1", got)
	}

	handlerMu.Lock()
	err := handlerErr
	handlerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDat9FSReadSingleFlightUnresponsiveFetchIsBounded(t *testing.T) {
	const (
		path = "/file.bin"
		rev  = int64(9)
	)
	data := []byte("hung-read-data")
	started := make(chan struct{}, 1)
	firstCtxDone := make(chan struct{}, 1)
	var getCalls atomic.Int32
	var (
		handlerMu  sync.Mutex
		handlerErr error
	)
	recordHandlerErr := func(err error) {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Path != "/v1/fs/file.bin" {
				recordHandlerErr(fmt.Errorf("GET path = %q, want /v1/fs/file.bin", r.URL.Path))
				http.NotFound(w, r)
				return
			}
			call := getCalls.Add(1)
			if call == 1 {
				started <- struct{}{}
				<-r.Context().Done()
				firstCtxDone <- struct{}{}
				return
			}
			http.Error(w, "retry should not hang", http.StatusBadRequest)
		default:
			recordHandlerErr(fmt.Errorf("unexpected method %s", r.Method))
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.remoteReadTimeout = 30 * time.Millisecond
	fs.readSlots = make(chan struct{}, 1)
	ino := fs.inodes.Lookup(path, false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, rev)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	readDone := make(chan gofuse.Status, 1)
	go func() {
		_, st, err := readDat9FSTestBytes(fs, ino, out.Fh, len(data))
		if err != nil {
			readDone <- gofuse.EIO
			return
		}
		readDone <- st
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("backend GET did not start")
	}

	select {
	case <-firstCtxDone:
	case <-time.After(time.Second):
		t.Fatal("unresponsive backend GET was not cancelled by bounded shared context")
	}

	select {
	case st := <-readDone:
		if st == gofuse.OK {
			t.Fatal("Read status = OK, want failure for hung backend")
		}
	case <-time.After(time.Second):
		t.Fatal("Read did not return after bounded shared context expired")
	}

	if got := fs.readFlight.Inflight(); got != 0 {
		t.Fatalf("readFlight.Inflight() = %d, want 0 after bounded failure", got)
	}
	slotCtx, slotCancel := context.WithTimeout(context.Background(), time.Second)
	releaseSlot, err := fs.acquireRemoteReadSlot(slotCtx)
	slotCancel()
	if err != nil {
		t.Fatalf("acquireRemoteReadSlot after bounded failure: %v", err)
	}
	releaseSlot()

	if got := getCalls.Load(); got < 1 {
		t.Fatalf("GET calls = %d, want at least 1", got)
	}
	handlerMu.Lock()
	handlerError := handlerErr
	handlerMu.Unlock()
	if handlerError != nil {
		t.Fatal(handlerError)
	}
}

func TestCreateWriteThroughShadow(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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

func TestOpenWritablePendingShadowSkipsRemoteStat(t *testing.T) {
	var headCalls atomic.Int32
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			_, _ = io.WriteString(w, "stale")
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup("/file.bin", false, 5, time.Now())

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
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
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
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("GET calls = %d, want 0", got)
	}
}

func TestFlushLargeOverwritePatchCarriesExpectedRevision(t *testing.T) {
	const (
		fileSize = defaultSmallFileThreshold + 1024
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	const fileSize = defaultSmallFileThreshold + 2048

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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

func TestFlushSmallNewFileRefreshesSiblingHandleRevision(t *testing.T) {
	const filePath = "/sqlite/workload.db-journal"

	var (
		mu        sync.Mutex
		revision  int64
		content   []byte
		expected  []int64
		serverErr error
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs/sqlite/workload.db-journal" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			mu.Lock()
			if serverErr == nil {
				serverErr = err
			}
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotExpected, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
		if err != nil {
			mu.Lock()
			if serverErr == nil {
				serverErr = fmt.Errorf("expected revision header: %w", err)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		defer mu.Unlock()
		expected = append(expected, gotExpected)
		if gotExpected != revision {
			http.Error(w, "revision conflict", http.StatusConflict)
			return
		}
		revision++
		content = append([]byte(nil), body...)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": revision})
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())

	first := NewWriteBuffer(filePath, 0, 0)
	if _, err := first.Write(0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	fh1 := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: first,
		IsNew: true,
	}
	fh1.DirtySeq = fs.markDirtySize(ino, first.Size())
	fh1ID := fs.allocateFileHandle(fh1)

	second := NewWriteBuffer(filePath, 0, 0)
	if _, err := second.Write(0, []byte("second")); err != nil {
		t.Fatal(err)
	}
	fh2 := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: second,
		IsNew: true,
	}
	fh2.DirtySeq = fs.markDirtySize(ino, second.Size())
	fh2ID := fs.allocateFileHandle(fh2)

	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh1ID}); st != gofuse.OK {
		t.Fatalf("first Flush status = %v, want OK", st)
	}
	if fh2.IsNew {
		t.Fatal("second handle should no longer be create-if-absent after sibling commit")
	}
	if fh2.BaseRev != 1 {
		t.Fatalf("second handle BaseRev = %d, want 1", fh2.BaseRev)
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh2ID}); st != gofuse.OK {
		t.Fatalf("second Flush status = %v, want OK", st)
	}

	mu.Lock()
	defer mu.Unlock()
	if serverErr != nil {
		t.Fatal(serverErr)
	}
	if !reflect.DeepEqual(expected, []int64{0, 1}) {
		t.Fatalf("expected revisions = %v, want [0 1]", expected)
	}
	if revision != 2 {
		t.Fatalf("server revision = %d, want 2", revision)
	}
	if string(content) != "second" {
		t.Fatalf("server content = %q, want second", content)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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

func TestGetAttrUsesRevisionBoundDirCacheStatWithoutRemoteHead(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/cached.txt", false, 1, time.Unix(1, 0))
	mtime := time.Unix(123, 0)
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Mtime:    mtime,
		Revision: 7,
		Mode:     0o600,
		HasMode:  true,
	})

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	if got, want := out.Size, uint64(12); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
	if got, want := out.Mode&0o777, uint32(0o600); got != want {
		t.Fatalf("GetAttr mode = %o, want %o", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Revision != 7 {
		t.Fatalf("inode revision = %d, want 7", entry.Revision)
	}
	if !entry.Mtime.Equal(mtime) {
		t.Fatalf("inode mtime = %s, want %s", entry.Mtime, mtime)
	}
}

func TestGetAttrDoesNotTrustLocalEventsByDefault(t *testing.T) {
	var headCalls atomic.Int32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "33")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "8")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/cached.txt", false, 1, time.Unix(1, 0))
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Revision: 7,
	})

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got, want := out.Size, uint64(33); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
}

func TestGetAttrIgnoresOlderDirCacheRevision(t *testing.T) {
	var headCalls atomic.Int32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "33")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "10")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/cached.txt", false, 1, time.Unix(1, 0))
	fs.inodes.UpdateRevision(ino, 9)
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Revision: 7,
	})

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got, want := out.Size, uint64(33); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Revision != 10 {
		t.Fatalf("inode revision = %d, want 10", entry.Revision)
	}
}

func TestGetAttrSSEChangeInvalidatesCachedStat(t *testing.T) {
	var headCalls atomic.Int32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "44")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "8")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/docs/readme.md", false, 12, time.Unix(1, 0))
	fs.inodes.UpdateRevision(ino, 7)
	fs.dirCache.Upsert("/docs", CachedFileInfo{
		Name:     "readme.md",
		Size:     12,
		IsDir:    false,
		Revision: 7,
	})

	w := &SSEWatcher{fs: fs, actor: "mount-b"}
	w.handleChange(&client.ChangeEvent{
		Seq:   1,
		Path:  "/docs/readme.md",
		Op:    "write",
		Actor: "mount-a",
	})

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got, want := out.Size, uint64(44); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Revision != 8 {
		t.Fatalf("inode revision = %d, want 8", entry.Revision)
	}
}

func TestSetAttrRefreshesDirCacheStatMetadata(t *testing.T) {
	var chmodCalls atomic.Int32
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "unexpected stale-cache revalidation", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	oldMtime := time.Unix(10, 0)
	newMtime := time.Unix(123, 0)
	ino := fs.inodes.Lookup("/cached.txt", false, 12, oldMtime)
	fs.inodes.UpdateRevision(ino, 7)
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Mtime:    oldMtime,
		Revision: 7,
		Mode:     0o644,
		HasMode:  true,
	})

	var setOut gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE | gofuse.FATTR_MTIME,
			Mode:     0o600,
			Mtime:    uint64(newMtime.Unix()),
		},
	}, &setOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}

	var out gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if got, want := out.Mode&0o777, uint32(0o600); got != want {
		t.Fatalf("GetAttr mode = %o, want %o", got, want)
	}
	if got, want := out.Mtime, uint64(newMtime.Unix()); got != want {
		t.Fatalf("GetAttr mtime = %d, want %d", got, want)
	}
}

func TestSetAttrModePreservesSpecialPermissionBits(t *testing.T) {
	var chmodCalls atomic.Int32
	var gotMode uint32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			var req struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				handlerErrors.Recordf("decode chmod body: %v", err)
				http.Error(w, "bad body", http.StatusBadRequest)
				return
			}
			gotMode = req.Mode
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/special.txt", false, 12, time.Unix(10, 0))

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o6755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if gotMode != 0o755 {
		t.Fatalf("remote chmod mode = %o, want 0755", gotMode)
	}
	if got, want := out.Mode&0o7777, uint32(0o6755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if got, want := entry.Mode&0o7777, uint32(0o6755); got != want {
		t.Fatalf("inode mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeUsesDirectoryRemotePath(t *testing.T) {
	var chmodCalls atomic.Int32
	var gotPath string
	var gotMode uint32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			gotPath = r.URL.Path
			var req struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				handlerErrors.Recordf("decode chmod body: %v", err)
				http.Error(w, "bad body", http.StatusBadRequest)
				return
			}
			gotMode = req.Mode
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/dir", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFDIR)|0o755)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     uint32(syscall.S_IFDIR) | 0o1755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if gotPath != "/v1/fs/dir/" {
		t.Fatalf("chmod path = %q, want /v1/fs/dir/", gotPath)
	}
	if gotMode != 0o755 {
		t.Fatalf("remote chmod mode = %o, want 0755", gotMode)
	}
	if got, want := out.Mode&0o7777, uint32(0o1755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeRetriesTransientPostUploadNotFound(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			if chmodCalls.Add(1) == 1 {
				http.Error(w, "not yet visible", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/racy.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o644)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o600,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got := chmodCalls.Load(); got != 2 {
		t.Fatalf("remote chmod calls = %d, want 2", got)
	}
	if got, want := out.Mode&0o7777, uint32(0o600); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeKeepsLocalMetadataWhenRemoteChmodNotFoundButTargetExists(t *testing.T) {
	var chmodCalls atomic.Int32
	var statCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/chowned.txt":
			statCalls.Add(1)
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/chowned.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o755)
	fs.inodes.UpdateOwner(ino, 65534, 65534, true, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: ino,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
				},
			},
			Valid: gofuse.FATTR_MODE,
			Mode:  0o2755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("remote chmod calls = %d, want 1", got)
	}
	if got := statCalls.Load(); got != 1 {
		t.Fatalf("remote stat calls = %d, want 1", got)
	}
	if got, want := out.Mode&0o7777, uint32(0o2755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeRemoteChmodNotFoundStillFailsWhenTargetMissing(t *testing.T) {
	var chmodCalls atomic.Int32
	var statCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodHead:
			statCalls.Add(1)
			http.Error(w, "missing", http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/missing-remotely.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o755)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o600,
		},
	}, &out)
	if st != gofuse.ENOENT {
		t.Fatalf("SetAttr status = %v, want ENOENT", st)
	}
	if got := chmodCalls.Load(); got != postUploadModeAttempts {
		t.Fatalf("remote chmod calls = %d, want %d", got, postUploadModeAttempts)
	}
	if got := statCalls.Load(); got != postUploadModeAttempts {
		t.Fatalf("remote stat calls = %d, want %d", got, postUploadModeAttempts)
	}
}

func TestSetAttrModeRejectsNonOwner(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			chmodCalls.Add(1)
			http.Error(w, "non-owner chmod should not reach server", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/owned.txt", false, 12, time.Unix(10, 0))
	oldCTime := time.Unix(20, 0)
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(ino, 1001, 1002, true, true)
	fs.inodes.UpdateCtime(ino, oldCTime)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: ino,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
				},
			},
			Valid: gofuse.FATTR_MODE,
			Mode:  0o111,
		},
	}, &out)
	if st != gofuse.EPERM {
		t.Fatalf("SetAttr status = %v, want EPERM", st)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("remote chmod calls = %d, want 0", got)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if got, want := entry.Mode&0o7777, uint32(0o644); got != want {
		t.Fatalf("inode mode = %o, want %o", got, want)
	}
	if !entry.Ctime.Equal(oldCTime) {
		t.Fatalf("ctime = %v, want unchanged %v", entry.Ctime, oldCTime)
	}
}

func TestSetAttrPathModeRequiresParentSearchPermission(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			chmodCalls.Add(1)
			http.Error(w, "chmod should not reach server without parent search permission", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	parentIno := fs.inodes.Lookup("/locked", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(parentIno, uint32(syscall.S_IFDIR)|0o644)
	fs.inodes.UpdateOwner(parentIno, 1001, 1001, true, true)
	ino := fs.inodes.Lookup("/locked/file.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(ino, 65534, 65534, true, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: ino,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
				},
			},
			Valid: gofuse.FATTR_MODE,
			Mode:  0o600,
		},
	}, &out)
	if st != gofuse.EACCES {
		t.Fatalf("SetAttr status = %v, want EACCES", st)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("remote chmod calls = %d, want 0", got)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if got, want := entry.Mode&0o7777, uint32(0o644); got != want {
		t.Fatalf("inode mode = %o, want %o", got, want)
	}
}

func TestSetAttrFileHandleModeSkipsParentSearchPermission(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	parentIno := fs.inodes.Lookup("/locked", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(parentIno, uint32(syscall.S_IFDIR)|0o644)
	fs.inodes.UpdateOwner(parentIno, 1001, 1001, true, true)
	ino := fs.inodes.Lookup("/locked/file.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(ino, 65534, 65534, true, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: ino,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
				},
			},
			Valid: gofuse.FATTR_MODE | gofuse.FATTR_FH,
			Fh:    99,
			Mode:  0o600,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("remote chmod calls = %d, want 1", got)
	}
	if got, want := out.Mode&0o7777, uint32(0o600); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeClearsSetGIDForRegularFileWhenCallerNotInFileGroup(t *testing.T) {
	var chmodCalls atomic.Int32
	var gotMode uint32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls.Add(1)
			var req struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				handlerErrors.Recordf("decode chmod body: %v", err)
				http.Error(w, "bad body", http.StatusBadRequest)
				return
			}
			gotMode = req.Mode
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/setgid.txt", false, 12, time.Unix(10, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o2755)
	fs.inodes.UpdateOwner(ino, 65534, 65534, true, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: ino,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65533},
				},
			},
			Valid: gofuse.FATTR_MODE,
			Mode:  0o2755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if gotMode != 0o755 {
		t.Fatalf("remote chmod mode = %o, want 0755", gotMode)
	}
	if got, want := out.Mode&0o7777, uint32(0o755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if got, want := entry.Mode&0o7777, uint32(0o755); got != want {
		t.Fatalf("inode mode = %o, want %o", got, want)
	}
}

func TestSetAttrModeUpdatesPendingMetadataWithoutRemoteChmod(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			chmodCalls.Add(1)
			http.Error(w, "remote object not committed yet", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatalf("NewPendingIndex: %v", err)
	}
	fs.pendingIndex = idx

	const path = "/pending-new.txt"
	if _, err := fs.pendingIndex.PutWithBaseRevAndMode(path, 0, PendingNew, 0, 0, false); err != nil {
		t.Fatalf("PutWithBaseRevAndMode: %v", err)
	}
	ino := fs.inodes.Lookup(path, false, 0, time.Unix(10, 0))

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("remote chmod calls = %d, want 0", got)
	}
	meta, ok := fs.pendingIndex.GetMeta(path)
	if !ok {
		t.Fatal("pending metadata missing")
	}
	if !meta.HasMode || meta.Mode != 0o755 {
		t.Fatalf("pending mode = %o/%t, want 755/true", meta.Mode, meta.HasMode)
	}
	if got, want := out.Mode&0o777, uint32(0o755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
}

func TestMknodMetadataOnlySpecialNodeLifecycle(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unexpected remote mutation", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{
			NodeId: 1,
			Caller: gofuse.Caller{
				Owner: gofuse.Owner{Uid: 65534, Gid: 65533},
			},
		},
		Mode: uint32(syscall.S_IFIFO) | 0o644,
		Rdev: 12,
	}, "pipe", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls after Mknod = %d, want one HEAD probe", got)
	}
	if got, want := out.Mode&uint32(syscall.S_IFMT), uint32(syscall.S_IFIFO); got != want {
		t.Fatalf("Mknod kind = %o, want %o", got, want)
	}
	if got, want := out.Mode&0o7777, uint32(0o644); got != want {
		t.Fatalf("Mknod mode = %o, want %o", got, want)
	}
	if out.Uid != 65534 || out.Gid != 65533 {
		t.Fatalf("Mknod owner = %d:%d, want 65534:65533", out.Uid, out.Gid)
	}
	if out.Rdev != 12 {
		t.Fatalf("Mknod rdev = %d, want 12", out.Rdev)
	}

	var lookup gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "pipe", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls after Lookup = %d, want unchanged", got)
	}

	var attr gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: out.NodeId},
			Valid:    gofuse.FATTR_MODE | gofuse.FATTR_UID | gofuse.FATTR_GID,
			Mode:     0o6755,
			Owner:    gofuse.Owner{Uid: 123, Gid: 456},
		},
	}, &attr)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got, want := attr.Mode&uint32(syscall.S_IFMT), uint32(syscall.S_IFIFO); got != want {
		t.Fatalf("SetAttr kind = %o, want %o", got, want)
	}
	if got, want := attr.Mode&0o7777, uint32(0o6755); got != want {
		t.Fatalf("SetAttr mode = %o, want %o", got, want)
	}
	if attr.Uid != 123 || attr.Gid != 456 {
		t.Fatalf("SetAttr owner = %d:%d, want 123:456", attr.Uid, attr.Gid)
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls after SetAttr = %d, want unchanged", got)
	}

	entries := fs.mergePendingDirEntries("/", nil)
	var listed bool
	for _, e := range entries {
		if e.Name == "pipe" {
			listed = true
			if got, want := e.Mode&uint32(syscall.S_IFMT), uint32(syscall.S_IFIFO); got != want {
				t.Fatalf("dir entry kind = %o, want %o", got, want)
			}
		}
	}
	if !listed {
		t.Fatal("special node missing from listDir")
	}

	st = fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "pipe", "renamed-pipe")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "renamed-pipe", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup renamed status = %v, want OK", st)
	}

	st = fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "renamed-pipe")
	if st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if _, ok := fs.specialNodeEntry("/renamed-pipe"); ok {
		t.Fatal("special node still present after unlink")
	}
}

func TestFinishLocalRenameRetargetsSpecialNodeSubtree(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), trustedProcessLocalEventsOptions())
	_ = fs.inodes.Lookup("/d", true, 0, time.Unix(10, 0))
	childIno := fs.inodes.Lookup("/d/pipe", false, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(childIno, uint32(syscall.S_IFIFO)|0o644)
	fs.addSpecialNode("/d/pipe", childIno)

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "/d", "/e")

	if _, ok := fs.specialNodeEntry("/d/pipe"); ok {
		t.Fatal("old special-node path remained after directory rename")
	}
	if _, ok := fs.specialNodeEntry("/e/pipe"); !ok {
		t.Fatal("renamed special-node path missing after directory rename")
	}
	var lookup gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "e", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup renamed directory status = %v, want OK", st)
	}
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: lookup.NodeId}, "pipe", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup renamed special child status = %v, want OK", st)
	}
}

func TestMknodMetadataOnlySpecialRevalidatesNegativeCache(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		if r.Method == http.MethodHead && r.URL.Path == "/v1/fs/stale" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	fs.cacheNegativePath("/stale")

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "stale", &out)
	if st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("Mknod status = %v, want EEXIST", st)
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls = %d, want 1", got)
	}
	if _, ok := fs.specialNodeEntry("/stale"); ok {
		t.Fatal("special node shadowed remote path")
	}
}

func TestLinkMetadataOnlySpecialNodeCreatesLocalAlias(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unexpected remote mutation", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "pipe", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	var linkOut gofuse.EntryOut
	st = fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: out.NodeId,
	}, "pipe-link", &linkOut)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	if got := linkOut.NodeId; got != out.NodeId {
		t.Fatalf("link node id = %d, want %d", got, out.NodeId)
	}
	if got := linkOut.Nlink; got != 2 {
		t.Fatalf("link nlink = %d, want 2", got)
	}
	if _, ok := fs.specialNodeEntry("/pipe-link"); !ok {
		t.Fatal("linked special node missing")
	}

	var lookup gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "pipe", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := lookup.Nlink; got != 2 {
		t.Fatalf("source nlink = %d, want 2", got)
	}

	st = fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "pipe-link")
	if st != gofuse.OK {
		t.Fatalf("Unlink alias status = %v, want OK", st)
	}
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "pipe", &lookup)
	if st != gofuse.OK {
		t.Fatalf("Lookup after unlink status = %v, want OK", st)
	}
	if got := lookup.Nlink; got != 1 {
		t.Fatalf("source nlink after unlink = %d, want 1", got)
	}
	if got := remoteCalls.Load(); got != 2 {
		t.Fatalf("remote calls = %d, want HEAD probes only", got)
	}
}

func TestSetAttrModeRejectsNonOwnerMetadataOnlySpecialNode(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		if r.Method == http.MethodHead {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unexpected remote mutation", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	var entryOut gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{
			NodeId: 1,
			Caller: gofuse.Caller{
				Owner: gofuse.Owner{Uid: 1001, Gid: 1002},
			},
		},
		Mode: uint32(syscall.S_IFIFO) | 0o644,
	}, "pipe", &entryOut)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	oldCTime := time.Unix(20, 0)
	fs.inodes.UpdateCtime(entryOut.NodeId, oldCTime)

	var attr gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{
				NodeId: entryOut.NodeId,
				Caller: gofuse.Caller{
					Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
				},
			},
			Valid: gofuse.FATTR_MODE,
			Mode:  0o111,
		},
	}, &attr)
	if st != gofuse.EPERM {
		t.Fatalf("SetAttr status = %v, want EPERM", st)
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls = %d, want only initial HEAD", got)
	}
	entry, ok := fs.inodes.GetEntry(entryOut.NodeId)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if got, want := entry.Mode&0o7777, uint32(0o644); got != want {
		t.Fatalf("inode mode = %o, want %o", got, want)
	}
	if !entry.Ctime.Equal(oldCTime) {
		t.Fatalf("ctime = %v, want unchanged %v", entry.Ctime, oldCTime)
	}
}

func TestRenameRejectsStickySourceParentWhenCallerOwnsNeitherParentNorSource(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "rename should fail before remote mutation", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	stickyIno := fs.inodes.Lookup("/sticky", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(stickyIno, uint32(syscall.S_IFDIR)|0o1777)
	fs.inodes.UpdateOwner(stickyIno, 0, 0, true, true)
	dstIno := fs.inodes.Lookup("/dst", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(dstIno, uint32(syscall.S_IFDIR)|0o777)
	fs.inodes.UpdateOwner(dstIno, 65534, 65534, true, true)
	srcIno := fs.inodes.Lookup("/sticky/file", false, 1, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(srcIno, 0, 0, true, true)

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{
			NodeId: stickyIno,
			Caller: gofuse.Caller{
				Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
			},
		},
		Newdir: dstIno,
	}, "file", "file")
	if st != gofuse.EPERM {
		t.Fatalf("Rename status = %v, want EPERM", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	if _, ok := fs.inodes.GetInode("/sticky/file"); !ok {
		t.Fatal("source inode should remain after rejected rename")
	}
}

func TestRenameRejectsStickyTargetParentWhenCallerOwnsNeitherParentNorTarget(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "rename should fail before remote mutation", http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	srcParentIno := fs.inodes.Lookup("/src", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcParentIno, uint32(syscall.S_IFDIR)|0o777)
	fs.inodes.UpdateOwner(srcParentIno, 65534, 65534, true, true)
	stickyIno := fs.inodes.Lookup("/sticky", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(stickyIno, uint32(syscall.S_IFDIR)|0o1777)
	fs.inodes.UpdateOwner(stickyIno, 0, 0, true, true)
	srcIno := fs.inodes.Lookup("/src/file", false, 1, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(srcIno, 65534, 65534, true, true)
	targetIno := fs.inodes.Lookup("/sticky/target", false, 1, time.Unix(10, 0))
	fs.inodes.UpdateMode(targetIno, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(targetIno, 0, 0, true, true)

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{
			NodeId: srcParentIno,
			Caller: gofuse.Caller{
				Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
			},
		},
		Newdir: stickyIno,
	}, "file", "target")
	if st != gofuse.EPERM {
		t.Fatalf("Rename status = %v, want EPERM", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	if _, ok := fs.inodes.GetInode("/src/file"); !ok {
		t.Fatal("source inode should remain after rejected rename")
	}
}

func TestRenamePreflightAllowsNonOwnerDirectoryAcrossWritableParents(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	srcParentIno := fs.inodes.Lookup("/src", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcParentIno, uint32(syscall.S_IFDIR)|0o777)
	fs.inodes.UpdateOwner(srcParentIno, 0, 0, true, true)
	dstParentIno := fs.inodes.Lookup("/dst", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(dstParentIno, uint32(syscall.S_IFDIR)|0o777)
	fs.inodes.UpdateOwner(dstParentIno, 0, 0, true, true)
	dirIno := fs.inodes.Lookup("/src/dir", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(dirIno, uint32(syscall.S_IFDIR)|0o755)
	fs.inodes.UpdateOwner(dirIno, 0, 0, true, true)

	_, _, st := fs.renamePreflight(context.Background(), &gofuse.RenameIn{
		InHeader: gofuse.InHeader{
			NodeId: srcParentIno,
			Caller: gofuse.Caller{
				Owner: gofuse.Owner{Uid: 65534, Gid: 65534},
			},
		},
		Newdir: dstParentIno,
	}, "/src/dir", "/dst/dir")
	if st != gofuse.OK {
		t.Fatalf("renamePreflight status = %v, want OK", st)
	}
}

func TestRenameMetadataOnlySpecialReplacesRemoteFile(t *testing.T) {
	var deleteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/pipe":
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/target":
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(uint32(syscall.S_IFREG)|0o644), 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/target":
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "pipe", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	st = fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "pipe", "target")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}
	if _, ok := fs.specialNodeEntry("/pipe"); ok {
		t.Fatal("old special node still present")
	}
	if _, ok := fs.specialNodeEntry("/target"); !ok {
		t.Fatal("renamed special node missing at target")
	}
}

func TestRenameRemoteFileReplacesMetadataOnlySpecialTarget(t *testing.T) {
	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/src":
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(uint32(syscall.S_IFREG)|0o644), 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/target":
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "rename":
			renameCalls.Add(1)
			if got := r.Header.Get("X-Dat9-Rename-Source"); got != "/src" {
				t.Errorf("rename source = %q, want /src", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	srcIno := fs.inodes.Lookup("/src", false, 1, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFREG)|0o644)
	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "target", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	st = fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "src", "target")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("rename calls = %d, want 1", got)
	}
	if _, ok := fs.specialNodeEntry("/target"); ok {
		t.Fatal("special target still present after remote rename")
	}
	if _, ok := fs.inodes.GetInode("/src"); ok {
		t.Fatal("old source inode still mapped")
	}
	if _, ok := fs.inodes.GetInode("/target"); !ok {
		t.Fatal("target inode missing after rename")
	}
}

func TestRenameRemoteDirReplacesEmptyRemoteDirTarget(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "list=1":
			record("list-target")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"entries":[]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "kind=dir":
			record("delete-target")
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "rename":
			record("rename")
			if got := r.Header.Get("X-Dat9-Rename-Source"); got != "/src" {
				t.Errorf("rename source = %q, want /src", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	srcIno := fs.inodes.Lookup("/src", true, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFDIR)|0o755)
	targetIno := fs.inodes.Lookup("/target", true, 0, time.Unix(11, 0))
	fs.inodes.UpdateMode(targetIno, uint32(syscall.S_IFDIR)|0o755)

	st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "src", "target")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}

	mu.Lock()
	gotEvents := strings.Join(events, ",")
	mu.Unlock()
	if gotEvents != "list-target,rename" {
		t.Fatalf("events = %q, want list-target,rename", gotEvents)
	}
	if _, ok := fs.inodes.GetInode("/src"); ok {
		t.Fatal("old source inode still mapped")
	}
	if _, ok := fs.inodes.GetInode("/target"); !ok {
		t.Fatal("target inode missing after rename")
	}
}

func TestFinishLocalRenameDirectoryAcrossParentsUpdatesParentNlink(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), trustedProcessLocalEventsOptions())
	now := time.Unix(10, 0)
	srcParentIno := fs.inodes.Lookup("/src-parent", true, 0, now)
	dstParentIno := fs.inodes.Lookup("/dst-parent", true, 0, now)
	fs.inodes.UpdateLinkCount(srcParentIno, 3)
	fs.inodes.UpdateLinkCount(dstParentIno, 2)
	childIno := fs.inodes.Lookup("/src-parent/child", true, 0, now)
	fs.inodes.UpdateLinkCount(childIno, 2)

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: srcParentIno},
		Newdir:   dstParentIno,
	}, "/src-parent/child", "/dst-parent/child")

	srcParent, ok := fs.inodes.GetEntry(srcParentIno)
	if !ok {
		t.Fatal("source parent missing")
	}
	if got := srcParent.Nlink; got != 2 {
		t.Fatalf("source parent nlink = %d, want 2", got)
	}
	dstParent, ok := fs.inodes.GetEntry(dstParentIno)
	if !ok {
		t.Fatal("target parent missing")
	}
	if got := dstParent.Nlink; got != 3 {
		t.Fatalf("target parent nlink = %d, want 3", got)
	}
	if _, ok := fs.inodes.GetInode("/src-parent/child"); ok {
		t.Fatal("old child path still mapped")
	}
	if got, ok := fs.inodes.GetInode("/dst-parent/child"); !ok || got != childIno {
		t.Fatalf("new child inode = %d/%v, want %d/true", got, ok, childIno)
	}
}

func TestCreateTouchesParentDirectoryChangeTime(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), trustedProcessLocalEventsOptions())
	oldTime := time.Unix(10, 0)
	parentIno := fs.inodes.Lookup("/dir", true, 0, oldTime)
	fs.inodes.UpdateMtime(parentIno, oldTime)
	fs.inodes.UpdateCtime(parentIno, oldTime)

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: parentIno},
		Flags:    uint32(syscall.O_WRONLY),
		Mode:     0o644,
	}, "file", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	parent, ok := fs.inodes.GetEntry(parentIno)
	if !ok {
		t.Fatal("parent inode missing")
	}
	if !parent.Mtime.After(oldTime) {
		t.Fatalf("parent mtime = %v, want after %v", parent.Mtime, oldTime)
	}
	if !parent.Ctime.After(oldTime) {
		t.Fatalf("parent ctime = %v, want after %v", parent.Ctime, oldTime)
	}
}

func TestRenameZeroByteRemoteFileToMissingTargetFallsBackToCreateDelete(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/target":
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "rename":
			record("rename")
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/target" && r.URL.RawQuery == "create=1":
			record("create-target")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 12})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/src" && r.URL.RawQuery == "kind=file":
			record("delete-src")
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	srcIno := fs.inodes.Lookup("/src", false, 0, time.Unix(10, 0))
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFREG)|0o644)

	st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "src", "target")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}

	mu.Lock()
	gotEvents := strings.Join(events, ",")
	mu.Unlock()
	if gotEvents != "rename,create-target,delete-src" {
		t.Fatalf("events = %q, want rename,create-target,delete-src", gotEvents)
	}
	if _, ok := fs.inodes.GetInode("/src"); ok {
		t.Fatal("old source inode still mapped")
	}
	if targetIno, ok := fs.inodes.GetInode("/target"); !ok {
		t.Fatal("target inode missing after rename")
	} else if targetIno != srcIno {
		t.Fatalf("target inode = %d, want original source inode %d", targetIno, srcIno)
	}
}

func TestChildPathRejectsOverlongNameAndAllowsLongPath(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), trustedProcessLocalEventsOptions())

	if _, st := fs.childPath(1, strings.Repeat("a", posixNameMax+1)); st != gofuse.Status(syscall.ENAMETOOLONG) {
		t.Fatalf("overlong name status = %v, want ENAMETOOLONG", st)
	}

	longParent := "/" + strings.TrimSuffix(strings.Repeat("a/", 4096), "/")
	longParentIno := fs.inodes.Lookup(longParent, true, 0, time.Unix(10, 0))
	if child, st := fs.childPath(longParentIno, "bb"); st != gofuse.OK {
		t.Fatalf("long path status = %v, want OK", st)
	} else if len(child) <= 4096 {
		t.Fatalf("long path length = %d, want > 4096", len(child))
	}
}

func TestGetAttrRevalidatesDirCacheStatWhenSSEUnverified(t *testing.T) {
	var headCalls atomic.Int32
	var handlerErrors testErrorRecorder
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "44")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "8")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	ino := fs.inodes.Lookup("/cached.txt", false, 12, time.Unix(1, 0))
	fs.inodes.UpdateRevision(ino, 7)
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Revision: 7,
	})
	fs.markStatCacheUnverified()

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	handlerErrors.Check(t)
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got, want := out.Size, uint64(44); got != want {
		t.Fatalf("GetAttr size = %d, want %d", got, want)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Revision != 8 {
		t.Fatalf("inode revision = %d, want 8", entry.Revision)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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

func TestLookupStatNotFoundReturnsENOENTWithoutParentListByDefault(t *testing.T) {
	var headCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Query().Get("list") == "1" {
				listCalls.Add(1)
			}
			http.Error(w, "unexpected list fallback", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "missing", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("list calls = %d, want 0", got)
	}
	if out.NodeId != 0 {
		t.Fatalf("negative lookup NodeId = %d, want 0", out.NodeId)
	}
}

func TestLookupLegacyDirStatFallbackListsParentWhenEnabled(t *testing.T) {
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Path == "/v1/fs/" && r.URL.RawQuery == "list=1" {
				listCalls.Add(1)
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

	opts := &MountOptions{LegacyDirStatFallback: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "dir", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Lookup mode = %o, want directory mode", out.Mode)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
}

func TestLookupUsesDirCachePositiveEntryWithoutRemoteStat(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	mtime := time.Unix(123, 0)
	fs.dirCache.Put("/", []CachedFileInfo{{
		Name:     "cached.txt",
		Size:     12,
		IsDir:    false,
		Mtime:    mtime,
		Revision: 7,
	}})

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "cached.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	if got, want := out.Size, uint64(12); got != want {
		t.Fatalf("Lookup size = %d, want %d", got, want)
	}
	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("lookup inode entry not found")
	}
	if entry.Revision != 7 {
		t.Fatalf("inode revision = %d, want 7", entry.Revision)
	}
}

func TestLookupRecognizesRemoteSymlinkMode(t *testing.T) {
	target := []byte("../target")
	symlinkMode := uint32(syscall.S_IFLNK) | 0o777
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(target)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(symlinkMode), 10))
			w.Header().Set("X-Dat9-Revision", "11")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(target)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "link", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := out.Mode & uint32(syscall.S_IFMT); got != uint32(syscall.S_IFLNK) {
		t.Fatalf("Lookup mode type = %o, want symlink", got)
	}
	if got := out.Size; got != uint64(len(target)) {
		t.Fatalf("Lookup size = %d, want %d", got, len(target))
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}

	got, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK {
		t.Fatalf("Readlink status = %v, want OK", st)
	}
	if string(got) != string(target) {
		t.Fatalf("Readlink target = %q, want %q", got, target)
	}
}

func TestSetAttrSymlinkTimesSurviveGetAttr(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			headCalls.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/link", false, 6, time.Unix(100, 0))
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFLNK)|0o777)
	fs.inodes.UpdateRevision(ino, 11)

	atime := time.Unix(1960000000, 0)
	mtime := time.Unix(1970000000, 0)
	var setOut gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_ATIME | gofuse.FATTR_MTIME,
			Atime:    uint64(atime.Unix()),
			Mtime:    uint64(mtime.Unix()),
		},
	}, &setOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if setOut.Mtime != uint64(mtime.Unix()) {
		t.Fatalf("SetAttr mtime = %d, want %d", setOut.Mtime, mtime.Unix())
	}

	var out gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
	if out.Atime != uint64(atime.Unix()) {
		t.Fatalf("GetAttr atime = %d, want %d", out.Atime, atime.Unix())
	}
	if out.Mtime != uint64(mtime.Unix()) {
		t.Fatalf("GetAttr mtime = %d, want %d", out.Mtime, mtime.Unix())
	}
}

func TestReadlinkRetriesTransientRead(t *testing.T) {
	target := []byte("../target")
	symlinkMode := uint32(syscall.S_IFLNK) | 0o777
	var getCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(target)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(symlinkMode), 10))
			w.Header().Set("X-Dat9-Revision", "11")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if getCalls.Add(1) == 1 {
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(target)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "link", &out)
	if st != gofuse.OK {
		t.Errorf("Lookup status = %v, want OK", st)
		return
	}

	got, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK {
		t.Errorf("Readlink status = %v, want OK", st)
		return
	}
	if string(got) != string(target) {
		t.Errorf("Readlink target = %q, want %q", got, target)
		return
	}
	if calls := getCalls.Load(); calls < 2 {
		t.Errorf("GET calls = %d, want retry", calls)
		return
	}
}

func TestSymlinkCreatesRemoteLinkAndCachesEntry(t *testing.T) {
	const target = "../target"
	symlinkMode := uint32(syscall.S_IFLNK) | 0o777
	var gotTarget string
	var postCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			if r.URL.Path != "/v1/fs/link" {
				t.Errorf("POST path = %s, want /v1/fs/link", r.URL.Path)
			}
			if got := r.URL.Query().Get("symlink"); got != "1" {
				t.Errorf("symlink query = %q, want 1", got)
			}
			var req struct {
				Target string `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode body: %v", err)
			}
			gotTarget = req.Target
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(target)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(symlinkMode), 10))
			w.Header().Set("X-Dat9-Revision", "12")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write([]byte(target))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, target, "link", &out)
	if st != gofuse.OK {
		t.Fatalf("Symlink status = %v, want OK", st)
	}
	if gotTarget != target {
		t.Fatalf("posted target = %q, want %q", gotTarget, target)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1", got)
	}
	if got := out.Mode & uint32(syscall.S_IFMT); got != uint32(syscall.S_IFLNK) {
		t.Fatalf("Symlink mode type = %o, want symlink", got)
	}
	if got := out.Size; got != uint64(len(target)) {
		t.Fatalf("Symlink size = %d, want %d", got, len(target))
	}

	got, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK {
		t.Fatalf("Readlink status = %v, want OK", st)
	}
	if string(got) != target {
		t.Fatalf("Readlink target = %q, want %q", got, target)
	}
}

func TestSymlinkNegativeCachedConflictDoesNotDeleteRemoteTarget(t *testing.T) {
	const target = "test"
	var postCalls atomic.Int32
	var deleteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/link":
			if got := r.URL.Query().Get("symlink"); got != "1" {
				t.Errorf("symlink query = %q, want 1", got)
			}
			postCalls.Add(1)
			http.Error(w, "exists", http.StatusConflict)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/link":
			w.Header().Set("X-Dat9-IsDir", "true")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/link":
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/link":
			_, _ = w.Write([]byte(target))
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	fs.cacheNegativePath("/link")

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, target, "link", &out)
	if st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("Symlink status = %v, want EEXIST", st)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1", got)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("DELETE calls = %d, want 0", got)
	}
}

func TestLayerSymlinkWritesLayerEntry(t *testing.T) {
	const target = "base.txt"
	var got client.FSLayerEntryRequest
	var layerPosts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
			layerPosts.Add(1)
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:     "layer-1",
				Path:        got.Path,
				Op:          got.Op,
				Kind:        got.Kind,
				ContentText: got.ContentText,
				Mode:        got.Mode,
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			t.Errorf("layer symlink used base fs symlink endpoint: %s", r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/",
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, target, "link", &out)
	if st != gofuse.OK {
		t.Fatalf("Symlink status = %v, want OK", st)
	}
	if layerPosts.Load() != 1 {
		t.Fatalf("layer POST calls = %d, want 1", layerPosts.Load())
	}
	if got.Path != "/link" || got.Op != "symlink" || got.Kind != "symlink" || got.ContentText != target {
		t.Fatalf("layer symlink request = %+v, want symlink entry", got)
	}
	if got.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("layer symlink mode type = %#o, want S_IFLNK", got.Mode&uint32(syscall.S_IFMT))
	}
	readTarget, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK || string(readTarget) != target {
		t.Fatalf("Readlink = (%q, %v), want %q OK", readTarget, st, target)
	}
}

func TestLinkCreatesRemoteHardlinkAndCachesAlias(t *testing.T) {
	var gotSource string
	var postCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			if r.URL.Path != "/v1/fs/dst.txt" {
				t.Errorf("POST path = %s, want /v1/fs/dst.txt", r.URL.Path)
			}
			if got := r.URL.Query().Get("hardlink"); got != "1" {
				t.Errorf("hardlink query = %q, want 1", got)
			}
			gotSource = r.Header.Get("X-Dat9-Hardlink-Source")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case http.MethodHead:
			if r.URL.Path != "/v1/fs/dst.txt" {
				t.Errorf("HEAD path = %s, want /v1/fs/dst.txt", r.URL.Path)
			}
			w.Header().Set("Content-Length", "6")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, time.Now())

	var out gofuse.EntryOut
	st := fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	if gotSource != "/src.txt" {
		t.Fatalf("posted source = %q, want /src.txt", gotSource)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1", got)
	}
	if out.NodeId != srcIno {
		t.Fatalf("linked node id = %d, want source inode %d", out.NodeId, srcIno)
	}
	if out.Nlink != 2 {
		t.Fatalf("entry nlink = %d, want 2", out.Nlink)
	}
	if dstIno, ok := fs.inodes.GetInode("/dst.txt"); !ok || dstIno != srcIno {
		t.Fatalf("dst inode = %d/%v, want %d/true", dstIno, ok, srcIno)
	}
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing")
	}
	if entry.Path != "/dst.txt" {
		t.Fatalf("primary path = %q, want /dst.txt", entry.Path)
	}
	if _, ok := entry.Paths["/src.txt"]; !ok {
		t.Fatalf("entry paths missing src alias: %+v", entry.Paths)
	}
	if _, ok := entry.Paths["/dst.txt"]; !ok {
		t.Fatalf("entry paths missing dst alias: %+v", entry.Paths)
	}
	if cached := fs.dirCache.Lookup("/", "dst.txt"); cached.kind != namespaceLookupPositive || cached.item.Nlink != 2 {
		t.Fatalf("cached dst = %+v, want positive nlink 2", cached)
	}
}

func TestLinkRecoversCommittedHardlinkAfterTransientError(t *testing.T) {
	var postCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			w.WriteHeader(statusClientClosedRequest)
		case http.MethodHead:
			if r.URL.Path != "/v1/fs/src.txt" && r.URL.Path != "/v1/fs/dst.txt" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", "6")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, time.Now())

	var out gofuse.EntryOut
	st := fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1 recovery stat without retry", got)
	}
	if out.NodeId != srcIno || out.Nlink != 2 {
		t.Fatalf("entry out = node %d nlink %d, want node %d nlink 2", out.NodeId, out.Nlink, srcIno)
	}
}

func TestLinkKeepsDestinationPathWhenDestinationStatForbidden(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, time.Now())

	var out gofuse.EntryOut
	st := fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	if out.NodeId != srcIno || out.Nlink != 2 {
		t.Fatalf("entry out = node %d nlink %d, want node %d nlink 2", out.NodeId, out.Nlink, srcIno)
	}
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing")
	}
	if entry.Path != "/dst.txt" {
		t.Fatalf("primary path = %q, want /dst.txt", entry.Path)
	}
	if entry.ResourceID != "file-1" {
		t.Fatalf("resource id = %q, want file-1", entry.ResourceID)
	}
}

func TestLinkUpdatesSourceCtime(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", "6")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	oldTime := time.Now().Add(-time.Hour)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, oldTime)
	fs.inodes.UpdateCtime(srcIno, oldTime)

	var out gofuse.EntryOut
	st := fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing")
	}
	if !entry.Ctime.After(oldTime) {
		t.Fatalf("source ctime = %v, want after %v", entry.Ctime, oldTime)
	}
}

func TestLinkSyncsDirtyOpenSourceAndRejectsOpenDestination(t *testing.T) {
	var putCalls atomic.Int32
	var postCalls atomic.Int32
	var putBody string
	var putMu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			putMu.Lock()
			putBody = string(body)
			putMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		case http.MethodPost:
			postCalls.Add(1)
			if got := r.Header.Get("X-Dat9-Hardlink-Source"); got != "/src.txt" {
				t.Errorf("hardlink source header = %q, want /src.txt", got)
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.Lookup("/src.txt", false, 0, time.Now())
	dirty := fs.newWriteBuffer("/src.txt", 1024, 0)
	if _, err := dirty.Write(0, []byte("dirty")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{Ino: srcIno, Path: "/src.txt", Dirty: dirty, IsNew: true}
	fh.DirtySeq = fs.markDirtySize(srcIno, dirty.Size())
	fs.openHandles.Add(fh)

	var out gofuse.EntryOut
	st := fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("dirty source Link status = %v, want OK", st)
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1 source sync before hardlink", got)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1 hardlink", got)
	}
	putMu.Lock()
	gotBody := putBody
	putMu.Unlock()
	if gotBody != "dirty" {
		t.Fatalf("synced body = %q, want dirty", gotBody)
	}
	fh.Lock()
	if fh.IsNew || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		t.Fatalf("source handle not clean after hardlink sync: isNew=%t dirtySeq=%d dirty=%t", fh.IsNew, fh.DirtySeq, fh.Dirty.HasDirtyParts())
	}
	fh.Unlock()
	fs.openHandles.Remove(fh)

	dstIno := fs.inodes.Lookup("/dst.txt", false, 0, time.Now())
	dstHandle := &FileHandle{Ino: dstIno, Path: "/dst.txt"}
	fs.openHandles.Add(dstHandle)
	st = fs.Link(nil, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("open destination Link status = %v, want EEXIST", st)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want no extra hardlink while destination is open", got)
	}
}

func TestLookupFromDirCachePreservesHardlinkIdentity(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 2, false, 6, time.Now())
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:       "dst.txt",
		Size:       6,
		IsDir:      false,
		Mtime:      time.Now(),
		ResourceID: "file-1",
		Nlink:      2,
	})

	var out gofuse.EntryOut
	handled, st := fs.lookupFromDirCache("/", "/dst.txt", "dst.txt", &out)
	if !handled || st != gofuse.OK {
		t.Fatalf("lookupFromDirCache handled/status = %v/%v, want true/OK", handled, st)
	}
	if out.NodeId != srcIno {
		t.Fatalf("cached hardlink node = %d, want source inode %d", out.NodeId, srcIno)
	}
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing")
	}
	if _, ok := entry.Paths["/dst.txt"]; !ok {
		t.Fatalf("entry paths missing cached dst alias: %+v", entry.Paths)
	}
}

func TestSymlinkRecoversWhenInterruptedAfterRemoteCreate(t *testing.T) {
	const target = "../target"
	symlinkMode := uint32(syscall.S_IFLNK) | 0o777
	var postCalls atomic.Int32
	var headCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/link" && r.URL.RawQuery == "symlink=1":
			postCalls.Add(1)
			w.WriteHeader(statusClientClosedRequest)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/link":
			headCalls.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(target)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(symlinkMode), 10))
			w.Header().Set("X-Dat9-Revision", "12")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, target, "link", &out)
	if st != gofuse.OK {
		t.Errorf("Symlink status = %v, want OK", st)
		return
	}
	if got := postCalls.Load(); got != 1 {
		t.Errorf("POST calls = %d, want 1", got)
		return
	}
	if got := headCalls.Load(); got == 0 {
		t.Error("symlink retry did not probe created path")
		return
	}
	if got := out.Mode & uint32(syscall.S_IFMT); got != uint32(syscall.S_IFLNK) {
		t.Errorf("Symlink mode type = %o, want symlink", got)
		return
	}
}

func TestSymlinkRecoveryRejectsNonSymlinkProbe(t *testing.T) {
	const target = "../target"
	var postCalls atomic.Int32
	var headCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/link" && r.URL.RawQuery == "symlink=1":
			postCalls.Add(1)
			w.WriteHeader(statusClientClosedRequest)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/link":
			headCalls.Add(1)
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(uint32(syscall.S_IFREG)|0o644), 10))
			w.Header().Set("X-Dat9-Revision", "12")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, target, "link", &out)
	if st != gofuse.Status(syscall.EAGAIN) {
		t.Errorf("Symlink status = %v, want EAGAIN", st)
		return
	}
	if got := postCalls.Load(); got != 1 {
		t.Errorf("POST calls = %d, want 1", got)
		return
	}
	if got := headCalls.Load(); got == 0 {
		t.Error("symlink recovery did not stat created path")
		return
	}
}

func TestLookupUsesDirCacheNegativeEntryWithoutRemoteStat(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "other.txt", Size: 1}})

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "missing.txt", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	if out.NodeId != 0 {
		t.Fatalf("negative lookup NodeId = %d, want 0", out.NodeId)
	}
}

func TestLookupPartialNamespaceMissFallsThroughToRemoteStat(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "9")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.dirCache.Upsert("/", CachedFileInfo{Name: "known.txt", Size: 1})

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "remote.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got, want := out.Size, uint64(9); got != want {
		t.Fatalf("Lookup size = %d, want %d", got, want)
	}
}

func TestLookupStatNotFoundSeedsShortNegativeNamespaceCache(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	for range 2 {
		var out gofuse.EntryOut
		st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "missing.txt", &out)
		if st != gofuse.ENOENT {
			t.Fatalf("Lookup status = %v, want ENOENT", st)
		}
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
}

func TestLookupLockFileStatNotFoundDoesNotSeedNamespaceNegative(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	for range 2 {
		var out gofuse.EntryOut
		st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config.lock", &out)
		if st != gofuse.ENOENT {
			t.Fatalf("Lookup status = %v, want ENOENT", st)
		}
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("HEAD calls = %d, want 2", got)
	}
}

func TestLookupLockFileIgnoresCompleteNamespaceMiss(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "config"}})

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config.lock", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
}

func TestLookupLockFileIgnoresPositiveDirCache(t *testing.T) {
	var handlerErrors testErrorRecorder
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.dirCache.Put("/repo/.git", []CachedFileInfo{{
		Name:     "config.lock",
		Size:     251,
		Revision: 7,
	}})
	gitIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: gitIno}, "config.lock", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	handlerErrors.Check(t)
}

func TestGetAttrLockFileIgnoresPositiveDirCache(t *testing.T) {
	var handlerErrors testErrorRecorder
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handlerErrors.Recordf("method = %s, want HEAD", r.Method)
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	fs := NewDat9FS(newTestClient(ts.URL), trustedProcessLocalEventsOptions())
	fs.dirCache.Put("/repo/.git", []CachedFileInfo{{
		Name:     "config.lock",
		Size:     251,
		Revision: 7,
	}})
	lockIno := fs.inodes.Lookup("/repo/.git/config.lock", false, 251, time.Now())
	fs.inodes.UpdateRevision(lockIno, 7)

	var out gofuse.AttrOut
	st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: lockIno},
	}, &out)
	if st != gofuse.ENOENT {
		t.Fatalf("GetAttr status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	handlerErrors.Check(t)
}

func TestLookupSessionCreatedDirMissAvoidsRemoteStat(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && (r.URL.RawQuery == "mkdir" || r.URL.RawQuery == "mkdir&mode=0"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "unexpected remote stat", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var mkdirOut gofuse.EntryOut
	st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "dir", &mkdirOut)
	if st != gofuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", st)
	}

	var lookupOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: mkdirOut.NodeId}, "missing.txt", &lookupOut)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HEAD calls = %d, want 0", got)
	}
}

func TestLookupSSEForeignCreateInvalidatesSessionCreatedMiss(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "4")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	fs.dirCache.MarkSessionCreatedDir("/dir")

	w := &SSEWatcher{fs: fs, actor: "mine"}
	w.handleChange(&client.ChangeEvent{
		Seq:   1,
		Path:  "/dir/remote.txt",
		Op:    "write",
		Actor: "other",
	})

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "remote.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
}

func TestLookupNegativeStormEscalatesToSingleList(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"entries":[{"name":"existing.txt","size":4}]}`))
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	dirIno := fs.inodes.Lookup("/bench", true, 0, time.Now())

	for i := 0; i < 10; i++ {
		var out gofuse.EntryOut
		st := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, fmt.Sprintf("file.%d", i), &out)
		if st != gofuse.ENOENT {
			t.Fatalf("Lookup file.%d status = %v, want ENOENT", i, st)
		}
	}

	// Misses 1-3 each pay a HEAD; the third triggers the listing, which runs
	// synchronously inside that Lookup (the in-process test server makes this
	// deterministic), so misses 4-10 are answered locally from the complete
	// listing: exactly threshold HEADs and one LIST.
	if got := headCalls.Load(); got != int32(escalateMissThreshold) {
		t.Fatalf("HEAD calls = %d, want %d (storm should escalate to a listing)", got, escalateMissThreshold)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1", got)
	}

	// The listing also serves positive lookups without another remote stat.
	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "existing.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup existing.txt status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != int32(escalateMissThreshold) {
		t.Fatalf("HEAD calls after positive lookup = %d, want %d", got, escalateMissThreshold)
	}
}

func TestLookupNegativeStormOversizedListingCoolsDown(t *testing.T) {
	var headCalls, listCalls atomic.Int32
	entries := make([]string, 0, defaultNamespaceCacheMaxEntries+1)
	for i := 0; i <= defaultNamespaceCacheMaxEntries; i++ {
		entries = append(entries, fmt.Sprintf(`{"name":"e%d","size":1}`, i))
	}
	listBody := `{"entries":[` + strings.Join(entries, ",") + `]}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			listCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(listBody))
		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	dirIno := fs.inodes.Lookup("/huge", true, 0, time.Now())

	for i := 0; i < 10; i++ {
		var out gofuse.EntryOut
		st := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, fmt.Sprintf("file.%d", i), &out)
		if st != gofuse.ENOENT {
			t.Fatalf("Lookup file.%d status = %v, want ENOENT", i, st)
		}
	}

	// Oversized listings cannot answer misses; every probe stays remote, but
	// the cooldown must prevent more than the single listing attempt.
	// Misses 1-3 pay HEADs and trigger the listing; it exceeds the cache
	// limit, so DeferEscalation kicks in and misses 4-10 fall back to one
	// HEAD each: all 10 probes stay remote but only one LIST is ever sent.
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1 (cooldown should stop repeat listings)", got)
	}
	if got := headCalls.Load(); got != 10 {
		t.Fatalf("HEAD calls = %d, want 10", got)
	}
}

// TestReadDirEmitsDotAndDotDot verifies that readdir on a Drive9 directory
// yields the POSIX "." and ".." entries before the real children (LTP
// getdents01 asserts their presence). The entries must carry S_IFDIR and
// reference the directory itself / its parent, and must not be repeated on
// later readdir pages (offset > 0).
func TestReadDirEmitsDotAndDotDot(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	dirIno := fs.inodes.Lookup("/parent/dir", true, 0, time.Now())
	parentIno := fs.inodes.Lookup("/parent/", true, 0, time.Now())

	dh := &DirHandle{
		Ino:  dirIno,
		Path: "/parent/dir",
		Entries: []DirEntry{{
			Name: "file.txt",
			Ino:  fs.inodes.Lookup("/parent/dir/file.txt", false, 9, time.Now()),
			Mode: uint32(syscall.S_IFREG) | 0o644,
		}},
	}
	fh := fs.dirHandles.Allocate(dh)

	// First page (offset 0) must contain ".", "..", then "file.txt".
	buf := make([]byte, 4096)
	out := gofuse.NewDirEntryList(buf, 0)
	if st := fs.ReadDir(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, out); st != gofuse.OK {
		t.Fatalf("ReadDir status = %v, want OK", st)
	}
	names := parseDirEntryNames(t, out)
	if len(names) != 3 || names[0] != "." || names[1] != ".." || names[2] != "file.txt" {
		t.Fatalf("readdir first page = %v, want [. .. file.txt]", names)
	}

	// A second call with offset past the synthetic entries must not repeat them.
	out2 := gofuse.NewDirEntryList(make([]byte, 4096), 1)
	if st := fs.ReadDir(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Offset:   1,
		Size:     4096,
	}, out2); st != gofuse.OK {
		t.Fatalf("ReadDir page 2 status = %v, want OK", st)
	}
	page2 := parseDirEntryNames(t, out2)
	for _, n := range page2 {
		if n == "." || n == ".." {
			t.Fatalf("readdir page 2 repeated synthetic entry %q; got %v", n, page2)
		}
	}

	// dotDotEntries must reference the directory itself and its parent.
	dots := fs.dotDotEntries(dh)
	if len(dots) != 2 || dots[0].Name != "." || dots[1].Name != ".." {
		t.Fatalf("dotDotEntries = %v, want [. ..]", dots)
	}
	if dots[0].Ino != dirIno {
		t.Fatalf("dotDotEntries '.' ino = %d, want dir ino %d", dots[0].Ino, dirIno)
	}
	if dots[1].Ino != parentIno {
		t.Fatalf("dotDotEntries '..' ino = %d, want parent ino %d", dots[1].Ino, parentIno)
	}
	for _, e := range dots {
		if e.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
			t.Fatalf("dotDotEntries %q mode = %o, want S_IFDIR", e.Name, e.Mode)
		}
	}
}

// TestReadDirEmitsDotAndDotDotAtRoot verifies ".." at the root resolves to the
// root inode itself rather than a missing parent.
func TestReadDirEmitsDotAndDotDotAtRoot(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	rootIno := fs.inodes.Lookup("/", true, 0, time.Now())
	dh := &DirHandle{Ino: rootIno, Path: "/", Entries: nil}
	// Populate Entries directly to avoid a remote listDir round-trip.
	dh.Entries = []DirEntry{{
		Name: "child",
		Ino:  fs.inodes.Lookup("/child", false, 0, time.Now()),
		Mode: uint32(syscall.S_IFREG) | 0o644,
	}}
	fh := fs.dirHandles.Allocate(dh)

	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	if st := fs.ReadDir(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: rootIno},
		Fh:       fh,
		Offset:   0,
		Size:     4096,
	}, out); st != gofuse.OK {
		t.Fatalf("ReadDir status = %v, want OK", st)
	}
	names := parseDirEntryNames(t, out)
	if len(names) != 3 || names[0] != "." || names[1] != ".." || names[2] != "child" {
		t.Fatalf("root readdir first page = %v, want [. .. child]", names)
	}
	dots := fs.dotDotEntries(dh)
	if dots[1].Ino != rootIno {
		t.Fatalf("root '..' ino = %d, want root ino %d (root is its own parent)", dots[1].Ino, rootIno)
	}
}

func parseDirEntryNames(t *testing.T, out *gofuse.DirEntryList) []string {
	t.Helper()
	// DirEntryList.bytes() is unexported in go-fuse; read the underlying buf
	// via reflect so the test stays in package fuse without a go-fuse patch.
	v := reflect.ValueOf(out).Elem()
	buf := v.FieldByName("buf").Bytes()
	var names []string
	// Parse the FUSE _Dirent wire layout (Ino u64, Off u64, NameLen u32,
	// Typ u32 = 24-byte header, then name padded to 8 bytes). go-fuse's
	// DirEntry.Parse is platform-dependent (darwin uses Namlen), so decode
	// the wire format directly here.
	const headerSize = 24
	for len(buf) >= headerSize {
		nameLen := nativeEndian.Uint32(buf[16:20])
		_ = nativeEndian.Uint32(buf[20:24]) // Typ
		// Record length: header + name padded to 8 bytes.
		recLen := int(headerSize+nameLen+7) &^ 7
		if recLen > len(buf) {
			t.Fatalf("dirent recLen=%d exceeds buf len %d (nameLen=%d)", recLen, len(buf), nameLen)
		}
		nameBytes := buf[headerSize : headerSize+nameLen]
		names = append(names, string(nameBytes))
		buf = buf[recLen:]
	}
	return names
}

func TestReadDirPlusRecreatesStaleSnapshotInode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	mtime := time.Now().Add(-time.Minute)
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	fileIno := fs.inodes.Lookup("/dir/file.txt", false, 1, time.Now())
	fs.inodes.Forget(fileIno, 1)
	if _, ok := fs.inodes.GetEntry(fileIno); ok {
		t.Fatalf("stale file inode %d is still mapped", fileIno)
	}
	fs.dirCache.Upsert("/dir", CachedFileInfo{
		Name:     "file.txt",
		Size:     9,
		Mtime:    mtime,
		Revision: 12,
	})

	dh := &DirHandle{
		Ino:  dirIno,
		Path: "/dir",
		Entries: []DirEntry{{
			Name: "file.txt",
			Ino:  fileIno,
			Mode: uint32(syscall.S_IFREG) | 0o644,
		}},
	}
	fh := fs.dirHandles.Allocate(dh)
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	st := fs.ReadDirPlus(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Size:     4096,
	}, out)
	if st != gofuse.OK {
		t.Fatalf("ReadDirPlus status = %v, want OK", st)
	}

	newIno, ok := fs.inodes.GetInode("/dir/file.txt")
	if !ok {
		t.Fatal("file path was not remapped")
	}
	if newIno == fileIno {
		t.Fatalf("file inode = %d, want a replacement for stale inode", newIno)
	}
	if dh.Entries[0].Ino != newIno {
		t.Fatalf("snapshot inode = %d, want remapped inode %d", dh.Entries[0].Ino, newIno)
	}
	entry, ok := fs.inodes.GetEntry(newIno)
	if !ok {
		t.Fatalf("replacement inode %d is not mapped", newIno)
	}
	if entry.Nlookup != 1 {
		t.Fatalf("replacement inode lookup refs = %d, want 1", entry.Nlookup)
	}
	if entry.Size != 9 {
		t.Fatalf("replacement inode size = %d, want cached size 9", entry.Size)
	}
	if entry.Revision != 12 {
		t.Fatalf("replacement inode revision = %d, want cached revision 12", entry.Revision)
	}
}

func TestReadDirPlusStaleSnapshotDoesNotZeroLiveInode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	staleIno := fs.inodes.Lookup("/dir/file.txt", false, 1, time.Now())
	fs.inodes.Forget(staleIno, 1)
	if _, ok := fs.inodes.GetEntry(staleIno); ok {
		t.Fatalf("stale file inode %d is still mapped", staleIno)
	}
	liveIno := fs.inodes.Lookup("/dir/file.txt", false, 17, time.Now())

	dh := &DirHandle{
		Ino:  dirIno,
		Path: "/dir",
		Entries: []DirEntry{{
			Name: "file.txt",
			Ino:  staleIno,
			Mode: uint32(syscall.S_IFREG) | 0o644,
		}},
	}
	fh := fs.dirHandles.Allocate(dh)
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	st := fs.ReadDirPlus(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Size:     4096,
	}, out)
	if st != gofuse.OK {
		t.Fatalf("ReadDirPlus status = %v, want OK", st)
	}

	if dh.Entries[0].Ino != liveIno {
		t.Fatalf("snapshot inode = %d, want existing live inode %d", dh.Entries[0].Ino, liveIno)
	}
	entry, ok := fs.inodes.GetEntry(liveIno)
	if !ok {
		t.Fatalf("live inode %d is not mapped", liveIno)
	}
	if entry.Size != 17 {
		t.Fatalf("live inode size = %d, want preserved size 17", entry.Size)
	}
}

func TestReadDirPlusStaleSnapshotUsesStoredEntryMetadataAfterDirCacheInvalidated(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	mtime := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	entries := fs.cachedToDirEntries("/dir", []CachedFileInfo{{
		Name:     "file.txt",
		Size:     23,
		Mtime:    mtime,
		Revision: 44,
		Mode:     0o600,
		HasMode:  true,
	}})
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	staleIno := entries[0].Ino
	fs.inodes.Forget(staleIno, 1)
	if _, ok := fs.inodes.GetEntry(staleIno); ok {
		t.Fatalf("stale file inode %d is still mapped", staleIno)
	}
	fs.dirCache.Invalidate("/dir")

	dh := &DirHandle{
		Ino:     dirIno,
		Path:    "/dir",
		Entries: entries,
	}
	fh := fs.dirHandles.Allocate(dh)
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	st := fs.ReadDirPlus(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Size:     4096,
	}, out)
	if st != gofuse.OK {
		t.Fatalf("ReadDirPlus status = %v, want OK", st)
	}

	newIno, ok := fs.inodes.GetInode("/dir/file.txt")
	if !ok {
		t.Fatal("file path was not remapped")
	}
	if newIno == staleIno {
		t.Fatalf("file inode = %d, want a replacement for stale inode", newIno)
	}
	entry, ok := fs.inodes.GetEntry(newIno)
	if !ok {
		t.Fatalf("replacement inode %d is not mapped", newIno)
	}
	if entry.Size != 23 {
		t.Fatalf("replacement inode size = %d, want stored size 23", entry.Size)
	}
	if entry.Revision != 44 {
		t.Fatalf("replacement inode revision = %d, want stored revision 44", entry.Revision)
	}
	if !entry.HasMode || entry.Mode != 0o600 {
		t.Fatalf("replacement inode mode = %o has=%t, want 0600 true", entry.Mode, entry.HasMode)
	}
	if !entry.Mtime.Equal(mtime) {
		t.Fatalf("replacement inode mtime = %s, want %s", entry.Mtime, mtime)
	}
}

func TestRemoteListSkipsInvalidDirEntryNames(t *testing.T) {
	items := []client.FileInfo{
		{Name: "/"},
		{Name: ""},
		{Name: "."},
		{Name: ".."},
		{Name: "bad/name"},
		{Name: "ok.txt"},
	}
	cached := cachedFileInfos(items)
	if len(cached) != 1 || cached[0].Name != "ok.txt" {
		t.Fatalf("cachedFileInfos = %+v, want only ok.txt", cached)
	}

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	entries := fs.cachedToDirEntries("/", []CachedFileInfo{
		{Name: "/"},
		{Name: "bad/name"},
		{Name: "ok.txt"},
	})
	if len(entries) != 1 || entries[0].Name != "ok.txt" {
		t.Fatalf("cachedToDirEntries = %+v, want only ok.txt", entries)
	}
}

func TestLookupSSEForeignDeleteInvalidatesPositiveNamespaceHit(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/dir", true, 0, time.Now())
	fs.dirCache.Upsert("/dir", CachedFileInfo{Name: "stale.txt", Size: 9})

	w := &SSEWatcher{fs: fs, actor: "mine"}
	w.handleChange(&client.ChangeEvent{
		Seq:   1,
		Path:  "/dir/stale.txt",
		Op:    "delete",
		Actor: "other",
	})

	dirIno, _ := fs.inodes.GetInode("/dir")
	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "stale.txt", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
}

func TestNamespaceCacheCrossDirRenameUpdatesBothParents(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	srcIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	dstIno := fs.inodes.Lookup("/dst", true, 0, time.Now())
	fs.inodes.Lookup("/src/file.txt", false, 5, time.Unix(10, 0))
	fs.dirCache.Put("/src", []CachedFileInfo{{Name: "file.txt", Size: 5}})
	fs.dirCache.Put("/dst", []CachedFileInfo{{Name: "other.txt", Size: 1}})

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: srcIno},
		Newdir:   dstIno,
	}, "/src/file.txt", "/dst/file.txt")

	var oldOut gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: srcIno}, "file.txt", &oldOut)
	if st != gofuse.ENOENT {
		t.Fatalf("old parent Lookup status = %v, want ENOENT", st)
	}

	var newOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: dstIno}, "file.txt", &newOut)
	if st != gofuse.OK {
		t.Fatalf("new parent Lookup status = %v, want OK", st)
	}
	if got, want := newOut.Size, uint64(5); got != want {
		t.Fatalf("renamed entry size = %d, want %d", got, want)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestLookupLegacyListFallbackPopulatesDirCacheForLaterMisses(t *testing.T) {
	var headCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case http.MethodGet:
			if r.URL.Path == "/v1/fs/" && r.URL.Query().Get("list") == "1" {
				listCalls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []map[string]any{{
						"name":  "listed.txt",
						"isDir": false,
						"size":  3,
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

	opts := &MountOptions{LegacyDirStatFallback: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var first gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "first-missing.txt", &first)
	if st != gofuse.ENOENT {
		t.Fatalf("first Lookup status = %v, want ENOENT", st)
	}

	var second gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "second-missing.txt", &second)
	if st != gofuse.ENOENT {
		t.Fatalf("second Lookup status = %v, want ENOENT", st)
	}

	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
}

func TestLookupUsesRemoteRootMapping(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "12")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "5")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{RemoteRoot: "/remote"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "file.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if gotPath != "/v1/fs/remote/file.txt" {
		t.Fatalf("lookup API path = %q, want /v1/fs/remote/file.txt", gotPath)
	}
	if _, ok := fs.inodes.GetInode("/file.txt"); !ok {
		t.Fatal("lookup should keep local inode path rebased to /file.txt")
	}
}

func TestListDirUsesRemoteRootMapping(t *testing.T) {
	var gotListPath string
	var gotListQuery string
	var gotBatchPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			gotListPath = r.URL.Path
			gotListQuery = r.URL.RawQuery
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "nested.txt",
					"isDir": false,
					"size":  7,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			gotBatchPath = r.URL.Path
			var req struct {
				Paths []string `json:"paths"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode batch request: %v", err)
			}
			if got, want := strings.Join(req.Paths, ","), "/remote/subdir/nested.txt"; got != want {
				t.Fatalf("batch stat paths = %q, want %q", got, want)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":     "/remote/subdir/nested.txt",
					"status":   200,
					"isDir":    false,
					"size":     7,
					"revision": 3,
				}},
			})
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{RemoteRoot: "/remote"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	entries, err := fs.listDir(context.Background(), "/subdir")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if gotListPath != "/v1/fs/remote/subdir" || gotListQuery != "list=1" {
		t.Fatalf("listDir request = %q?%s, want /v1/fs/remote/subdir?list=1", gotListPath, gotListQuery)
	}
	if gotBatchPath != "/v1/fs:batch-stat" {
		t.Fatalf("batch stat path = %q, want /v1/fs:batch-stat", gotBatchPath)
	}
	if len(entries) != 1 || entries[0].Name != "nested.txt" {
		t.Fatalf("listDir entries = %+v, want nested.txt", entries)
	}
	entry, ok := fs.inodes.GetEntry(entries[0].Ino)
	if !ok {
		t.Fatal("listDir entry inode not found")
	}
	if entry.Revision != 3 {
		t.Fatalf("entry revision = %d, want 3 from batch stat", entry.Revision)
	}
}

func TestListDirIgnoresBatchStatPerPathFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "listed.txt",
					"isDir": false,
					"size":  9,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":   "/listed.txt",
					"status": 404,
					"error":  "not found",
				}},
			})
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	entries, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if len(entries) != 1 || entries[0].Name != "listed.txt" {
		t.Fatalf("listDir entries = %+v, want listed.txt despite batch 404", entries)
	}
	entry, ok := fs.inodes.GetEntry(entries[0].Ino)
	if !ok {
		t.Fatal("listDir entry inode not found")
	}
	if entry.Size != 9 || entry.Revision != 0 {
		t.Fatalf("entry = %+v, want list metadata preserved with no revision", entry)
	}
}

func TestListDirIgnoresBatchStatTransportFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "listed.txt",
					"isDir": false,
					"size":  9,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			http.Error(w, "batch stat unavailable", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	entries, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if len(entries) != 1 || entries[0].Name != "listed.txt" {
		t.Fatalf("listDir entries = %+v, want listed.txt despite batch transport failure", entries)
	}
	entry, ok := fs.inodes.GetEntry(entries[0].Ino)
	if !ok {
		t.Fatal("listDir entry inode not found")
	}
	if entry.Size != 9 || entry.Revision != 0 {
		t.Fatalf("entry = %+v, want list metadata preserved with no revision", entry)
	}
}

func TestListDirPrefetchesSmallFilesIntoReadCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "prefetch.txt",
					"isDir": false,
					"size":  5,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":     "/prefetch.txt",
					"status":   200,
					"isDir":    false,
					"size":     5,
					"revision": 7,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			var req struct {
				Paths    []string `json:"paths"`
				MaxBytes int64    `json:"max_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode batch read-small request: %v", err)
			}
			if got, want := strings.Join(req.Paths, ","), "/prefetch.txt"; got != want {
				t.Fatalf("batch read-small paths = %q, want %q", got, want)
			}
			if req.MaxBytes != 16 {
				t.Fatalf("batch read-small max_bytes = %d, want 16", req.MaxBytes)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":     "/prefetch.txt",
					"status":   200,
					"data":     []byte("hello"),
					"size":     5,
					"revision": 7,
				}},
			})
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{
		ReadDirPrefetch:      true,
		PrefetchMaxFiles:     8,
		PrefetchMaxFileBytes: 16,
		PrefetchMaxBytes:     64,
		PrefetchTimeout:      time.Second,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	entries, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if len(entries) != 1 || entries[0].Name != "prefetch.txt" {
		t.Fatalf("listDir entries = %+v, want prefetch.txt", entries)
	}
	data, ok := fs.readCache.Get("/prefetch.txt", 7)
	if !ok {
		t.Fatal("readCache miss after readdir prefetch")
	}
	if string(data) != "hello" {
		t.Fatalf("readCache data = %q, want hello", data)
	}
}

func TestListDirPrefetchIgnoresBatchReadTransportFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "listed.txt",
					"isDir": false,
					"size":  9,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":     "/listed.txt",
					"status":   200,
					"isDir":    false,
					"size":     9,
					"revision": 4,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			http.Error(w, "batch read unavailable", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{ReadDirPrefetch: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	entries, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if len(entries) != 1 || entries[0].Name != "listed.txt" {
		t.Fatalf("listDir entries = %+v, want listed.txt despite batch read transport failure", entries)
	}
	if _, ok := fs.readCache.Get("/listed.txt", 4); ok {
		t.Fatal("readCache hit after failed batch read-small, want miss")
	}
}

func TestListDirPrefetchSkipsPendingFile(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "dirty.txt",
					"isDir": false,
					"size":  6,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{{
					"path":     "/dirty.txt",
					"status":   200,
					"isDir":    false,
					"size":     6,
					"revision": 5,
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			t.Fatalf("batch read-small should not be called for pending local file")
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{ReadDirPrefetch: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/dirty.txt", 6, PendingOverwrite, 4); err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending

	entries, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if len(entries) != 1 || entries[0].Name != "dirty.txt" {
		t.Fatalf("listDir entries = %+v, want dirty.txt", entries)
	}
	if _, ok := fs.readCache.Get("/dirty.txt", 5); ok {
		t.Fatal("readCache hit for pending local file, want miss")
	}
}

func TestListDirPrefetchRespectsBudgets(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{
					{"name": "a.txt", "isDir": false, "size": 4},
					{"name": "b.txt", "isDir": false, "size": 5},
					{"name": "c.txt", "isDir": false, "size": 4},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{
					{"path": "/a.txt", "status": 200, "isDir": false, "size": 4, "revision": 1},
					{"path": "/b.txt", "status": 200, "isDir": false, "size": 5, "revision": 2},
					{"path": "/c.txt", "status": 200, "isDir": false, "size": 4, "revision": 3},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			var req struct {
				Paths []string `json:"paths"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode batch read-small request: %v", err)
			}
			if got, want := strings.Join(req.Paths, ","), "/a.txt,/b.txt"; got != want {
				t.Fatalf("batch read-small paths = %q, want %q", got, want)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []map[string]any{
					{"path": "/a.txt", "status": 200, "data": []byte("aaaa"), "size": 4, "revision": 1},
					{"path": "/b.txt", "status": 200, "data": []byte("bbbbb"), "size": 5, "revision": 2},
				},
			})
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{
		ReadDirPrefetch:      true,
		PrefetchMaxFiles:     2,
		PrefetchMaxFileBytes: 16,
		PrefetchMaxBytes:     9,
		PrefetchTimeout:      time.Second,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	_, err := fs.listDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if data, ok := fs.readCache.Get("/a.txt", 1); !ok || string(data) != "aaaa" {
		t.Fatalf("readCache a.txt = %q, %v; want aaaa hit", data, ok)
	}
	if data, ok := fs.readCache.Get("/b.txt", 2); !ok || string(data) != "bbbbb" {
		t.Fatalf("readCache b.txt = %q, %v; want bbbbb hit", data, ok)
	}
	if _, ok := fs.readCache.Get("/c.txt", 3); ok {
		t.Fatal("readCache c.txt hit despite max-files budget, want miss")
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	case <-time.After(time.Duration(lookupTransientRetryCount+1) * lookupTransientRetryTimeout * 2):
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

func TestHTTPToFuseStatus_MapsConflictToEEXIST(t *testing.T) {
	want := gofuse.Status(syscall.EEXIST)
	if got := httpToFuseStatus(&client.StatusError{StatusCode: http.StatusConflict, Message: "path conflict"}); got != want {
		t.Fatalf("status error 409 = %v, want %v", got, want)
	}
	if got := httpToFuseStatus(fmt.Errorf("HTTP 409: conflict")); got != want {
		t.Fatalf("string error 409 = %v, want %v", got, want)
	}
}

func TestHTTPToFuseStatus_PreservesRevisionConflictAsEIO(t *testing.T) {
	if got := httpToFuseStatus(&client.StatusError{StatusCode: http.StatusConflict, Message: "revision conflict"}); got != gofuse.EIO {
		t.Fatalf("revision conflict status = %v, want %v", got, gofuse.EIO)
	}
}

func TestIsCreateActionUnsupportedErr(t *testing.T) {
	if !isCreateActionUnsupportedErr(&client.StatusError{StatusCode: http.StatusBadRequest, Message: "unknown POST action"}) {
		t.Fatal("unknown POST action should be treated as unsupported create action")
	}
	if !isCreateActionUnsupportedErr(&client.StatusError{StatusCode: http.StatusNotFound, Message: ""}) {
		t.Fatal("plain 404 should be treated as unsupported create action")
	}
	if isCreateActionUnsupportedErr(&client.StatusError{StatusCode: http.StatusBadRequest, Message: "invalid path"}) {
		t.Fatal("plain bad request should not be treated as unsupported create action")
	}
}

func TestDeleteRemotePathRejectsInvalidKind(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	err := fs.deleteRemotePathWithInterruptRecovery(context.Background(), "/file.txt", deleteKind(""))
	if err == nil {
		t.Fatal("deleteRemotePathWithInterruptRecovery error = nil, want invalid kind error")
	}
	if !strings.Contains(err.Error(), "unsupported delete kind") {
		t.Fatalf("error = %v, want unsupported delete kind", err)
	}
}

// TestHTTPToFuseStatus_MapsClientClosedRequestToEAGAIN locks the contract
// between the server's tenantAuthMiddleware (which writes 499 when a request
// is canceled mid-auth) and the FUSE client. Without this mapping, a canceled
// read/lookup would surface as EIO instead of going through the existing
// retryable EAGAIN path that other transient/canceled errors use.
func TestHTTPToFuseStatus_MapsClientClosedRequestToEAGAIN(t *testing.T) {
	want := gofuse.Status(syscall.EAGAIN)
	if got := httpToFuseStatus(&client.StatusError{StatusCode: 499, Message: ""}); got != want {
		t.Fatalf("status error 499 = %v, want %v", got, want)
	}
	if got := httpToFuseStatus(fmt.Errorf("HTTP 499: client closed request")); got != want {
		t.Fatalf("string error 499 = %v, want %v", got, want)
	}
}

// TestIsTransientLookupErr_Treats499AsTransient ensures the retry path used by
// Lookup/GetAttr classifies a 499 (Client Closed Request) the same way it
// treats context.Canceled and 5xx, keeping retry-after-cancel semantics aligned
// with the server's auth middleware.
func TestIsTransientLookupErr_Treats499AsTransient(t *testing.T) {
	if !isTransientLookupErr(&client.StatusError{StatusCode: 499, Message: ""}) {
		t.Fatal("499 should be classified as transient")
	}
}

func TestOpenReadOnlyLargeFileGetsPrefetcher(t *testing.T) {
	size := int64(defaultReadCacheMaxFileSize + 1024) // above default read-cache admission limit
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
	if out.OpenFlags != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("open flags = %d, want FOPEN_KEEP_CACHE for mmap compatibility", out.OpenFlags)
	}
}

func TestOpenReadOnlySQLiteUsesDirectIO(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	ino := fs.inodes.Lookup("/workload.db", false, 4096, time.Now())

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if out.OpenFlags != gofuse.FOPEN_DIRECT_IO {
		t.Fatalf("open flags = %d, want FOPEN_DIRECT_IO for SQLite reader coherence", out.OpenFlags)
	}
}

func TestOpenReadOnlyPendingShadowUsesDirectIO(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending

	path := "/handles/open-renamed.txt"
	data := []byte("pending shadow bytes")
	if err := shadow.WriteFull(path, data, 0); err != nil {
		t.Fatalf("WriteFull: %v", err)
	}
	if _, err := pending.PutWithBaseRev(path, int64(len(data)), PendingOverwrite, 0); err != nil {
		t.Fatalf("PutWithBaseRev: %v", err)
	}
	fs.inodes.Lookup("/handles", true, 0, time.Now())
	ino := fs.inodes.Lookup(path, false, int64(len(data)), time.Now())

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if out.OpenFlags != gofuse.FOPEN_DIRECT_IO {
		t.Fatalf("open flags = %d, want FOPEN_DIRECT_IO for pending shadow coherence", out.OpenFlags)
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if !fh.ShadowPinned || fh.ShadowGen == 0 {
		t.Fatalf("shadow pin = (%v, %d), want pinned generation", fh.ShadowPinned, fh.ShadowGen)
	}
}

func TestOpenWritableSQLiteUsesDirectIO(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	ino := fs.inodes.Lookup("/workload.db", false, 4, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.readCache.Put("/workload.db", []byte("seed"), 1)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if out.OpenFlags != gofuse.FOPEN_DIRECT_IO {
		t.Fatalf("open flags = %d, want FOPEN_DIRECT_IO for SQLite writer coherence", out.OpenFlags)
	}
}

func TestOpenReadOnlyCacheableFileSkipsPrefetcher(t *testing.T) {
	size := int64(defaultReadCacheMaxFileSize)
	fs, ino, cleanup := newTestDat9FS(t, size, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, int(size)))
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
	if fh.Prefetch != nil {
		t.Fatal("cacheable read-only file should use read cache without prefetcher")
	}
}

func TestOpenWritableLargeFileGetsLazyPreload(t *testing.T) {
	size := int64(1024 * 1024) // 1MB — above defaultSmallFileThreshold
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

func TestLazyWritablePreloadUsesRenamedPath(t *testing.T) {
	const size = int64(1024 * 1024)
	var getPath string

	fs, ino, cleanup := newTestDat9FS(t, size, func(w http.ResponseWriter, r *http.Request) {
		getPath = r.URL.Path
		if !strings.HasSuffix(r.URL.Path, "/new.bin") {
			http.Error(w, "unexpected read path "+r.URL.Path, http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("renamed-data"))
	})
	defer cleanup()

	oldPath := "/file.bin"
	newPath := "/new.bin"
	fh := &FileHandle{Ino: ino, Path: oldPath}
	if st := fs.preloadWritableHandle(context.Background(), fh); st != gofuse.OK {
		t.Fatalf("preloadWritableHandle status = %v, want OK", st)
	}
	if fh.Dirty == nil || fh.Dirty.LoadPart == nil {
		t.Fatal("expected lazy dirty buffer")
	}
	fhID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(fhID, fh)

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, oldPath, newPath)
	if fh.Path != newPath {
		t.Fatalf("file handle path after rename = %q, want %q", fh.Path, newPath)
	}

	fh.Lock()
	err := fh.Dirty.EnsureLoaded(0)
	fh.Unlock()
	if err != nil {
		t.Fatalf("EnsureLoaded after rename: %v", err)
	}
	if !strings.HasSuffix(getPath, newPath) {
		t.Fatalf("lazy load GET path = %q, want suffix %q", getPath, newPath)
	}
}

func TestDefaultTTLIs60Seconds(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	if opts.AttrTTL != defaultPositiveKernelCacheTTL {
		t.Fatalf("default AttrTTL = %v, want %v", opts.AttrTTL, defaultPositiveKernelCacheTTL)
	}
	if opts.EntryTTL != defaultPositiveKernelCacheTTL {
		t.Fatalf("default EntryTTL = %v, want %v", opts.EntryTTL, defaultPositiveKernelCacheTTL)
	}
	if opts.NegativeEntryTTL != time.Second {
		t.Fatalf("default NegativeEntryTTL = %v, want 1s", opts.NegativeEntryTTL)
	}
}

func TestMountOptionsReadCacheTTLDefaults(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	if opts.ReadCacheTTL != defaultReadCacheTTL {
		t.Fatalf("default ReadCacheTTL = %v, want %v", opts.ReadCacheTTL, defaultReadCacheTTL)
	}
}

func TestMountOptionsReadCacheTTLKeepsNoExpiry(t *testing.T) {
	opts := &MountOptions{ReadCacheTTL: readCacheNoExpiryTTL}
	opts.setDefaults()
	if opts.ReadCacheTTL != readCacheNoExpiryTTL {
		t.Fatalf("ReadCacheTTL = %v, want no-expiry sentinel", opts.ReadCacheTTL)
	}
}

func TestNewDat9FSUsesReadCacheTTL(t *testing.T) {
	opts := &MountOptions{ReadCacheTTL: readCacheNoExpiryTTL}
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	if fs.readCache.ttl != readCacheNoExpiryTTL {
		t.Fatalf("read cache ttl = %v, want no-expiry sentinel", fs.readCache.ttl)
	}
}

func TestInteractiveProfileAppliesTTLDefaults(t *testing.T) {
	opts := &MountOptions{Profile: "interactive"}
	opts.setDefaults()
	if opts.AttrTTL != time.Second {
		t.Fatalf("interactive AttrTTL = %v, want 1s", opts.AttrTTL)
	}
	if opts.EntryTTL != time.Second {
		t.Fatalf("interactive EntryTTL = %v, want 1s", opts.EntryTTL)
	}
	if opts.DirTTL != 2*time.Second {
		t.Fatalf("interactive DirTTL = %v, want 2s", opts.DirTTL)
	}
}

func TestInteractiveProfileKeepsExplicitTTLs(t *testing.T) {
	opts := &MountOptions{
		Profile:  "interactive",
		AttrTTL:  5 * time.Second,
		EntryTTL: 6 * time.Second,
		DirTTL:   7 * time.Second,
	}
	opts.setDefaults()
	if opts.AttrTTL != 5*time.Second {
		t.Fatalf("explicit AttrTTL = %v, want 5s", opts.AttrTTL)
	}
	if opts.EntryTTL != 6*time.Second {
		t.Fatalf("explicit EntryTTL = %v, want 6s", opts.EntryTTL)
	}
	if opts.DirTTL != 7*time.Second {
		t.Fatalf("explicit DirTTL = %v, want 7s", opts.DirTTL)
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

func TestMountOptionsReadConcurrencyDefaults(t *testing.T) {
	defaults := &MountOptions{}
	defaults.setDefaults()
	if defaults.ReadConcurrency != defaultRemoteReadConcurrency {
		t.Fatalf("default ReadConcurrency = %d, want %d", defaults.ReadConcurrency, defaultRemoteReadConcurrency)
	}

	explicit := &MountOptions{ReadConcurrency: 7}
	explicit.setDefaults()
	if explicit.ReadConcurrency != 7 {
		t.Fatalf("explicit ReadConcurrency = %d, want 7", explicit.ReadConcurrency)
	}
}

func TestMountOptionsParallelReadDefaults(t *testing.T) {
	defaults := &MountOptions{}
	defaults.setDefaults()
	if defaults.ParallelReadConcurrency != defaultParallelReadConcurrency {
		t.Fatalf("default ParallelReadConcurrency = %d, want %d", defaults.ParallelReadConcurrency, defaultParallelReadConcurrency)
	}
	if defaults.ParallelReadBlockSize != defaultParallelReadBlockSize {
		t.Fatalf("default ParallelReadBlockSize = %d, want %d", defaults.ParallelReadBlockSize, defaultParallelReadBlockSize)
	}

	explicit := &MountOptions{ParallelReadConcurrency: 7, ParallelReadBlockSize: 2 << 20}
	explicit.setDefaults()
	if explicit.ParallelReadConcurrency != 7 {
		t.Fatalf("explicit ParallelReadConcurrency = %d, want 7", explicit.ParallelReadConcurrency)
	}
	if explicit.ParallelReadBlockSize != 2<<20 {
		t.Fatalf("explicit ParallelReadBlockSize = %d, want %d", explicit.ParallelReadBlockSize, int64(2<<20))
	}
}

func TestMountOptionsUploadConcurrencyDefaults(t *testing.T) {
	defaults := &MountOptions{}
	defaults.setDefaults()
	if defaults.UploadConcurrency != defaultUploadConcurrency {
		t.Fatalf("default UploadConcurrency = %d, want %d", defaults.UploadConcurrency, defaultUploadConcurrency)
	}

	explicit := &MountOptions{UploadConcurrency: 7}
	explicit.setDefaults()
	if explicit.UploadConcurrency != 7 {
		t.Fatalf("explicit UploadConcurrency = %d, want 7", explicit.UploadConcurrency)
	}

	profileExplicit := &MountOptions{Profile: "interactive", UploadConcurrency: 9}
	profileExplicit.setDefaults()
	if profileExplicit.UploadConcurrency != 9 {
		t.Fatalf("interactive profile explicit UploadConcurrency = %d, want 9", profileExplicit.UploadConcurrency)
	}
}

func TestDirCacheMaxEntriesOrDefault(t *testing.T) {
	// Zero uses default.
	if got := dirCacheMaxEntriesOrDefault(0); got != defaultNamespaceCacheMaxEntries {
		t.Fatalf("dirCacheMaxEntriesOrDefault(0) = %d, want %d", got, defaultNamespaceCacheMaxEntries)
	}
	// Negative uses default.
	if got := dirCacheMaxEntriesOrDefault(-1); got != defaultNamespaceCacheMaxEntries {
		t.Fatalf("dirCacheMaxEntriesOrDefault(-1) = %d, want %d", got, defaultNamespaceCacheMaxEntries)
	}
	// Explicit positive value is used.
	if got := dirCacheMaxEntriesOrDefault(200000); got != 200000 {
		t.Fatalf("dirCacheMaxEntriesOrDefault(200000) = %d, want 200000", got)
	}
}

func TestCommitQueueMaxPendingResolution(t *testing.T) {
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Zero maxPending → NewCommitQueue uses internal default (maxCommitQueuePending=500).
	cq0 := NewCommitQueue(nil, shadow, pending, nil, 1, 0)
	if cq0.maxPending != maxCommitQueuePending {
		t.Fatalf("NewCommitQueue(maxPending=0).maxPending = %d, want %d", cq0.maxPending, maxCommitQueuePending)
	}

	// Explicit positive value is honored.
	cq200 := NewCommitQueue(nil, shadow, pending, nil, 1, 200)
	if cq200.maxPending != 200 {
		t.Fatalf("NewCommitQueue(maxPending=200).maxPending = %d, want 200", cq200.maxPending)
	}
}

func TestMountOptionsCodingAgentPolicyValidation(t *testing.T) {
	codingAgent := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: t.TempDir(),
	}
	codingAgent.setDefaults()
	if err := validateMountOptionsProfile(codingAgent); err != nil {
		t.Fatalf("coding-agent profile validation failed: %v", err)
	}

	paddedLocalRoot := t.TempDir()
	codingAgentPadded := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: " " + paddedLocalRoot + " ",
	}
	codingAgentPadded.setDefaults()
	if err := validateMountOptionsProfile(codingAgentPadded); err != nil {
		t.Fatalf("coding-agent padded LocalRoot validation failed: %v", err)
	}
	if codingAgentPadded.LocalRoot != paddedLocalRoot {
		t.Fatalf("LocalRoot = %q, want trimmed %q", codingAgentPadded.LocalRoot, paddedLocalRoot)
	}

	missingRoot := &MountOptions{Profile: MountProfileCodingAgent}
	missingRoot.setDefaults()
	if err := validateMountOptionsProfile(missingRoot); err == nil {
		t.Fatal("coding-agent profile without LocalRoot should fail")
	}

	ordinaryWithPolicy := &MountOptions{LocalOnlyPatterns: []string{"**/.git/**"}}
	ordinaryWithPolicy.setDefaults()
	if err := validateMountOptionsProfile(ordinaryWithPolicy); err == nil {
		t.Fatal("ordinary mount with local-only policy should fail")
	}

	invalidPattern := &MountOptions{
		Profile:           MountProfileCodingAgent,
		LocalRoot:         t.TempDir(),
		LocalOnlyPatterns: []string{"**/../.git/**"},
	}
	invalidPattern.setDefaults()
	if err := validateMountOptionsProfile(invalidPattern); err == nil {
		t.Fatal("coding-agent mount with unsafe policy pattern should fail")
	}
}

func TestDat9FSClassifiesCodingAgentLocalPolicy(t *testing.T) {
	opts := &MountOptions{
		CacheSize:    1 << 20,
		DirTTL:       time.Second,
		Profile:      MountProfileCodingAgent,
		LocalRoot:    t.TempDir(),
		PerfCounters: true,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	if got := fs.observePathPolicy("/repo/.git/config"); got != PathLayerLocalOnly {
		t.Fatalf(".git classification = %s, want local-only", got)
	}
	if got := fs.observePathPolicy("/repo/src/main.go"); got != PathLayerRemotePersistent {
		t.Fatalf("source classification = %s, want remote persistent", got)
	}

	snap := fs.perf.snapshot()
	if got := snap.Counters["local_policy_local_only"]; got != 1 {
		t.Fatalf("local policy local-only counter = %d, want 1", got)
	}
	if got := snap.Counters["local_policy_remote_default"]; got != 1 {
		t.Fatalf("local policy remote-default counter = %d, want 1", got)
	}
}

func TestCodingAgentLocalOverlayCreateReadWriteDoesNotUseRemote(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be used for local-only overlay paths", http.StatusInternalServerError)
	}))
	defer ts.Close()

	localRoot := t.TempDir()
	opts := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: localRoot,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	var gitOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o755,
	}, ".git", &gitOut); st != gofuse.OK {
		t.Fatalf("Mkdir .git: %v", st)
	}

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: gitOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "config", &createOut); st != gofuse.OK {
		t.Fatalf("Create config: %v", st)
	}

	content := []byte("[core]\n\trepositoryformatversion = 0\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write config: %v", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush config: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})

	localPath := localRoot + "/overlay/repo/.git/config"
	gotFile, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("ReadFile local overlay: %v", err)
	}
	if string(gotFile) != string(content) {
		t.Fatalf("local overlay content = %q, want %q", gotFile, content)
	}

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: gitOut.NodeId}, "config", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup config: %v", st)
	}
	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open config: %v", st)
	}
	buf := make([]byte, len(content)+8)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       openOut.Fh,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read config: %v", st)
	}
	got, _ := result.Bytes(buf)
	if string(got) != string(content) {
		t.Fatalf("Read config = %q, want %q", got, content)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestSQLiteWALIndexSidecarUsesTransientLocalOverlay(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be used for sqlite wal-index sidecars", http.StatusInternalServerError)
	}))
	defer ts.Close()

	transientRoot := t.TempDir()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.transientLocalOverlay = NewLocalOverlay(transientRoot)
	if err := fs.transientLocalOverlay.EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot transient overlay: %v", err)
	}
	fs.openHandles.Add(&FileHandle{Path: "/repo/workload.db"})

	ctx := context.Background()
	if overlay, local, st := fs.localOverlayForPath(ctx, "/repo/workload.db-shm"); !local || st != gofuse.OK || overlay != fs.transientLocalOverlay {
		t.Fatalf("sqlite -shm overlay = (%p, %t, %v), want transient local overlay", overlay, local, st)
	}
	for _, persistentPath := range []string{"/repo/workload.db", "/repo/workload.db-wal", "/repo/workload.db-journal"} {
		if overlay, local, st := fs.localOverlayForPath(ctx, persistentPath); local || st != gofuse.OK || overlay != nil {
			t.Fatalf("%s overlay = (%p, %t, %v), want remote persistent", persistentPath, overlay, local, st)
		}
	}

	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "workload.db-shm", &createOut); st != gofuse.OK {
		t.Fatalf("Create workload.db-shm: %v", st)
	}

	content := []byte("sqlite wal-index bytes")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write workload.db-shm: %v", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush workload.db-shm: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})

	localPath := transientRoot + "/overlay/repo/workload.db-shm"
	gotFile, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("ReadFile transient overlay: %v", err)
	}
	if string(gotFile) != string(content) {
		t.Fatalf("transient overlay content = %q, want %q", gotFile, content)
	}

	entries, err := fs.mergeLocalDirEntries(ctx, "/repo", nil)
	if err != nil {
		t.Fatalf("mergeLocalDirEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "workload.db-shm" {
		t.Fatalf("merged entries = %#v, want workload.db-shm only", entries)
	}

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "workload.db-shm", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup workload.db-shm: %v", st)
	}
	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open workload.db-shm: %v", st)
	}
	buf := make([]byte, len(content)+8)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       openOut.Fh,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read workload.db-shm: %v", st)
	}
	got, _ := result.Bytes(buf)
	if string(got) != string(content) {
		t.Fatalf("Read workload.db-shm = %q, want %q", got, content)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestSQLiteWALIndexSidecarFailsClosedWhenTransientOverlayMissing(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be used for sqlite wal-index sidecars", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.openHandles.Add(&FileHandle{Path: "/repo/workload.db"})

	overlay, local, st := fs.localOverlayForPath(context.Background(), "/repo/workload.db-shm")
	if overlay != nil || !local || st != gofuse.EIO {
		t.Fatalf("sqlite -shm missing overlay = (%p, %t, %v), want local EIO", overlay, local, st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestWaitQueuedRemoteCommitBeforeWriteBlocksUntilQueuedCommitDone(t *testing.T) {
	path := "/repo/data.bin"
	entry := &CommitEntry{Path: path}
	cq := &CommitQueue{
		queue:        []*CommitEntry{entry},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.rebuildQueuedIndexLocked()
	fs := &Dat9FS{commitQueue: cq}

	done := make(chan struct{})
	go func() {
		unlock := fs.waitQueuedRemoteCommitBeforeWrite(path)
		unlock()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("waitQueuedRemoteCommitBeforeWrite returned while commit was queued")
	case <-time.After(25 * time.Millisecond):
	}

	cq.removeFromQueue(entry)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitQueuedRemoteCommitBeforeWrite did not return after queue removal")
	}
}

func TestLockWritableRemoteCommitPathTimeoutProceedsAfterDeadline(t *testing.T) {
	// Verify that lockWritableRemoteCommitPath returns within a bounded
	// timeout even when the commit queue never releases the path. This covers
	// BOTH the queued case and the in-flight case (worker holds the per-path
	// remote commit lock, simulating a slow upload that never completes).
	path := "/repo/stuck.bin"
	entry := &CommitEntry{Path: path}
	cq := &CommitQueue{
		queue:        []*CommitEntry{entry},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.rebuildQueuedIndexLocked()

	opts := &MountOptions{}
	opts.setDefaults()
	// Use a short timeout for the test so we don't wait 5s.
	opts.RemoteCommitWaitTimeout = 200 * time.Millisecond
	fs := &Dat9FS{commitQueue: cq, opts: opts}
	fs.remoteCommitLocks = make(map[string]*sync.Mutex)

	// Pre-lock the per-path remote commit mutex to simulate an in-flight
	// worker holding the lock during a slow upload. This reproduces the
	// actual deadlock scenario: the FUSE handler busy-waits holding fh.mu
	// while the worker holds remoteCommitLocks[path] across a long upload.
	fs.lockRemoteCommitPath(path)

	start := time.Now()
	unlock := fs.lockWritableRemoteCommitPath(path)
	elapsed := time.Since(start)
	unlock()

	// Should have waited at least ~200ms (the timeout) but not more than ~2s.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("lockWritableRemoteCommitPath returned in %s, expected >= 150ms (timeout)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("lockWritableRemoteCommitPath returned in %s, expected < 2s (timeout bounded)", elapsed)
	}
}

func TestCodingAgentLocalOverlayFlushSkipsSyncForGeneratedPath(t *testing.T) {
	var syncCalls atomic.Int32
	previousSync := syncOpenLocalFile
	syncOpenLocalFile = func(file *os.File) error {
		syncCalls.Add(1)
		return syscall.EIO
	}
	t.Cleanup(func() {
		syncOpenLocalFile = previousSync
	})

	opts := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: t.TempDir(),
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	var nodeModulesOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o755,
	}, "node_modules", &nodeModulesOut); st != gofuse.OK {
		t.Fatalf("Mkdir node_modules: %v", st)
	}
	var pkgOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: nodeModulesOut.NodeId},
		Mode:     0o755,
	}, "pkg", &pkgOut); st != gofuse.OK {
		t.Fatalf("Mkdir pkg: %v", st)
	}

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: pkgOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "index.js", &createOut); st != gofuse.OK {
		t.Fatalf("Create index.js: %v", st)
	}
	t.Cleanup(func() {
		fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	})

	content := []byte("module.exports = 1\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write index.js: %v", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush generated local-only file status = %v, want OK", st)
	}
	if got := syncCalls.Load(); got != 0 {
		t.Fatalf("Flush sync calls = %d, want 0", got)
	}

	if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: createOut.Fh}); st != gofuse.Status(syscall.EIO) {
		t.Fatalf("Fsync status = %v, want EIO", st)
	}
	if got := syncCalls.Load(); got != 1 {
		t.Fatalf("Fsync sync calls = %d, want 1", got)
	}
}

func TestCodingAgentLocalOverlayFlushSyncsGitState(t *testing.T) {
	var syncCalls atomic.Int32
	previousSync := syncOpenLocalFile
	syncOpenLocalFile = func(file *os.File) error {
		syncCalls.Add(1)
		return nil
	}
	t.Cleanup(func() {
		syncOpenLocalFile = previousSync
	})

	opts := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: t.TempDir(),
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	var gitOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o755,
	}, ".git", &gitOut); st != gofuse.OK {
		t.Fatalf("Mkdir .git: %v", st)
	}

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: gitOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "config", &createOut); st != gofuse.OK {
		t.Fatalf("Create config: %v", st)
	}
	t.Cleanup(func() {
		fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	})

	content := []byte("[core]\n\trepositoryformatversion = 0\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write config: %v", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush .git config status = %v, want OK", st)
	}
	if got := syncCalls.Load(); got != 1 {
		t.Fatalf("Flush sync calls = %d, want 1", got)
	}
}

func TestCodingAgentLocalOverlayCrossLayerRenameReturnsEXDEV(t *testing.T) {
	opts := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: t.TempDir(),
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	gitIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())
	fs.inodes.Lookup("/repo/.git/config.lock", false, 4, time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: gitIno},
		Newdir:   repoIno,
	}, "config.lock", "config")
	if st != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename local->remote status = %v, want EXDEV", st)
	}
}

func TestCodingAgentLocalOverlayReadDirMergesRemoteAndLocalEntries(t *testing.T) {
	localRoot := t.TempDir()
	opts := &MountOptions{
		Profile:   MountProfileCodingAgent,
		LocalRoot: localRoot,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	if err := os.MkdirAll(localRoot+"/overlay/repo/.git", 0o755); err != nil {
		t.Fatalf("MkdirAll local .git: %v", err)
	}
	remote := []DirEntry{
		{Name: ".git", Ino: 99, Mode: uint32(syscall.S_IFREG) | 0o644},
		{Name: "README.md", Ino: 100, Mode: uint32(syscall.S_IFREG) | 0o644},
	}
	entries, err := fs.mergeLocalDirEntries(context.Background(), "/repo", remote)
	if err != nil {
		t.Fatalf("mergeLocalDirEntries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("entry count = %d, want 2: %#v", len(entries), entries)
	}
	if entries[0].Name != ".git" || !entries[0].IsDir {
		t.Fatalf("first entry = %#v, want local .git directory override", entries[0])
	}
	if entries[1].Name != "README.md" {
		t.Fatalf("second entry = %#v, want README.md", entries[1])
	}
}

func TestMountOptionsSyncReadDefaultsToAsyncReads(t *testing.T) {
	defaults := &MountOptions{}
	defaults.setDefaults()
	if defaults.SyncRead {
		t.Fatal("default SyncRead = true, want false")
	}

	explicit := &MountOptions{SyncRead: true}
	explicit.setDefaults()
	if !explicit.SyncRead {
		t.Fatal("explicit SyncRead = false, want true")
	}
}

func TestGoFuseMountOptionsMapsSyncRead(t *testing.T) {
	defaults := newGoFuseMountOptions(&MountOptions{})
	if defaults.SyncRead {
		t.Fatal("default go-fuse SyncRead = true, want false")
	}

	explicit := newGoFuseMountOptions(&MountOptions{SyncRead: true})
	if !explicit.SyncRead {
		t.Fatal("explicit go-fuse SyncRead = false, want true")
	}
	if explicit.MaxBackground != 32 {
		t.Fatalf("MaxBackground = %d, want 32", explicit.MaxBackground)
	}
}

func TestGoFuseMountOptionsAllowOtherUsesDefaultPermissionsOnLinux(t *testing.T) {
	opts := newGoFuseMountOptions(&MountOptions{AllowOther: true})
	hasDefaultPermissions := false
	for _, opt := range opts.Options {
		if opt == "default_permissions" {
			hasDefaultPermissions = true
			break
		}
	}
	if runtime.GOOS == "linux" && !hasDefaultPermissions {
		t.Fatal("AllowOther linux mount missing default_permissions")
	}
	if runtime.GOOS != "linux" && hasDefaultPermissions {
		t.Fatal("non-linux mount should not force default_permissions")
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
	// The entry timeout should match the configured default TTL.
	// go-fuse stores timeouts in seconds + nanoseconds.
	wantSeconds := uint64(defaultPositiveKernelCacheTTL / time.Second)
	if out.EntryValid < wantSeconds || out.EntryValid > wantSeconds+1 {
		t.Fatalf("EntryValid = %d, want ~%d", out.EntryValid, wantSeconds)
	}
	if out.AttrValid < wantSeconds || out.AttrValid > wantSeconds+1 {
		t.Fatalf("AttrValid = %d, want ~%d", out.AttrValid, wantSeconds)
	}
}

func TestLockFileLookupDisablesEntryCache(t *testing.T) {
	fs, _, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	var lockOut gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config.lock", &lockOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup config.lock status = %v, want OK", st)
	}
	if got := lockOut.EntryTimeout(); got != 0 {
		t.Fatalf("config.lock EntryTimeout = %v, want 0", got)
	}
	if got := lockOut.AttrTimeout(); got != fs.opts.AttrTTL {
		t.Fatalf("config.lock AttrTimeout = %v, want %v", got, fs.opts.AttrTTL)
	}

	var regularOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config", &regularOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup config status = %v, want OK", st)
	}
	if got := regularOut.EntryTimeout(); got != fs.opts.EntryTTL {
		t.Fatalf("regular EntryTimeout = %v, want %v", got, fs.opts.EntryTTL)
	}
}

func TestLockFileCreateDisablesEntryCache(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	var lockOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL),
	}, "config.lock", &lockOut)
	if st != gofuse.OK {
		t.Fatalf("Create config.lock status = %v, want OK", st)
	}
	if got := lockOut.EntryTimeout(); got != 0 {
		t.Fatalf("config.lock EntryTimeout = %v, want 0", got)
	}

	var regularOut gofuse.CreateOut
	st = fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL),
	}, "regular.txt", &regularOut)
	if st != gofuse.OK {
		t.Fatalf("Create regular.txt status = %v, want OK", st)
	}
	if got := regularOut.EntryTimeout(); got != fs.opts.EntryTTL {
		t.Fatalf("regular EntryTimeout = %v, want %v", got, fs.opts.EntryTTL)
	}
}

func TestCreatePreservesInputMode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     0o755,
	}, "exec.sh", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if got, want := out.Mode, uint32(syscall.S_IFREG)|0o755; got != want {
		t.Fatalf("Create mode = %o, want %o", got, want)
	}

	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("created inode not found")
	}
	if !entry.HasMode || entry.Mode != 0o755 {
		t.Fatalf("inode mode = %o has=%t, want 0755 true", entry.Mode, entry.HasMode)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("created file handle not found")
	}
	fh.Lock()
	defer fh.Unlock()
	if !fh.HasPendingMode || fh.PendingMode != 0o755 {
		t.Fatalf("pending mode = %o has=%t, want 0755 true", fh.PendingMode, fh.HasPendingMode)
	}
}

func TestCreatePreservesZeroMode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     0,
	}, "private.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if got, want := out.Mode, uint32(syscall.S_IFREG); got != want {
		t.Fatalf("Create mode = %o, want %o", got, want)
	}

	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("created inode not found")
	}
	if !entry.HasMode || entry.Mode != 0 {
		t.Fatalf("inode mode = %o has=%t, want 000 true", entry.Mode, entry.HasMode)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("created file handle not found")
	}
	fh.Lock()
	defer fh.Unlock()
	if !fh.HasPendingMode || fh.PendingMode != 0 {
		t.Fatalf("pending mode = %o has=%t, want 000 true", fh.PendingMode, fh.HasPendingMode)
	}
}

func TestCreateDefaultModeDoesNotStageRemoteMode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     defaultRegularFileMode,
	}, "plain.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if got, want := out.Mode, uint32(syscall.S_IFREG)|defaultRegularFileMode; got != want {
		t.Fatalf("Create mode = %o, want %o", got, want)
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("created file handle not found")
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.HasPendingMode {
		t.Fatalf("default create mode should not require remote chmod, got pending %o", fh.PendingMode)
	}
}

func TestDeferredChmodRollbackRestoresUnknownMode(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
			http.Error(w, "chmod failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/rollback.txt", false, 4, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if entry.HasMode {
		t.Fatal("test setup expected unknown mode")
	}

	wb := NewWriteBuffer("/rollback.txt", maxPreloadSize, 0)
	if _, err := wb.Write(0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:     ino,
		Path:    "/rollback.txt",
		Dirty:   wb,
		BaseRev: 3,
	}
	fs.fileHandles.Allocate(fh)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	fh.Lock()
	err := fs.applyPendingModeForHandleLocked(context.Background(), fh)
	fh.Unlock()
	if err == nil {
		t.Fatal("applyPendingModeForHandleLocked should fail")
	}

	entry, ok = fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode not found after rollback")
	}
	if entry.HasMode {
		t.Fatalf("rollback should restore unknown mode, got mode=%o", entry.Mode)
	}
}

func TestReleaseRetriesDeferredChmodNotFound(t *testing.T) {
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/release-retry.txt":
			http.Error(w, "not found", http.StatusNotFound)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/release-retry.txt", false, 0, time.Now())
	fs.inodes.UpdateMode(ino, 0o644)
	fh := &FileHandle{
		Ino:               ino,
		Path:              "/release-retry.txt",
		PendingMode:       0o755,
		HasPendingMode:    true,
		PreviousMode:      0o644,
		HasPreviousMode:   true,
		PreviousModeKnown: true,
	}
	fhID := fs.fileHandles.Allocate(fh)

	fs.Release(nil, &gofuse.ReleaseIn{Fh: fhID})

	if got := chmodCalls.Load(); got != 2 {
		t.Fatalf("chmod calls = %d, want 2", got)
	}
	if fh.HasPendingMode {
		t.Fatal("pending mode should be cleared after Release retry succeeds")
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode not found after Release")
	}
	if !entry.HasMode || entry.Mode&0o777 != 0o755 {
		t.Fatalf("inode mode = %o has=%t, want 0755 true", entry.Mode&0o777, entry.HasMode)
	}
}

func TestApplyPendingModeDoesNotClearNewerConcurrentChmod(t *testing.T) {
	var chmodCalls atomic.Int32
	chmodStarted := make(chan struct{})
	allowChmod := make(chan struct{})
	var startedOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				startedOnce.Do(func() { close(chmodStarted) })
				<-allowChmod
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/chmod-race.txt", false, 4, time.Now())
	fs.inodes.UpdateMode(ino, 0o644)
	fh1 := &FileHandle{Ino: ino, Path: "/chmod-race.txt", Dirty: NewWriteBuffer("/chmod-race.txt", maxPreloadSize, 0), BaseRev: 1}
	fh2 := &FileHandle{Ino: ino, Path: "/chmod-race.txt", Dirty: NewWriteBuffer("/chmod-race.txt", maxPreloadSize, 0), BaseRev: 1}
	fs.fileHandles.Allocate(fh1)
	fs.fileHandles.Allocate(fh2)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr 0755 status = %v, want OK", st)
	}

	done := make(chan error, 1)
	go func() {
		fh1.Lock()
		err := fs.applyPendingModeForHandleLocked(context.Background(), fh1)
		fh1.Unlock()
		done <- err
	}()

	select {
	case <-chmodStarted:
	case <-time.After(time.Second):
		t.Fatal("first chmod did not start")
	}

	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o700,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr 0700 status = %v, want OK", st)
	}
	close(allowChmod)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("applyPendingModeForHandleLocked: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending chmod did not finish")
	}

	for i, fh := range []*FileHandle{fh1, fh2} {
		fh.Lock()
		hasPending := fh.HasPendingMode
		pendingMode := fh.PendingMode & 0o777
		fh.Unlock()
		if !hasPending || pendingMode != 0o700 {
			t.Fatalf("fh%d pending mode = %o has=%t, want 0700 true", i+1, pendingMode, hasPending)
		}
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if !entry.HasMode || entry.Mode&0o777 != 0o700 {
		t.Fatalf("inode mode = %o has=%t, want 0700 true", entry.Mode&0o777, entry.HasMode)
	}
}

func TestReleaseDeferredChmodDoesNotClearNewerConcurrentChmod(t *testing.T) {
	var chmodCalls atomic.Int32
	chmodStarted := make(chan struct{})
	allowChmod := make(chan struct{})
	var startedOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				startedOnce.Do(func() { close(chmodStarted) })
				<-allowChmod
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/release-chmod-race.txt", false, 0, time.Now())
	fs.inodes.UpdateMode(ino, 0o644)
	fh1 := &FileHandle{Ino: ino, Path: "/release-chmod-race.txt", Dirty: NewWriteBuffer("/release-chmod-race.txt", maxPreloadSize, 0), BaseRev: 1}
	fh2 := &FileHandle{Ino: ino, Path: "/release-chmod-race.txt", Dirty: NewWriteBuffer("/release-chmod-race.txt", maxPreloadSize, 0), BaseRev: 1}
	fh1ID := fs.fileHandles.Allocate(fh1)
	fs.fileHandles.Allocate(fh2)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o755,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr 0755 status = %v, want OK", st)
	}

	releaseDone := make(chan struct{})
	go func() {
		fs.Release(nil, &gofuse.ReleaseIn{Fh: fh1ID})
		close(releaseDone)
	}()

	select {
	case <-chmodStarted:
	case <-time.After(time.Second):
		t.Fatal("release chmod did not start")
	}

	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o700,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr 0700 status = %v, want OK", st)
	}
	close(allowChmod)

	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("Release did not finish")
	}

	fh2.Lock()
	hasPending := fh2.HasPendingMode
	pendingMode := fh2.PendingMode & 0o777
	fh2.Unlock()
	if !hasPending || pendingMode != 0o700 {
		t.Fatalf("sibling pending mode = %o has=%t, want 0700 true", pendingMode, hasPending)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if !entry.HasMode || entry.Mode&0o777 != 0o700 {
		t.Fatalf("inode mode = %o has=%t, want 0700 true", entry.Mode&0o777, entry.HasMode)
	}
}

func TestLookupPendingIndexPreservesMode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending
	if _, err := pending.PutWithBaseRevAndMode("/exec.sh", 3, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "exec.sh", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got, want := out.Mode, uint32(syscall.S_IFREG)|0o755; got != want {
		t.Fatalf("Lookup mode = %o, want %o", got, want)
	}
}

func TestFlushStagesCreateModeInPendingMetadata(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.writeBack = writeBack

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     0o755,
	}, "exec.sh", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: out.NodeId},
		Fh:       out.Fh,
	}, []byte("echo ok\n")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	st = fs.Flush(nil, &gofuse.FlushIn{Fh: out.Fh})
	if st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}

	pendingMeta, ok := pending.GetMeta("/exec.sh")
	if !ok {
		t.Fatal("pending index entry missing")
	}
	if !pendingMeta.HasMode || pendingMeta.Mode != 0o755 {
		t.Fatalf("pending mode = %o has=%t, want 0755 true", pendingMeta.Mode, pendingMeta.HasMode)
	}
	writeBackMeta, ok := writeBack.GetMeta("/exec.sh")
	if !ok {
		t.Fatal("writeback entry missing")
	}
	if !writeBackMeta.HasMode || writeBackMeta.Mode != 0o755 {
		t.Fatalf("writeback mode = %o has=%t, want 0755 true", writeBackMeta.Mode, writeBackMeta.HasMode)
	}
}

func TestInitStoresServer(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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

func TestMkdirRetriesDetachedAfterTransientInterrupt(t *testing.T) {
	var mkdirCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && (r.URL.RawQuery == "mkdir" || r.URL.RawQuery == "mkdir&mode=0"):
			if r.URL.Path != "/v1/fs/dir" {
				t.Fatalf("POST path = %q, want /v1/fs/dir", r.URL.Path)
			}
			if mkdirCalls.Add(1) == 1 {
				w.WriteHeader(statusClientClosedRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "dir", &out)
	if st != gofuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", st)
	}
	if got := mkdirCalls.Load(); got != 2 {
		t.Fatalf("mkdir calls = %d, want 2", got)
	}
	if out.NodeId == 0 {
		t.Fatal("expected mkdir response to include a node id")
	}
}

func TestMkdirRetryTreatsRemoteDirectoryAsSuccessAfterAmbiguousCreate(t *testing.T) {
	var mkdirCalls atomic.Int64
	var statCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && (r.URL.RawQuery == "mkdir" || r.URL.RawQuery == "mkdir&mode=0"):
			switch mkdirCalls.Add(1) {
			case 1:
				w.WriteHeader(statusClientClosedRequest)
			case 2:
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "path already exists"})
			default:
				t.Fatalf("unexpected extra mkdir call")
			}
		case r.Method == http.MethodHead:
			if statCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "dir", &out)
	if st != gofuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", st)
	}
	if got := mkdirCalls.Load(); got != 2 {
		t.Fatalf("mkdir calls = %d, want 2", got)
	}
	if got := statCalls.Load(); got != 2 {
		t.Fatalf("stat calls = %d, want 2", got)
	}
}

func TestLookupOpenCreatedFileAfterForgetSupportsGitChmodLock(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for open-created file", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
	}, "config.lock", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if _, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, []byte("[core]\n\tfilemode = false\n")); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	// The kernel may drop the lookup ref while the file handle is still open.
	// A subsequent chmod(path), as used by git's lock-file code, must resolve
	// the still-open local file rather than returning ENOENT before Flush has
	// staged it into PendingIndex.
	fs.Forget(createOut.NodeId, 1)

	var lookupOut gofuse.EntryOut
	st = fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config.lock", &lookupOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup after Forget status = %v, want OK", st)
	}
	if lookupOut.NodeId != createOut.NodeId {
		t.Fatalf("Lookup NodeId = %d, want original inode %d", lookupOut.NodeId, createOut.NodeId)
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o644,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("mode-only SetAttr after Lookup status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0 for open-created lookup", got)
	}
}

func TestOpenWritableCreatedFileUsesOpenLocalHandle(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for open-created file", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL),
	}, "script.tmp", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	var openOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0 for open-created file", got)
	}

	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       openOut.Fh,
		Offset:   0,
	}, []byte("#!/bin/sh\n")); st != gofuse.OK {
		t.Fatalf("Write through reopened handle status = %v, want OK", st)
	}
}

func TestOpenWritablePreloadChoosesNewestOpenHandle(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	ino := fs.inodes.Lookup("/script.tmp", false, 0, time.Now())
	stale := &FileHandle{
		Ino:   ino,
		Path:  "/script.tmp",
		Dirty: fs.newWriteBuffer("/script.tmp", maxPreloadSize, 0),
		IsNew: true,
	}
	if err := stale.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	stale.DirtySeq = fs.markDirtySize(ino, 0)
	fs.openHandles.Add(stale)

	fresh := &FileHandle{
		Ino:   ino,
		Path:  "/script.tmp",
		Dirty: fs.newWriteBuffer("/script.tmp", maxPreloadSize, 0),
		IsNew: true,
	}
	want := []byte("fresh local bytes")
	if _, err := fresh.Dirty.Write(0, want); err != nil {
		t.Fatal(err)
	}
	fresh.DirtySeq = fs.markDirtySize(ino, int64(len(want)))
	fs.openHandles.Add(fresh)

	target := &FileHandle{
		Ino:   ino,
		Path:  "/script.tmp",
		Dirty: fs.newWriteBuffer("/script.tmp", maxPreloadSize, 0),
	}
	if !fs.loadWritableHandleFromOpenHandleLocked(target) {
		t.Fatal("preload from open handle returned false")
	}
	if got := target.Dirty.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("preloaded bytes = %q, want %q", got, want)
	}
	if target.OrigSize != 0 {
		t.Fatalf("target OrigSize = %d, want 0 for pending new file", target.OrigSize)
	}
}

func TestOpenWritablePreloadSkipsCleanSiblingReboundToNewRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	const filePath = "/workload.db"
	stale := []byte("sqlite db before checkpoint")
	committedSize := int64(len("sqlite db after checkpoint with event rows"))
	ino := fs.inodes.Lookup(filePath, false, int64(len(stale)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	sibling := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: int64(len(stale)),
		BaseRev:  1,
	}
	if _, err := sibling.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	sibling.Dirty.ClearDirty()
	fs.openHandles.Add(sibling)

	fs.inodes.UpdateSize(ino, committedSize)
	fs.refreshCommittedRevisionForOpenHandles(filePath, 2, nil)

	if sibling.BaseRev != 2 {
		t.Fatalf("sibling BaseRev = %d, want 2", sibling.BaseRev)
	}
	if sibling.OrigSize != committedSize {
		t.Fatalf("sibling OrigSize = %d, want %d", sibling.OrigSize, committedSize)
	}
	target := &FileHandle{
		Ino:     ino,
		Path:    filePath,
		Dirty:   fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		BaseRev: 2,
	}
	if fs.loadWritableHandleFromOpenHandleLocked(target) {
		t.Fatal("clean sibling rebound to a newer revision must not be used as preload data")
	}
	if got := target.Dirty.Size(); got != 0 {
		t.Fatalf("target dirty size = %d, want 0", got)
	}
}

func TestOpenWritablePreloadSkipsCleanOldRevisionShadowHandle(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const filePath = "/tier-transition.bin"
	stale := bytes.Repeat([]byte{0x5a}, 64*1024)
	ino := fs.inodes.Lookup(filePath, false, int64(len(stale)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.inodes.UpdateSize(ino, 0)
	fs.inodes.UpdateRevision(ino, 8)
	if err := shadow.WriteFull(filePath, stale, 7); err != nil {
		t.Fatal(err)
	}

	sibling := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize:    int64(len(stale)),
		BaseRev:     7,
		ShadowReady: true,
		ShadowSpill: true,
	}
	if _, err := sibling.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	sibling.Dirty.ClearDirty()
	fs.openHandles.Add(sibling)

	target := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: 0,
		BaseRev:  8,
	}
	if fs.loadWritableHandleFromOpenHandleLocked(target) {
		t.Fatal("clean old-revision sibling shadow must not be used as preload data")
	}
	if got := target.Dirty.Size(); got != 0 {
		t.Fatalf("target dirty size = %d, want 0", got)
	}
}

func TestReadCleanWritableHandleRefreshesSkippedCommittedRevision(t *testing.T) {
	stale := []byte("sqlite db before checkpoint")
	fresh := []byte("sqlite db after checkpoint with event rows")
	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(fresh)), func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fresh)
			return
		}
		start, end, ok := parseTestBytesRange(r.Header.Get("Range"))
		if !ok {
			t.Errorf("Range = %q, want bytes range", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start < 0 || end >= int64(len(fresh)) {
			t.Errorf("Range = %q outside fresh object", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fresh[start : end+1])
	})
	defer cleanup()

	const filePath = "/file.bin"
	fh := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: int64(len(stale)),
		BaseRev:  1,
	}
	if _, err := fh.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fhID := fs.allocateFileHandle(fh)

	fs.inodes.UpdateSize(ino, int64(len(fresh)))
	fs.recordCommittedRevision(filePath, 2)

	got, st, err := readDat9FSTestRange(fs, ino, fhID, 0, len(fresh))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatalf("Read returned stale clean buffer: got %q want %q", got, fresh)
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.BaseRev != 2 {
		t.Fatalf("fh.BaseRev = %d, want 2", fh.BaseRev)
	}
	if fh.OrigSize != int64(len(fresh)) {
		t.Fatalf("fh.OrigSize = %d, want %d", fh.OrigSize, len(fresh))
	}
}

func TestReadCleanShadowBackedHandleRefreshesRemovedCommittedShadow(t *testing.T) {
	stale := []byte("old shadow bytes")
	fresh := []byte("fresh committed remote bytes")
	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(fresh)), func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fresh)
			return
		}
		start, end, ok := parseTestBytesRange(r.Header.Get("Range"))
		if !ok {
			t.Errorf("Range = %q, want bytes range", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start < 0 || end >= int64(len(fresh)) {
			t.Errorf("Range = %q outside fresh object", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fresh[start : end+1])
	})
	defer cleanup()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const filePath = "/file.bin"
	fh := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize:    int64(len(stale)),
		BaseRev:     1,
		ShadowReady: true,
		ShadowSpill: true,
	}
	if _, err := fh.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fhID := fs.allocateFileHandle(fh)

	fs.inodes.UpdateSize(ino, int64(len(fresh)))
	fs.recordCommittedRevision(filePath, 2)

	got, st, err := readDat9FSTestRange(fs, ino, fhID, 0, len(fresh))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatalf("Read returned stale removed shadow: got %q want %q", got, fresh)
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.BaseRev != 2 {
		t.Fatalf("fh.BaseRev = %d, want 2", fh.BaseRev)
	}
	if fh.OrigSize != int64(len(fresh)) {
		t.Fatalf("fh.OrigSize = %d, want %d", fh.OrigSize, len(fresh))
	}
	if fh.ShadowReady || fh.ShadowSpill || fh.ShadowCommitReady {
		t.Fatalf("shadow flags = ready:%t spill:%t commit:%t, want all false", fh.ShadowReady, fh.ShadowSpill, fh.ShadowCommitReady)
	}
}

func TestReadCleanShadowBackedHandleRefreshesActiveStaleShadow(t *testing.T) {
	stale := []byte("old active shadow bytes")
	fresh := []byte("fresh committed remote bytes")
	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(fresh)), func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fresh)
			return
		}
		start, end, ok := parseTestBytesRange(r.Header.Get("Range"))
		if !ok {
			t.Errorf("Range = %q, want bytes range", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start < 0 || end >= int64(len(fresh)) {
			t.Errorf("Range = %q outside fresh object", r.Header.Get("Range"))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fresh[start : end+1])
	})
	defer cleanup()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const filePath = "/file.bin"
	if err := shadow.WriteFull(filePath, stale, 1); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize:    int64(len(stale)),
		BaseRev:     1,
		ShadowReady: true,
		ShadowSpill: true,
	}
	if _, err := fh.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fhID := fs.allocateFileHandle(fh)

	fs.inodes.UpdateSize(ino, int64(len(fresh)))
	fs.recordCommittedRevision(filePath, 2)

	got, st, err := readDat9FSTestRange(fs, ino, fhID, 0, len(fresh))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatalf("Read returned stale active shadow: got %q want %q", got, fresh)
	}
	fh.Lock()
	defer fh.Unlock()
	if fh.BaseRev != 2 {
		t.Fatalf("fh.BaseRev = %d, want 2", fh.BaseRev)
	}
	if fh.OrigSize != int64(len(fresh)) {
		t.Fatalf("fh.OrigSize = %d, want %d", fh.OrigSize, len(fresh))
	}
	if fh.ShadowReady || fh.ShadowSpill || fh.ShadowCommitReady {
		t.Fatalf("shadow flags = ready:%t spill:%t commit:%t, want all false", fh.ShadowReady, fh.ShadowSpill, fh.ShadowCommitReady)
	}

	var roOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &roOut)
	if st != gofuse.OK {
		t.Fatalf("read-only Open status = %v, want OK", st)
	}
	got, st, err = readDat9FSTestRange(fs, ino, roOut.Fh, 0, len(fresh))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.OK {
		t.Fatalf("read-only Read status = %v, want OK", st)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatalf("read-only handle pinned stale active shadow: got %q want %q", got, fresh)
	}
}

func TestDebouncedFlushPublishesSizeBeforeRefreshingCleanSibling(t *testing.T) {
	stale := []byte("old")
	fresh := []byte("fresh committed bytes")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read PUT body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if !bytes.Equal(body, fresh) {
				t.Errorf("PUT body = %q, want %q", body, fresh)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: time.Hour}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	const filePath = "/small.txt"
	ino := fs.inodes.Lookup(filePath, false, int64(len(stale)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	writer := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: int64(len(stale)),
		BaseRev:  1,
	}
	if _, err := writer.Dirty.Write(0, fresh); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())

	sibling := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: int64(len(stale)),
		BaseRev:  1,
	}
	if _, err := sibling.Dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	sibling.Dirty.ClearDirty()
	fs.openHandles.Add(sibling)

	writer.Lock()
	st := fs.flushHandleDebounced(context.Background(), writer, false)
	writer.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandleDebounced status = %v, want OK", st)
	}
	fs.debouncer.FlushAll()

	if sibling.BaseRev != 2 {
		t.Fatalf("sibling BaseRev = %d, want 2", sibling.BaseRev)
	}
	if sibling.OrigSize != int64(len(fresh)) {
		t.Fatalf("sibling OrigSize = %d, want %d", sibling.OrigSize, len(fresh))
	}
}

func TestOpenWritablePreloadSkipsSQLitePersistentJournalOpenHandle(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	const filePath = "/workload.db-wal"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, []byte("partial in-flight wal bytes")); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	fs.openHandles.Add(writer)

	target := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if fs.loadWritableHandleFromOpenHandleLocked(target) {
		t.Fatal("sqlite persistent journal must not preload in-flight bytes from sibling writer")
	}
	if got := target.Dirty.Size(); got != 0 {
		t.Fatalf("target dirty size = %d, want 0", got)
	}
}

func TestOpenWritableSQLitePersistentJournalLocalCreateDoesNotStatRemote(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for local uncommitted sqlite sidecar create", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	const filePath = "/workload.db-wal"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	creator := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		IsNew:    true,
		OrigSize: 0,
		BaseRev:  0,
	}
	if err := creator.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	creator.DirtySeq = fs.markDirtySize(ino, 0)
	fs.openHandles.Add(creator)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	reopened, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("reopened handle not found")
	}
	defer fs.deleteFileHandle(out.Fh, reopened)
	reopened.Lock()
	isNew := reopened.IsNew
	baseRev := reopened.BaseRev
	size := reopened.Dirty.Size()
	reopened.Unlock()
	if !isNew || baseRev != 0 || size != 0 {
		t.Fatalf("reopened state isNew=%t baseRev=%d size=%d, want true/0/0", isNew, baseRev, size)
	}
}

func TestOpenWritablePreloadReadsShadowBeforeEvictedBuffer(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for shadow-backed open handle", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL),
	}, "large.tmp", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	want := bytes.Repeat([]byte{0x5a}, DefaultPartSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, want); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	src, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created handle not found")
	}
	src.Lock()
	partLoaded := src.Dirty.IsPartLoaded(0)
	src.Unlock()
	if partLoaded {
		t.Fatal("test setup expected ShadowSpill to evict the first part")
	}

	var openOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	reopened, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("reopened handle not found")
	}
	reopened.Lock()
	got := reopened.Dirty.Bytes()
	origSize := reopened.OrigSize
	isNew := reopened.IsNew
	reopened.Unlock()
	if !bytes.Equal(got, want) {
		t.Fatal("reopened handle did not preload the authoritative shadow bytes")
	}
	if !isNew {
		t.Fatal("reopened handle should preserve pending-new state")
	}
	if origSize != 0 {
		t.Fatalf("reopened OrigSize = %d, want 0 for pending new file", origSize)
	}
}

func TestReadSQLiteSamePathDirtyHandleBeforeRemote(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for same-mount sqlite main-db dirty data", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/workload.db", false, 0, time.Now())
	writer := &FileHandle{
		Ino:   ino,
		Path:  "/workload.db",
		Dirty: fs.newWriteBuffer("/workload.db", maxPreloadSize, 0),
	}
	want := []byte("latest main db bytes")
	if _, err := writer.Dirty.Write(0, want); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(want)))
	fs.openHandles.Add(writer)

	reader := &FileHandle{Ino: ino, Path: "/workload.db"}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes = %q, want %q", got, want)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalSkipsIncompleteDirtyHandle(t *testing.T) {
	const filePath = "/workload.db-wal"
	stable := []byte("stable committed wal bytes")
	dirty := []byte("partial")
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			w.Header().Set("Content-Length", strconv.Itoa(len(stable)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			objectGets.Add(1)
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(stable)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(stable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(stable)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirty)))
	fs.openHandles.Add(writer)

	if _, _, ok, st := fs.readSamePathDirtyHandle(filePath, nil, 0, uint32(len(dirty))); ok || st != gofuse.OK {
		t.Fatalf("persistent journal same-path dirty claimed=%t status=%v, want false/OK", ok, st)
	}

	reader := &FileHandle{Ino: ino, Path: filePath, OrigSize: int64(len(stable)), BaseRev: 7}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(stable)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, stable) {
		t.Fatalf("read bytes = %q, want committed remote bytes %q", got, stable)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets = %d, want 1", got)
	}
}

func TestReadSQLitePersistentJournalUsesCompleteDirtyRangeBeforeCommittedCache(t *testing.T) {
	const filePath = "/workload.db-wal"
	committed := bytes.Repeat([]byte{0x5a}, 4096)
	dirty := bytes.Repeat([]byte{0x7c}, 4096)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted when same-mount WAL bytes are available", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(filePath, 7)
	fs.readCache.Put(filePath, committed, 7)
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirty)))
	fs.openHandles.Add(writer)

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, len(committed))
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(committed)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, dirty) {
		t.Fatalf("read bytes = %q, want complete dirty WAL bytes", got)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalUsesCompleteDirtyRangeBeyondCommittedCache(t *testing.T) {
	const filePath = "/workload.db-wal"
	committed := bytes.Repeat([]byte{0x5a}, 4096)
	dirty := append(bytes.Repeat([]byte{0x5a}, 4096), bytes.Repeat([]byte{0x7c}, 4096)...)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted when complete same-mount WAL bytes are available", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(filePath, 7)
	fs.readCache.Put(filePath, committed, 7)
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirty)))
	fs.openHandles.Add(writer)

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 4096)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   4096,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if want := dirty[4096:]; !bytes.Equal(got, want) {
		t.Fatalf("read bytes = %x, want complete dirty WAL range %x", got[:4], want[:4])
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalDirtyRangeRequiresFullRequest(t *testing.T) {
	const filePath = "/workload.db-wal"
	dirty := append(bytes.Repeat([]byte{0x5a}, 4096), bytes.Repeat([]byte{0x7c}, 512)...)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be called by helper", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(dirty)), time.Now())
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirty)))
	fs.openHandles.Add(writer)

	if _, n, ok, st := fs.readSQLitePersistentJournalDirtyRange(filePath, nil, 4096, 4096); ok || st != gofuse.OK || n != 0 {
		t.Fatalf("dirty range claimed=%t n=%d status=%v, want no claim/OK", ok, n, st)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalWaitsForCompleteDirtyRangeBeforeCommittedCache(t *testing.T) {
	const filePath = "/workload.db-wal"
	committed := bytes.Repeat([]byte{0x5a}, 4096)
	dirtyFirst := bytes.Repeat([]byte{0x5a}, 4096)
	dirtySecond := bytes.Repeat([]byte{0x7c}, 4096)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted while same-mount WAL bytes are still completing", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(filePath, 7)
	fs.readCache.Put(filePath, committed, 7)
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirtyFirst); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirtyFirst)))
	fs.openHandles.Add(writer)

	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ready
		time.Sleep(10 * time.Millisecond)
		writer.Lock()
		defer writer.Unlock()
		if _, err := writer.Dirty.Write(int64(len(dirtyFirst)), dirtySecond); err != nil {
			t.Errorf("writer dirty append failed: %v", err)
			return
		}
		writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	}()
	close(ready)

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, len(dirtySecond))
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   uint64(len(dirtyFirst)),
		Size:     uint32(len(buf)),
	}, buf)
	<-done
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, dirtySecond) {
		t.Fatalf("read bytes = %x, want completed dirty WAL range %x", got[:4], dirtySecond[:4])
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalCleanWritableHandleUsesCompleteDirtyRangeBeforeCommittedCache(t *testing.T) {
	const filePath = "/workload.db-wal"
	committed := bytes.Repeat([]byte{0x6b}, 4096)
	dirty := bytes.Repeat([]byte{0x19}, 4096)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted when same-mount WAL bytes are available", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(filePath, 7)
	fs.readCache.Put(filePath, committed, 7)
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(dirty)))
	fs.openHandles.Add(writer)

	cleanBuffer := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if err := cleanBuffer.Truncate(0); err != nil {
		t.Fatal(err)
	}
	cleanBuffer.ClearDirty()
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    cleanBuffer,
		OrigSize: int64(len(committed)),
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, len(committed))
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(committed)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, dirty) {
		t.Fatalf("read bytes = %q, want complete dirty WAL bytes", got)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalEmptyShadowCreateUsesSiblingDirtyRange(t *testing.T) {
	const filePath = "/workload.db-wal"
	dirtyFirst := bytes.Repeat([]byte{0x1a}, 4096)
	dirtySecond := bytes.Repeat([]byte{0x2b}, 4096)
	dirty := append(dirtyFirst, dirtySecond...)
	var remoteCalls atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted when same-mount WAL bytes are available", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())

	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, dirty); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	fs.openHandles.Add(writer)

	if err := fs.shadowStore.Ensure(filePath, 0, 0); err != nil {
		t.Fatal(err)
	}
	emptyCreate := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Flags:       syscall.O_RDWR | syscall.O_CREAT,
		Dirty:       fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		IsNew:       true,
		ShadowReady: true,
		ShadowSpill: true,
	}
	emptyCreate.Dirty.touched = true
	emptyCreate.DirtySeq = fs.markDirtySize(ino, 0)
	readerID := fs.allocateFileHandle(emptyCreate)
	defer fs.deleteFileHandle(readerID, emptyCreate)

	buf := make([]byte, len(dirtySecond))
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   uint64(len(dirtyFirst)),
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, dirtySecond) {
		t.Fatalf("read bytes = %x, want sibling dirty WAL bytes %x", got[:4], dirtySecond[:4])
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestFlushSQLitePersistentJournalSkipsStaleEmptyCreateAfterSiblingCommit(t *testing.T) {
	const filePath = "/workload.db-wal"

	for _, tc := range []struct {
		name    string
		isNew   bool
		baseRev int64
	}{
		{name: "pending create not refreshed", isNew: true, baseRev: 0},
		{name: "pending create already refreshed", isNew: false, baseRev: 41},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var remoteCalls atomic.Int64

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				remoteCalls.Add(1)
				http.Error(w, "stale empty sidecar create must not overwrite committed WAL", http.StatusInternalServerError)
			}))
			defer ts.Close()

			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
			fs.recordCommittedRevision(filePath, 41)
			fs.inodes.UpdateRevision(ino, 41)

			staleCreate := &FileHandle{
				Ino:      ino,
				Path:     filePath,
				Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
				IsNew:    tc.isNew,
				OrigSize: 0,
				BaseRev:  tc.baseRev,
			}
			staleCreate.Dirty.touched = true
			staleCreate.DirtySeq = fs.markDirtySize(ino, 0)

			staleCreate.Lock()
			st := fs.flushHandle(context.Background(), staleCreate)
			staleCreate.Unlock()
			if st != gofuse.OK {
				t.Fatalf("flushHandle status = %v, want OK", st)
			}
			if got := remoteCalls.Load(); got != 0 {
				t.Fatalf("remote calls = %d, want 0", got)
			}
			if staleCreate.IsNew {
				t.Fatalf("stale empty create still marked new")
			}
			if staleCreate.BaseRev != 41 {
				t.Fatalf("BaseRev = %d, want adopted committed revision 41", staleCreate.BaseRev)
			}
			if staleCreate.DirtySeq != 0 {
				t.Fatalf("DirtySeq = %d, want cleared", staleCreate.DirtySeq)
			}
			if staleCreate.Dirty.HasDirtyParts() {
				t.Fatalf("dirty state still set after stale empty create was cleared")
			}
		})
	}
}

func TestFlushSQLitePersistentJournalUploadAllSeedsCommittedCache(t *testing.T) {
	const filePath = "/workload.db-wal"
	data := bytes.Repeat([]byte{0x42}, int(DefaultPartSize)+1024)
	expectedRevision := int64(0)
	rec := newMultipartUploadRecorder(t, filePath, int64(len(data)), &expectedRevision)

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(rec.client(), opts)
	fs.readCache = NewReadCacheWithMaxFileSize(defaultReadCacheMaxSize, defaultReadCacheTTL, int64(len(data)))
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	writer := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		Streamer: NewStreamUploader(rec.client(), filePath, expectedRevision),
		IsNew:    true,
	}
	if _, err := writer.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	writer.Lock()
	st := fs.flushHandle(context.Background(), writer)
	writer.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if rec.initiateCalls.Load() != 1 || rec.completeCalls.Load() != 1 {
		t.Fatalf("multipart calls = initiate:%d complete:%d, want 1 each", rec.initiateCalls.Load(), rec.completeCalls.Load())
	}

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)
	buf := make([]byte, len(data))
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(data)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read bytes len=%d want=%d equal=%t", len(got), len(data), bytes.Equal(got, data))
	}
	if got := rec.statCalls.Load(); got != 0 {
		t.Fatalf("remote stat calls during read = %d, want 0", got)
	}
}

func TestReadSQLitePersistentJournalCleanWritableHandleUsesRemoteCommittedBytes(t *testing.T) {
	const filePath = "/workload.db-wal"
	stale := []byte("stale preloaded wal bytes")
	committed := []byte("newer fsync committed wal bytes")
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			w.Header().Set("Content-Length", strconv.Itoa(len(committed)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "8")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			objectGets.Add(1)
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(committed)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(committed)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 8)
	cleanBuffer := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := cleanBuffer.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	cleanBuffer.ClearDirty()
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(stale)),
		BaseRev:  7,
		Dirty:    cleanBuffer,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(committed)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, committed) {
		t.Fatalf("read bytes = %q, want committed remote bytes %q", got, committed)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets = %d, want 1", got)
	}
}

func TestReadSQLitePersistentJournalCleanNewWritableHandleReturnsEOF(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for clean local-new sqlite sidecar", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	const filePath = "/workload.db-wal"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	cleanBuffer := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if err := cleanBuffer.Truncate(0); err != nil {
		t.Fatal(err)
	}
	cleanBuffer.ClearDirty()
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    cleanBuffer,
		IsNew:    true,
		OrigSize: 0,
		BaseRev:  0,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 16)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if len(got) != 0 {
		t.Fatalf("read bytes = %q, want EOF", got)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLitePersistentJournalReadOnlyHandleSeesOpenEmptySidecarEOF(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for open empty sqlite sidecar", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	const filePath = "/workload.db-wal"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	creator := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		IsNew:    true,
		OrigSize: 0,
		BaseRev:  0,
	}
	if err := creator.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	creator.DirtySeq = fs.markDirtySize(ino, 0)
	fs.openHandles.Add(creator)

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 16)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if len(got) != 0 {
		t.Fatalf("read bytes = %q, want EOF", got)
	}
	if gotCalls := remoteCalls.Load(); gotCalls != 0 {
		t.Fatalf("remote calls = %d, want 0", gotCalls)
	}
}

func TestReadSQLiteSamePathDirtyHandleBeforeShadowStore(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "remote should not be consulted for same-mount sqlite main-db dirty data", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	fs.shadowStore = shadow

	const filePath = "/workload.db"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	stale := []byte("stale main db bytes")
	if _, err := fs.shadowStore.WriteAt(filePath, 0, stale, 1); err != nil {
		t.Fatalf("WriteAt stale shadow: %v", err)
	}
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	want := []byte("latest main db bytes")
	if _, err := writer.Dirty.Write(0, want); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(want)))
	fs.openHandles.Add(writer)

	reader := &FileHandle{Ino: ino, Path: filePath}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes = %q, want latest dirty bytes %q", got, want)
	}
}

func TestUnlinkPreservesOpenReadHandleSnapshot(t *testing.T) {
	const filePath = "/unlink-open.bin"
	want := []byte("open handle must keep these bytes after unlink")
	var deleted atomic.Bool
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(want)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Location", ts.URL+"/object/unlink-open")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object/unlink-open":
			objectGets.Add(1)
			if deleted.Load() {
				http.Error(w, "object deleted", http.StatusGone)
				return
			}
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(want)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(want)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(want)), time.Now())
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(want)),
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after unlink snapshot = %d, want 1", got)
	}

	buf := make([]byte, 128)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes after unlink = %q, want %q", got, want)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after open-handle read = %d, want still 1", got)
	}
}

func TestUnlinkPreservesLargeOpenReadHandleSnapshotInShadow(t *testing.T) {
	const filePath = "/large-unlink-open.bin"
	const revision = int64(7)
	const fileSize = maxPathTruncateInMemoryBytes + 1

	patternByte := func(offset int64) byte {
		return byte(offset % 251)
	}
	writePattern := func(w io.Writer, start, end int64) error {
		buf := make([]byte, 32*1024)
		for pos := start; pos <= end; {
			n := len(buf)
			remaining := int(end - pos + 1)
			if remaining < n {
				n = remaining
			}
			for i := 0; i < n; i++ {
				buf[i] = patternByte(pos + int64(i))
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return err
			}
			pos += int64(n)
		}
		return nil
	}
	expectedPattern := func(start int64, n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = patternByte(start + int64(i))
		}
		return out
	}

	var deleted atomic.Bool
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Location", ts.URL+"/object/large-unlink-open")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object/large-unlink-open":
			objectGets.Add(1)
			if deleted.Load() {
				http.Error(w, "object deleted", http.StatusGone)
				return
			}
			start := int64(0)
			end := fileSize - 1
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
				parsedStart, parsedEnd, ok := parseTestBytesRange(rangeHeader)
				if !ok || parsedEnd >= fileSize {
					http.Error(w, "wrong range: "+rangeHeader, http.StatusRequestedRangeNotSatisfiable)
					return
				}
				start, end = parsedStart, parsedEnd
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
				w.WriteHeader(http.StatusPartialContent)
			} else {
				w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
				w.WriteHeader(http.StatusOK)
			}
			if err := writePattern(w, start, end); err != nil {
				t.Errorf("write object pattern: %v", err)
			}
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	ino := fs.inodes.Lookup(filePath, false, fileSize, time.Now())
	fs.inodes.UpdateRevision(ino, revision)
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: fileSize,
		BaseRev:  revision,
	}
	readerID := fs.allocateFileHandle(reader)
	released := false
	defer func() {
		if !released {
			fs.Release(nil, &gofuse.ReleaseIn{Fh: readerID})
		}
	}()

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}
	reader.Lock()
	unlinkedShadowGen := reader.UnlinkedShadowGen
	unlinkedSize := reader.UnlinkedSize
	reader.Unlock()
	if unlinkedShadowGen == 0 {
		t.Fatal("large open-unlink handle did not pin a shadow snapshot")
	}
	if unlinkedSize != fileSize {
		t.Fatalf("unlinked size = %d, want %d", unlinkedSize, fileSize)
	}
	if got := shadow.SizeGen(unlinkedShadowGen); got != fileSize {
		t.Fatalf("shadow generation size = %d, want %d", got, fileSize)
	}
	snapshotGets := objectGets.Load()
	if snapshotGets != 1 {
		t.Fatalf("object gets after unlink snapshot = %d, want 1", snapshotGets)
	}

	const readSize = 64
	offset := fileSize - readSize
	buf := make([]byte, readSize)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   uint64(offset),
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if want := expectedPattern(offset, readSize); !bytes.Equal(got, want) {
		t.Fatalf("read bytes after unlink = %v, want %v", got, want)
	}
	if got := objectGets.Load(); got != snapshotGets {
		t.Fatalf("object gets after open-handle read = %d, want still %d", got, snapshotGets)
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: readerID})
	released = true
	if got := shadow.SizeGen(unlinkedShadowGen); got != -1 {
		t.Fatalf("shadow generation size after release = %d, want -1", got)
	}
}

func TestRenameReplacePreservesOpenDestinationSnapshotAndSourceHandle(t *testing.T) {
	const oldPath = "/replace-source.bin"
	const newPath = "/work.bin"
	oldDst := []byte("destination bytes that an existing open fh must retain")
	replacement := []byte("replacement bytes from the renamed source")
	var renamed atomic.Bool
	var oldDstObjectGets atomic.Int64
	var replacementPathGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+newPath:
			if renamed.Load() {
				replacementPathGets.Add(1)
				_, _ = w.Write(replacement)
				return
			}
			w.Header().Set("Location", ts.URL+"/object/old-dst")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object/old-dst":
			oldDstObjectGets.Add(1)
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(oldDst)-1) {
				http.Error(w, "wrong old destination range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(oldDst)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newPath && r.URL.RawQuery == "rename":
			if got := r.Header.Get("X-Dat9-Rename-Source"); got != oldPath {
				http.Error(w, "wrong rename source: "+got, http.StatusBadRequest)
				return
			}
			renamed.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.Lookup(oldPath, false, int64(len(replacement)), time.Now())
	dstIno := fs.inodes.Lookup(newPath, false, int64(len(oldDst)), time.Now())
	sourceHandle := &FileHandle{
		Ino:      srcIno,
		Path:     oldPath,
		OrigSize: int64(len(replacement)),
		BaseRev:  11,
	}
	sourceID := fs.allocateFileHandle(sourceHandle)
	defer fs.deleteFileHandle(sourceID, sourceHandle)
	dstHandle := &FileHandle{
		Ino:      dstIno,
		Path:     newPath,
		OrigSize: int64(len(oldDst)),
		BaseRev:  7,
	}
	dstID := fs.allocateFileHandle(dstHandle)
	defer fs.deleteFileHandle(dstID, dstHandle)

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, strings.TrimPrefix(oldPath, "/"), strings.TrimPrefix(newPath, "/"))
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}
	if !renamed.Load() {
		t.Fatal("remote rename was not called")
	}
	if sourceHandle.Path != newPath {
		t.Fatalf("source handle path = %q, want %q", sourceHandle.Path, newPath)
	}
	if got := oldDstObjectGets.Load(); got != 1 {
		t.Fatalf("old destination object gets after replacement snapshot = %d, want 1", got)
	}

	buf := make([]byte, 128)
	dstResult, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dstIno},
		Fh:       dstID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("destination Read status = %v, want OK", st)
	}
	gotDst, st := dstResult.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("destination result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(gotDst, oldDst) {
		t.Fatalf("destination handle read bytes = %q, want original %q", gotDst, oldDst)
	}
	if got := oldDstObjectGets.Load(); got != 1 {
		t.Fatalf("old destination object gets after destination handle read = %d, want still 1", got)
	}

	sourceResult, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: srcIno},
		Fh:       sourceID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("source Read status = %v, want OK", st)
	}
	gotSource, st := sourceResult.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("source result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(gotSource, replacement) {
		t.Fatalf("source handle read bytes = %q, want replacement %q", gotSource, replacement)
	}
	if got := replacementPathGets.Load(); got != 1 {
		t.Fatalf("replacement path gets = %d, want 1", got)
	}
}

func TestUnlinkSQLiteWALPreservesOpenReadHandle(t *testing.T) {
	const filePath = "/workload.db-wal"
	want := []byte("sqlite wal snapshot bytes")
	var deleted atomic.Bool
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(want)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			objectGets.Add(1)
			if deleted.Load() {
				http.Error(w, "object deleted", http.StatusGone)
				return
			}
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(want)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(want)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(want)), time.Now())
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(want)),
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after unlink snapshot = %d, want 1", got)
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes after unlink = %q, want %q", got, want)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after open-handle read = %d, want still 1", got)
	}
}

func TestUnlinkSQLiteWALPreservesCleanWritableReadHandle(t *testing.T) {
	const filePath = "/workload.db-wal"
	want := []byte("sqlite wal snapshot bytes")
	var deleted atomic.Bool
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(want)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			objectGets.Add(1)
			if deleted.Load() {
				http.Error(w, "object deleted", http.StatusGone)
				return
			}
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(want)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(want)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(want)), time.Now())
	cleanDirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	cleanDirty.totalSize = int64(len(want))
	cleanDirty.remoteSize = int64(len(want))
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(want)),
		BaseRev:  7,
		Dirty:    cleanDirty,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after unlink snapshot = %d, want 1", got)
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes after unlink = %q, want %q", got, want)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after open-handle read = %d, want still 1", got)
	}
}

func TestUnlinkSQLiteWALPreservesEmptyOpenReadHandle(t *testing.T) {
	const filePath = "/workload.db-wal"
	var deleted atomic.Bool
	var remoteReads atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && (r.URL.Path == "/v1/fs"+filePath || r.URL.Path == "/object"):
			remoteReads.Add(1)
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+filePath:
			remoteReads.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: 0,
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if len(got) != 0 {
		t.Fatalf("read bytes after empty unlink = %q, want EOF", got)
	}
	if got := remoteReads.Load(); got != 0 {
		t.Fatalf("remote reads after empty unlink snapshot = %d, want 0", got)
	}
}

func TestUnlinkSQLiteWALUsesLatestInodeSizeForStaleOpenReadHandle(t *testing.T) {
	const filePath = "/workload.db-wal"
	want := []byte("sqlite wal bytes after reader opened")
	var deleted atomic.Bool
	var objectGets atomic.Int64

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+filePath:
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Location", ts.URL+"/object")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object":
			objectGets.Add(1)
			if deleted.Load() {
				http.Error(w, "object deleted", http.StatusGone)
				return
			}
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(want)-1) {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(want)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+filePath:
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	fs.inodes.UpdateSize(ino, int64(len(want)))
	fs.inodes.UpdateRevision(ino, 7)
	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: 0,
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, strings.TrimPrefix(filePath, "/")); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if !deleted.Load() {
		t.Fatal("remote delete was not called")
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after unlink snapshot = %d, want 1", got)
	}

	buf := make([]byte, 64)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes after stale-size unlink = %q, want %q", got, want)
	}
	if got := objectGets.Load(); got != 1 {
		t.Fatalf("object gets after open-handle read = %d, want still 1", got)
	}
}

func TestFtruncateSQLiteWALPreservesOpenReaderSnapshot(t *testing.T) {
	const filePath = "/workload.db-wal"
	want := []byte("sqlite wal bytes before checkpoint truncate")
	inFlight := []byte("sqlite wal in-flight writer bytes that are not fsync committed")
	var remoteReads atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			remoteReads.Add(1)
		}
		http.Error(w, "remote should not be consulted for same-mount sqlite wal snapshot", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(want)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision(filePath, 7)
	fs.readCache.Put(filePath, want, 7)

	reader := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(want)),
		BaseRev:  7,
	}
	readerID := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(readerID, reader)

	writer := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		OrigSize: int64(len(want)),
		BaseRev:  7,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	if _, err := writer.Dirty.Write(0, inFlight); err != nil {
		t.Fatalf("writer dirty write: %v", err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	writerID := fs.allocateFileHandle(writer)
	defer fs.deleteFileHandle(writerID, writer)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       writerID,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr ftruncate status = %v, want OK", st)
	}

	buf := make([]byte, 128)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       readerID,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, st := result.Bytes(buf)
	if st != gofuse.OK {
		t.Fatalf("result.Bytes status = %v, want OK", st)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("reader bytes after ftruncate = %q, want %q", got, want)
	}
	if got := remoteReads.Load(); got != 0 {
		t.Fatalf("remote reads = %d, want 0", got)
	}
}

func TestReadSQLiteSamePathDirtyHandleSkipsLockedCandidate(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)

	const filePath = "/workload.db"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	want := []byte("locked writer bytes")
	if _, err := writer.Dirty.Write(0, want); err != nil {
		t.Fatal(err)
	}
	writer.DirtySeq = fs.markDirtySize(ino, int64(len(want)))
	fs.openHandles.Add(writer)

	writer.Lock()
	done := make(chan struct{})
	var (
		gotOK bool
		gotSt gofuse.Status
	)
	go func() {
		defer close(done)
		_, _, gotOK, gotSt = fs.readSamePathDirtyHandle(filePath, nil, 0, 64)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("read blocked on locked same-path dirty writer")
	}
	writer.Unlock()
	if gotOK {
		t.Fatal("locked same-path dirty candidate should not claim the read")
	}
	if gotSt != gofuse.OK {
		t.Fatalf("status = %v, want OK", gotSt)
	}
}

func TestReadSQLiteSamePathDirtyHandleSkipsIncompleteCandidate(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)

	const filePath = "/workload.db"
	ino := fs.inodes.Lookup(filePath, false, 4096, time.Now())
	writer := &FileHandle{
		Ino:   ino,
		Path:  filePath,
		Dirty: fs.newWriteBuffer(filePath, maxPreloadSize, 0),
	}
	writer.Dirty.totalSize = 4096
	writer.Dirty.remoteSize = 4096
	writer.Dirty.LoadPart = func(partNumber int) ([]byte, error) {
		return nil, errors.New("remote clean range unavailable")
	}
	writer.DirtySeq = 1
	fs.openHandles.Add(writer)

	_, _, ok, st := fs.readSamePathDirtyHandle(filePath, nil, 0, 128)
	if ok {
		t.Fatal("incomplete same-path dirty candidate should not claim the read")
	}
	if st != gofuse.OK {
		t.Fatalf("status = %v, want OK", st)
	}
}

func TestReadSQLiteSamePathDirtyShadowEOFIsShortRead(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "remote should not be consulted for same-mount sqlite shadow data", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewShadowStore: %v", err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	const filePath = "/workload.db"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	want := []byte("latest main db bytes")
	if _, err := fs.shadowStore.WriteAt(filePath, 0, want, 1); err != nil {
		t.Fatalf("WriteAt shadow: %v", err)
	}
	writer := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		ShadowReady: true,
	}
	writer.Dirty.totalSize = int64(len(want) + 16)
	writer.DirtySeq = fs.markDirtySize(ino, writer.Dirty.Size())
	fs.openHandles.Add(writer)

	got, n, ok, st := fs.readSamePathDirtyHandle(filePath, nil, 0, uint32(len(want)+16))
	if !ok {
		t.Fatal("same-path shadow candidate should claim the read")
	}
	if st != gofuse.OK {
		t.Fatalf("status = %v, want OK", st)
	}
	if n != len(want) {
		t.Fatalf("bytes read = %d, want %d", n, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read bytes = %q, want %q", got, want)
	}
}

func TestOpenWritablePreloadPreservesExistingOrigSize(t *testing.T) {
	var remoteCalls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "remote should not be consulted for cached existing file", http.StatusNotFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/grow.bin", false, 1, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.readCache.Put("/grow.bin", []byte("a"), 7)

	var firstOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDWR),
	}, &firstOut)
	if st != gofuse.OK {
		t.Fatalf("first Open status = %v, want OK", st)
	}
	want := bytes.Repeat([]byte("x"), defaultSmallFileThreshold+1024)
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       firstOut.Fh,
		Offset:   0,
	}, want); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}

	var secondOut gofuse.OpenOut
	st = fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_WRONLY),
	}, &secondOut)
	if st != gofuse.OK {
		t.Fatalf("second Open status = %v, want OK", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
	reopened, ok := fs.fileHandles.Get(secondOut.Fh)
	if !ok {
		t.Fatal("reopened handle not found")
	}
	reopened.Lock()
	origSize := reopened.OrigSize
	got := reopened.Dirty.Bytes()
	reopened.Unlock()
	if origSize != 1 {
		t.Fatalf("reopened OrigSize = %d, want original remote size 1", origSize)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("reopened handle did not preload local grown bytes")
	}
}

func TestCommitQueueCleanupRemovesForgottenPendingInode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

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

	const p = "/config.lock"
	ino := fs.inodes.Lookup(p, false, 7, time.Now())
	if err := shadow.WriteFull(p, []byte("config\n"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(p, 7, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	fs.Forget(ino, 1)
	if _, ok := fs.inodes.GetPath(ino); !ok {
		t.Fatal("Forget removed pending inode mapping; chmod/rename may see ENOENT")
	}

	pending.Remove(p)
	shadow.Remove(p)
	fs.onCommitQueueCleanup(&CommitEntry{Path: p, Inode: ino})
	if _, ok := fs.inodes.GetPath(ino); ok {
		t.Fatal("commit cleanup left forgotten inode mapping after local state was removed")
	}
}

func TestForgetPreservesQueuedCommitInodeUntilCleanup(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	const p = "/config.lock"
	ino := fs.inodes.Lookup(p, false, 7, time.Now())
	fs.commitQueue = &CommitQueue{
		queue:    []*CommitEntry{{Path: p, Inode: ino}},
		inFlight: map[string]*CommitEntry{},
	}

	fs.Forget(ino, 1)
	if _, ok := fs.inodes.GetPath(ino); !ok {
		t.Fatal("Forget removed queued commit inode mapping before commit cleanup")
	}

	fs.commitQueue.queue = nil
	fs.onCommitQueueCleanup(&CommitEntry{Path: p, Inode: ino})
	if _, ok := fs.inodes.GetPath(ino); ok {
		t.Fatal("commit cleanup left queued inode mapping after queue/local state cleared")
	}
}

func TestReleaseCleansForgottenInodeWithoutLocalState(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	const p = "/config.lock"
	ino := fs.inodes.Lookup(p, false, 7, time.Now())

	var openOut gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	fs.Forget(ino, 1)
	if _, ok := fs.inodes.GetPath(ino); !ok {
		t.Fatal("Forget removed inode mapping while file handle was still open")
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	if _, ok := fs.fileHandles.Get(openOut.Fh); ok {
		t.Fatal("Release left closed file handle in handle table")
	}
	if _, ok := fs.inodes.GetPath(ino); ok {
		t.Fatal("Release cleanup left forgotten inode mapping after local state was gone")
	}
}

func TestStatFs_ReportsVirtualCapacity(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

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

func TestXAttr_GetMissingReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	_, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: 1}, "user.test", nil)
	if st != gofuse.ENOATTR {
		t.Fatalf("GetXAttr status = %v, want ENOATTR", st)
	}
}

func TestXAttr_SetGetRoundTrip(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	st := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.test", []byte("hello"))
	if st != gofuse.OK {
		t.Fatalf("SetXAttr status = %v, want OK", st)
	}

	n, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", nil)
	if st != gofuse.OK {
		t.Fatalf("GetXAttr status = %v, want OK", st)
	}
	if n != 5 {
		t.Fatalf("GetXAttr size = %d, want 5", n)
	}

	dest := make([]byte, 16)
	n, st = fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", dest)
	if st != gofuse.OK {
		t.Fatalf("GetXAttr status = %v, want OK", st)
	}
	if string(dest[:n]) != "hello" {
		t.Fatalf("GetXAttr value = %q, want %q", string(dest[:n]), "hello")
	}
}

func TestXAttr_ListReturnsSetAttrs(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.foo", []byte("1"))
	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.bar", []byte("2"))

	n, st := fs.ListXAttr(nil, &gofuse.InHeader{NodeId: ino}, nil)
	if st != gofuse.OK {
		t.Fatalf("ListXAttr status = %v, want OK", st)
	}
	// Two names, each null-terminated.
	expectedSize := len("user.foo") + 1 + len("user.bar") + 1
	if n != uint32(expectedSize) {
		t.Fatalf("ListXAttr size = %d, want %d", n, expectedSize)
	}

	dest := make([]byte, expectedSize)
	n, st = fs.ListXAttr(nil, &gofuse.InHeader{NodeId: ino}, dest)
	if st != gofuse.OK {
		t.Fatalf("ListXAttr status = %v, want OK", st)
	}
	// Verify both names appear in the null-separated output.
	names := strings.Split(strings.TrimRight(string(dest[:n]), "\x00"), "\x00")
	if !contains(names, "user.foo") || !contains(names, "user.bar") {
		t.Fatalf("ListXAttr names = %v, want both user.foo and user.bar", names)
	}
}

func TestXAttr_RemoveWorks(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.test", []byte("val"))

	st := fs.RemoveXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test")
	if st != gofuse.OK {
		t.Fatalf("RemoveXAttr status = %v, want OK", st)
	}

	_, st = fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", nil)
	if st != gofuse.ENOATTR {
		t.Fatalf("GetXAttr after remove status = %v, want ENOATTR", st)
	}
}

func TestXAttr_RemoveMissingReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	st := fs.RemoveXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test")
	if st != gofuse.ENOATTR {
		t.Fatalf("RemoveXAttr status = %v, want ENOATTR", st)
	}
}

func TestXAttr_GetReturnsERANGEWhenDestTooSmall(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.test", []byte("hello world"))

	dest := make([]byte, 3)
	n, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", dest)
	if st != gofuse.Status(syscall.ERANGE) {
		t.Fatalf("GetXAttr status = %v, want ERANGE", st)
	}
	if n != 11 {
		t.Fatalf("GetXAttr size = %d, want 11", n)
	}
}

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func TestXAttr_RenameMigratesXattrs(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/old.txt", false, 0, time.Now())
	oldIno, _ := fs.inodes.GetInode("/old.txt")

	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: oldIno}}, "user.test", []byte("hello"))

	// Rename the file — xattrs should migrate to the new path.
	fs.xattrs.Rename("/old.txt", "/new.txt")

	// Old path should have no xattrs.
	_, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: oldIno}, "user.test", nil)
	if st != gofuse.ENOATTR {
		t.Errorf("GetXAttr on old path status = %v, want ENOATTR", st)
	}

	// New path should have the xattr.
	fs.inodes.Lookup("/new.txt", false, 0, time.Now())
	newIno, _ := fs.inodes.GetInode("/new.txt")
	n, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: newIno}, "user.test", nil)
	if st != gofuse.OK {
		t.Errorf("GetXAttr on new path status = %v, want OK", st)
	}
	if n != 5 {
		t.Errorf("GetXAttr on new path size = %d, want 5", n)
	}
}

func TestXAttr_SetXAttrCreateFlagFailsIfExists(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	// Set initial value.
	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.test", []byte("first"))

	// XATTR_CREATE should fail (EEXIST) since attr already exists.
	st := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: 1}, "user.test", []byte("second"))
	if st != gofuse.Status(syscall.EEXIST) {
		t.Errorf("SetXAttr with XATTR_CREATE on existing attr status = %v, want EEXIST", st)
	}

	// Value should be unchanged.
	dest := make([]byte, 16)
	n, _ := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", dest)
	if string(dest[:n]) != "first" {
		t.Errorf("value after failed CREATE = %q, want 'first'", string(dest[:n]))
	}
}

func TestXAttr_SetXAttrReplaceFlagFailsIfMissing(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	fs.inodes.Lookup("/test.txt", false, 0, time.Now())
	ino, _ := fs.inodes.GetInode("/test.txt")

	// XATTR_REPLACE should fail (ENODATA) since attr doesn't exist.
	st := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: 2}, "user.test", []byte("val"))
	if st != gofuse.Status(syscall.ENODATA) {
		t.Errorf("SetXAttr with XATTR_REPLACE on missing attr status = %v, want ENODATA", st)
	}

	// Set then replace — should succeed.
	_ = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, "user.test", []byte("first"))
	st = fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: 2}, "user.test", []byte("replaced"))
	if st != gofuse.OK {
		t.Errorf("SetXAttr with XATTR_REPLACE on existing attr status = %v, want OK", st)
	}
	dest := make([]byte, 16)
	n, _ := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: ino}, "user.test", dest)
	if string(dest[:n]) != "replaced" {
		t.Errorf("value after REPLACE = %q, want 'replaced'", string(dest[:n]))
	}
}

func TestXAttr_RemoveAllCleansChildren(t *testing.T) {
	store := NewXAttrStore()
	store.Set("/dir", "user.dir", []byte("d"))
	store.Set("/dir/file.txt", "user.file", []byte("f"))
	store.Set("/dir/sub/inner.txt", "user.inner", []byte("i"))

	// RemoveAll on "/dir" should clear itself and all children.
	store.RemoveAll("/dir")

	if _, ok := store.Get("/dir", "user.dir"); ok {
		t.Errorf("xattr on /dir still exists after RemoveAll")
	}
	if _, ok := store.Get("/dir/file.txt", "user.file"); ok {
		t.Errorf("xattr on /dir/file.txt still exists after RemoveAll")
	}
	if _, ok := store.Get("/dir/sub/inner.txt", "user.inner"); ok {
		t.Errorf("xattr on /dir/sub/inner.txt still exists after RemoveAll")
	}
}

func TestXAttr_RenameMigratesSubtree(t *testing.T) {
	store := NewXAttrStore()
	store.Set("/old", "user.top", []byte("t"))
	store.Set("/old/child.txt", "user.child", []byte("c"))
	store.Set("/old/sub/grand.txt", "user.grand", []byte("g"))

	store.Rename("/old", "/new")

	if _, ok := store.Get("/old", "user.top"); ok {
		t.Errorf("xattr on /old still exists after Rename")
	}
	if _, ok := store.Get("/old/child.txt", "user.child"); ok {
		t.Errorf("xattr on /old/child.txt still exists after Rename")
	}
	if v, ok := store.Get("/new", "user.top"); !ok || string(v) != "t" {
		t.Errorf("xattr not migrated to /new")
	}
	if v, ok := store.Get("/new/child.txt", "user.child"); !ok || string(v) != "c" {
		t.Errorf("xattr not migrated to /new/child.txt")
	}
	if v, ok := store.Get("/new/sub/grand.txt", "user.grand"); !ok || string(v) != "g" {
		t.Errorf("xattr not migrated to /new/sub/grand.txt")
	}
}

func TestXAttr_RameNoOpPreservesXattrs(t *testing.T) {
	// Rename to the same path should be a no-op — xattrs must survive.
	store := NewXAttrStore()
	store.Set("/file.txt", "user.test", []byte("hello"))

	store.Rename("/file.txt", "/file.txt")

	if v, ok := store.Get("/file.txt", "user.test"); !ok || string(v) != "hello" {
		t.Errorf("xattr lost after no-op Rename: ok=%v val=%q", ok, v)
	}
}

func TestXAttr_RenameOverExistingClearsDestinationXattrs(t *testing.T) {
	// POSIX rename replaces the destination object. Stale xattrs from the
	// old destination must not remain visible on the new object.
	store := NewXAttrStore()
	// Destination has xattrs; source does not.
	store.Set("/dst", "user.old", []byte("stale"))
	store.Set("/dst/child.txt", "user.child", []byte("stale_child"))

	// Rename /src -> /dst (source has no xattrs).
	store.Rename("/src", "/dst")

	// /dst should have NO xattrs (source had none, destination's were cleared).
	if _, ok := store.Get("/dst", "user.old"); ok {
		t.Errorf("stale xattr on /dst survived rename-over-existing")
	}
	// /dst/child.txt should also be gone (destination subtree cleared).
	if _, ok := store.Get("/dst/child.txt", "user.child"); ok {
		t.Errorf("stale xattr on /dst/child.txt survived rename-over-existing")
	}
}

func TestXAttr_RenameOverExistingReplacesDestinationXattrs(t *testing.T) {
	// Both source and destination have xattrs — source wins.
	store := NewXAttrStore()
	store.Set("/dst", "user.old", []byte("stale"))
	store.Set("/src", "user.new", []byte("fresh"))

	store.Rename("/src", "/dst")

	// Source's xattr should be on destination.
	if v, ok := store.Get("/dst", "user.new"); !ok || string(v) != "fresh" {
		t.Errorf("source xattr not migrated to destination: ok=%v val=%q", ok, v)
	}
	// Old destination xattr should be gone.
	if _, ok := store.Get("/dst", "user.old"); ok {
		t.Errorf("stale destination xattr survived rename-over-existing")
	}
	// Old source path should be empty.
	if _, ok := store.Get("/src", "user.new"); ok {
		t.Errorf("xattr still on old source path after rename")
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

func TestSetAttr_AtimeUpdate(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	atime := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_ATIME,
			Atime:    uint64(atime.Unix()),
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if !entry.Atime.Equal(atime) {
		t.Fatalf("Atime = %v, want %v", entry.Atime, atime)
	}
	if out.Atime != uint64(atime.Unix()) {
		t.Fatalf("out.Atime = %d, want %d", out.Atime, atime.Unix())
	}
}

func TestSetAttr_OwnerUpdate(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_UID | gofuse.FATTR_GID,
			Owner:    gofuse.Owner{Uid: 1234, Gid: 5678},
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if !entry.HasUID || !entry.HasGID || entry.Uid != 1234 || entry.Gid != 5678 {
		t.Fatalf("owner = uid:%d/%t gid:%d/%t, want 1234/true 5678/true", entry.Uid, entry.HasUID, entry.Gid, entry.HasGID)
	}
	if out.Uid != 1234 || out.Gid != 5678 {
		t.Fatalf("out owner = %d:%d, want 1234:5678", out.Uid, out.Gid)
	}
}

func TestAccess_ChecksModeAndOwner(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	fs.inodes.UpdateMode(ino, 0o640)
	fs.inodes.UpdateOwner(ino, 1001, 1002, true, true)

	tests := []struct {
		name   string
		uid    uint32
		gid    uint32
		mask   uint32
		status gofuse.Status
	}{
		{name: "owner read write", uid: 1001, gid: 9000, mask: gofuse.R_OK | gofuse.W_OK, status: gofuse.OK},
		{name: "owner missing execute", uid: 1001, gid: 9000, mask: gofuse.R_OK | gofuse.X_OK, status: gofuse.EACCES},
		{name: "group read", uid: 9001, gid: 1002, mask: gofuse.R_OK, status: gofuse.OK},
		{name: "group missing write", uid: 9001, gid: 1002, mask: gofuse.W_OK, status: gofuse.EACCES},
		{name: "other missing read", uid: 9001, gid: 9002, mask: gofuse.R_OK, status: gofuse.EACCES},
		{name: "f ok", uid: 9001, gid: 9002, mask: gofuse.F_OK, status: gofuse.OK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := fs.Access(nil, &gofuse.AccessIn{
				InHeader: gofuse.InHeader{
					NodeId: ino,
					Caller: gofuse.Caller{
						Owner: gofuse.Owner{Uid: tt.uid, Gid: tt.gid},
					},
				},
				Mask: tt.mask,
			})
			if st != tt.status {
				t.Fatalf("Access status = %v, want %v", st, tt.status)
			}
		})
	}
}

func TestAccess_MissingInode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	st := fs.Access(nil, &gofuse.AccessIn{
		InHeader: gofuse.InHeader{NodeId: 999},
		Mask:     gofuse.F_OK,
	})
	if st != gofuse.ENOENT {
		t.Fatalf("Access status = %v, want ENOENT", st)
	}
}

func TestOpen_ChecksModeAndOwner(t *testing.T) {
	fs, ino, cleanup := newTestDat9FS(t, 42, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 42))
	})
	defer cleanup()

	fs.inodes.UpdateOwner(ino, 1001, 1002, true, true)

	tests := []struct {
		name   string
		mode   uint32
		uid    uint32
		gid    uint32
		flags  uint32
		status gofuse.Status
	}{
		{name: "owner write denied", mode: 0o400, uid: 1001, gid: 1002, flags: uint32(syscall.O_WRONLY), status: gofuse.EACCES},
		{name: "owner read denied", mode: 0o200, uid: 1001, gid: 1002, flags: uint32(syscall.O_RDONLY), status: gofuse.EACCES},
		{name: "other read write denied", mode: 0o600, uid: 65534, gid: 65534, flags: uint32(syscall.O_RDWR), status: gofuse.EACCES},
		{name: "read truncate needs write", mode: 0o400, uid: 1001, gid: 1002, flags: uint32(syscall.O_RDONLY | syscall.O_TRUNC), status: gofuse.EACCES},
		{name: "owner read allowed", mode: 0o400, uid: 1001, gid: 1002, flags: uint32(syscall.O_RDONLY), status: gofuse.OK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|tt.mode)
			var out gofuse.OpenOut
			st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{
					NodeId: ino,
					Caller: gofuse.Caller{
						Owner: gofuse.Owner{Uid: tt.uid, Gid: tt.gid},
					},
				},
				Flags: tt.flags,
			}, &out)
			if st != tt.status {
				t.Fatalf("Open status = %v, want %v", st, tt.status)
			}
			if st == gofuse.OK {
				fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
			}
		})
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/file.bin", false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	var attrOut gofuse.AttrOut
	before := time.Now()
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	after := time.Now()
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
	if entry.Ctime.Before(before) || entry.Ctime.After(after) {
		t.Fatalf("inode ctime = %v, expected between %v and %v", entry.Ctime, before, after)
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

func TestSetAttr_WriteBackInteractivePathTruncateStagesAndReturnsBeforeRemoteCommit(t *testing.T) {
	putStarted := make(chan struct{})
	releasePut := make(chan struct{})
	var putOnce sync.Once
	var putBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			putBody = append([]byte(nil), body...)
			putOnce.Do(func() { close(putStarted) })
			<-releasePut
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq
	defer cq.DrainAll()

	ino := fs.inodes.Lookup("/file.bin", false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	done := make(chan gofuse.Status, 1)
	go func() {
		var out gofuse.AttrOut
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{
			SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: ino},
				Valid:    gofuse.FATTR_SIZE,
				Size:     0,
			},
		}, &out)
	}()

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("SetAttr status = %v, want OK", st)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SetAttr blocked on remote zero-byte commit")
	}

	meta, ok := pending.GetMeta("/file.bin")
	if !ok {
		t.Fatal("pending truncate metadata missing")
	}
	if meta.Size != 0 || meta.Kind != PendingOverwrite || meta.BaseRev != 1 {
		t.Fatalf("pending meta = %+v, want zero-byte overwrite at base rev 1", meta)
	}
	if !shadow.Has("/file.bin") {
		t.Fatal("shadow missing for staged truncate")
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	if entry.Size != 0 {
		t.Fatalf("inode size = %d, want 0", entry.Size)
	}

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("async zero-byte commit did not start")
	}
	close(releasePut)
	cq.DrainAll()
	if len(putBody) != 0 {
		t.Fatalf("remote truncate body = %q, want empty", string(putBody))
	}
	if pending.HasPending("/file.bin") {
		t.Fatal("pending truncate still present after commit")
	}
	if shadow.Has("/file.bin") {
		t.Fatal("shadow still present after commit")
	}
}

func TestSetAttr_WriteBackPathTruncateAdoptsSingleCallerWriter(t *testing.T) {
	const callerPID = 5151
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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
	fs.commitQueue = &CommitQueue{
		maxPending:   8,
		queue:        []*CommitEntry{},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
		workCh:       make(chan *CommitEntry, 16),
	}

	const path = "/tier-transition.bin"
	const baseRev int64 = 7
	ino := fs.inodes.Lookup(path, false, 8*1024*1024, time.Now())
	fs.inodes.UpdateRevision(ino, baseRev)
	fs.inodes.UpdateSize(ino, 8*1024*1024)

	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		Flags:       uint32(syscall.O_WRONLY),
		OpenPID:     callerPID,
		Dirty:       fs.newWriteBuffer(path, maxPreloadSize, 0),
		OrigSize:    8 * 1024 * 1024,
		BaseRev:     baseRev,
		WritePolicy: WritePolicyWriteBack,
	}
	if err := fh.Dirty.Truncate(8 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fhID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(fhID, fh)

	var attrOut gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: callerPID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if !fh.ZeroBase {
		t.Fatal("same-caller writer did not adopt zero base")
	}
	if got := fh.Dirty.Size(); got != 0 {
		t.Fatalf("dirty size after staged truncate = %d, want 0", got)
	}
	if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("dirty truncate not marked: dirtySeq=%d dirty=%t", fh.DirtySeq, fh.Dirty.HasDirtyParts())
	}

	finalData := bytes.Repeat([]byte{0x59}, 10*1024)
	if _, err := fh.Dirty.Write(0, finalData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	if got := fh.Dirty.Size(); got != int64(len(finalData)) {
		t.Fatalf("dirty size after write = %d, want %d", got, len(finalData))
	}
	if err := fs.stageShadowLocked(fh, true); err != nil {
		t.Fatal(err)
	}
	got, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, finalData) {
		t.Fatalf("shadow content mismatch after staged truncate writer adoption: got len=%d want len=%d", len(got), len(finalData))
	}
}

func TestSetAttr_WriteBackPathTruncateAdoptsCreatedSameCallerWriter(t *testing.T) {
	const callerPID = 6161
	var createCalls atomic.Int32
	var putCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/created.bin" && r.URL.RawQuery == "create=1":
			createCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			http.Error(w, "unexpected PUT", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs.commitQueue = &CommitQueue{
		maxPending:   8,
		queue:        []*CommitEntry{},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
		workCh:       make(chan *CommitEntry, 16),
	}

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Pid: callerPID}},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     defaultRegularFileMode,
	}, "created.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created file handle missing")
	}
	if fh.OpenPID != callerPID {
		t.Fatalf("created file handle OpenPID = %d, want %d", fh.OpenPID, callerPID)
	}

	oldData := bytes.Repeat([]byte{0x41}, 64*1024)
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, oldData); st != gofuse.OK {
		t.Fatalf("Write old data status = %v, want OK", st)
	}

	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId, Caller: gofuse.Caller{Pid: callerPID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if !fh.ZeroBase {
		t.Fatal("created same-caller writer did not adopt zero base")
	}
	if got := fh.Dirty.Size(); got != 0 {
		t.Fatalf("dirty size after staged truncate = %d, want 0", got)
	}
	if fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("dirty truncate not marked: dirtySeq=%d dirty=%t", fh.DirtySeq, fh.Dirty.HasDirtyParts())
	}
	if fs.commitQueue.HasPath("/created.bin") {
		t.Fatal("created truncate queued invalid path overwrite commit")
	}
	if meta, ok := pending.GetMeta("/created.bin"); ok {
		t.Fatalf("pending meta after created truncate = %+v, want no path-level overwrite", meta)
	}

	fh.Lock()
	kind := fs.pendingKindForHandle(fh)
	baseRev := fs.expectedRevisionForHandleLocked(fh)
	fh.Unlock()
	if kind != PendingNew || baseRev != 0 {
		t.Fatalf("created truncate handle commit state = kind %v baseRev %d, want PendingNew baseRev 0", kind, baseRev)
	}

	gotShadow, err := shadow.ReadAll("/created.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotShadow) != 0 {
		t.Fatalf("shadow content after staged truncate len=%d, want 0", len(gotShadow))
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("remote empty create calls = %d, want 1", got)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("remote PUT calls = %d, want 0", got)
	}
	if pending.HasPending("/created.bin") {
		t.Fatal("pending state remains after release")
	}
	if shadow.Has("/created.bin") {
		t.Fatal("shadow remains after release")
	}
}

// TestSetAttr_WriteBackPathTruncateCreatedNonZero reproduces LTP truncate02:
// a file created with O_CREAT and written to in write-back mode has no remote
// base revision yet, so a path-based truncate(path, size) with a non-zero size
// must not consult the remote server (which would return NotFound and surface
// ENOENT to the caller). The truncation should be applied to the open dirty
// handle and committed on Flush, leaving the remote untouched until then.
func TestSetAttr_WriteBackPathTruncateCreatedNonZero(t *testing.T) {
	const callerPID = 6262
	var readCalls atomic.Int32
	var putCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/created.bin" && r.URL.RawQuery == "create=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
		case r.Method == http.MethodGet:
			readCalls.Add(1)
			http.NotFound(w, r)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			http.Error(w, "unexpected PUT", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs.commitQueue = &CommitQueue{
		maxPending:   8,
		queue:        []*CommitEntry{},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
		workCh:       make(chan *CommitEntry, 16),
	}

	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Pid: callerPID}},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     defaultRegularFileMode,
	}, "created.bin", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	fh, ok := fs.fileHandles.Get(createOut.Fh)
	if !ok {
		t.Fatal("created file handle missing")
	}

	const initialSize = 1024
	oldData := bytes.Repeat([]byte{0x41}, initialSize)
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, oldData); st != gofuse.OK {
		t.Fatalf("Write old data status = %v, want OK", st)
	}
	if got := fh.Dirty.Size(); got != initialSize {
		t.Fatalf("dirty size after write = %d, want %d", got, initialSize)
	}

	// Path-based truncate(path, 256) — FATTR_SIZE only, no FATTR_FH.
	const truncSize = 256
	var attrOut gofuse.AttrOut
	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: createOut.NodeId, Caller: gofuse.Caller{Pid: callerPID}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     uint64(truncSize),
		},
	}, &attrOut)
	if st != gofuse.OK {
		t.Fatalf("SetAttr path truncate status = %v, want OK (truncate02 regression)", st)
	}
	if got := fh.Dirty.Size(); got != truncSize {
		t.Fatalf("dirty size after path truncate = %d, want %d", got, truncSize)
	}
	if attrOut.Size != truncSize {
		t.Fatalf("attr size after path truncate = %d, want %d", attrOut.Size, truncSize)
	}
	if got := readCalls.Load(); got != 0 {
		t.Fatalf("remote GET calls during created path truncate = %d, want 0 (no remote data to read)", got)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("remote PUT calls during created path truncate = %d, want 0 (commit deferred to Flush)", got)
	}
	if fs.commitQueue.HasPath("/created.bin") {
		t.Fatal("created non-zero truncate queued an overwrite commit before Flush")
	}

	// The first 256 bytes of the original content must survive the truncate.
	gotShadow, err := shadow.ReadAll("/created.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotShadow) != truncSize {
		t.Fatalf("shadow size after path truncate = %d, want %d", len(gotShadow), truncSize)
	}
	if !bytes.Equal(gotShadow, oldData[:truncSize]) {
		t.Fatalf("shadow prefix mismatch after path truncate: got %q, want %q", gotShadow, oldData[:truncSize])
	}

	// A read via the open handle must see the truncated content.
	readBuf := make([]byte, truncSize)
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
		Size:     uint32(truncSize),
	}, readBuf)
	if st != gofuse.OK {
		t.Fatalf("Read after path truncate status = %v, want OK", st)
	}
	read, st := result.Bytes(nil)
	if st != gofuse.OK {
		t.Fatalf("Read.Bytes after path truncate status = %v, want OK", st)
	}
	if len(read) != truncSize {
		t.Fatalf("Read after path truncate returned %d bytes, want %d", len(read), truncSize)
	}
	if !bytes.Equal(read, oldData[:truncSize]) {
		t.Fatalf("read content after path truncate = %q, want %q", read, oldData[:truncSize])
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
}

func TestSetAttr_WriteBackPathTruncateSkipsDefaultInheritedMode(t *testing.T) {
	fs, shadow, pending := newWriteBackPathTruncateModeTestFS(t)

	const path = "/file.bin"
	ino := fs.inodes.Lookup(path, false, 4096, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.inodes.UpdateMode(ino, defaultRegularFileMode)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if out.Size != 0 {
		t.Fatalf("out.Size = %d, want 0", out.Size)
	}
	if !shadow.Has(path) {
		t.Fatal("shadow missing for staged truncate")
	}

	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending truncate metadata missing")
	}
	if meta.HasMode {
		t.Fatalf("pending mode = has:%t mode:%o, want has:false for inherited default mode", meta.HasMode, meta.Mode)
	}

	commit := singleQueuedCommitForPath(t, fs.commitQueue, path)
	if commit.HasMode {
		t.Fatalf("queued commit mode = has:%t mode:%o, want has:false for inherited default mode", commit.HasMode, commit.Mode)
	}
}

func TestSetAttr_WriteBackPathTruncatePreservesNonDefaultInheritedMode(t *testing.T) {
	fs, _, pending := newWriteBackPathTruncateModeTestFS(t)

	const path = "/private.bin"
	ino := fs.inodes.Lookup(path, false, 4096, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.inodes.UpdateMode(ino, 0o600)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}

	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending truncate metadata missing")
	}
	if !meta.HasMode || meta.Mode&0o777 != 0o600 {
		t.Fatalf("pending mode = has:%t mode:%o, want has:true mode:0600", meta.HasMode, meta.Mode&0o777)
	}

	commit := singleQueuedCommitForPath(t, fs.commitQueue, path)
	if !commit.HasMode || commit.Mode&0o777 != 0o600 {
		t.Fatalf("queued commit mode = has:%t mode:%o, want has:true mode:0600", commit.HasMode, commit.Mode&0o777)
	}
}

func newWriteBackPathTruncateModeTestFS(t *testing.T) (*Dat9FS, *ShadowStore, *PendingIndex) {
	t.Helper()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadow.Close() })
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = &CommitQueue{
		maxPending:   8,
		queue:        []*CommitEntry{},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
		workCh:       make(chan *CommitEntry, 16),
	}
	return fs, shadow, pending
}

func singleQueuedCommitForPath(t *testing.T, cq *CommitQueue, path string) *CommitEntry {
	t.Helper()

	cq.mu.Lock()
	defer cq.mu.Unlock()
	if len(cq.queue) != 1 {
		t.Fatalf("queued commits = %d, want 1", len(cq.queue))
	}
	commit := cq.queue[0]
	if commit.Path != path {
		t.Fatalf("queued commit path = %q, want %q", commit.Path, path)
	}
	return commit
}

func TestSetAttr_CloseSyncPathTruncateKeepsSynchronousRemoteCommit(t *testing.T) {
	putStarted := make(chan struct{})
	releasePut := make(chan struct{})
	var putOnce sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putOnce.Do(func() { close(putStarted) })
			<-releasePut
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs.commitQueue = NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer fs.commitQueue.DrainAll()

	ino := fs.inodes.Lookup("/file.bin", false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	done := make(chan gofuse.Status, 1)
	go func() {
		var out gofuse.AttrOut
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{
			SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: ino},
				Valid:    gofuse.FATTR_SIZE,
				Size:     0,
			},
		}, &out)
	}()

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("close-sync SetAttr did not start remote truncate")
	}
	select {
	case st := <-done:
		t.Fatalf("close-sync SetAttr returned before remote truncate completed: %v", st)
	default:
	}
	close(releasePut)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("SetAttr status = %v, want OK", st)
		}
	case <-time.After(time.Second):
		t.Fatal("close-sync SetAttr did not return after remote truncate completed")
	}
}

func TestSetAttr_WriteBackStrictPathTruncateStagesAndReturnsBeforeRemoteCommit(t *testing.T) {
	putStarted := make(chan struct{})
	releasePut := make(chan struct{})
	var putOnce sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putOnce.Do(func() { close(putStarted) })
			<-releasePut
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq
	defer cq.DrainAll()

	ino := fs.inodes.Lookup("/file.bin", false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	done := make(chan gofuse.Status, 1)
	go func() {
		var out gofuse.AttrOut
		done <- fs.SetAttr(nil, &gofuse.SetAttrIn{
			SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: gofuse.InHeader{NodeId: ino},
				Valid:    gofuse.FATTR_SIZE,
				Size:     0,
			},
		}, &out)
	}()

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("SetAttr status = %v, want OK", st)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writeback strict SetAttr blocked on remote zero-byte commit")
	}
	if !pending.HasPending("/file.bin") {
		t.Fatal("pending truncate metadata missing")
	}
	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("async zero-byte commit did not start")
	}
	close(releasePut)
	cq.DrainAll()
}

func TestOpenTruncateCancelsQueuedPathTruncateWithoutCancelingInFlight(t *testing.T) {
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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
	path := "/file.bin"
	if err := shadow.WriteFull(path, nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 0, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	queued := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	fs.commitQueue = &CommitQueue{
		queue:        []*CommitEntry{queued},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
	}
	fs.commitQueue.rebuildQueuedIndexLocked()
	markCommitEntryDelayedForTest(t, fs.commitQueue, queued)

	ino := fs.inodes.Lookup(path, false, 42, time.Now())
	fs.inodes.UpdateRevision(ino, 7)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if !queued.canceled {
		t.Fatal("queued zero truncate was not canceled")
	}
	if fs.commitQueue.HasPath(path) {
		t.Fatal("queued zero truncate still blocks path")
	}
	if !pending.HasPending(path) {
		t.Fatal("pending local truncate metadata was removed")
	}
	if !shadow.Has(path) {
		t.Fatal("pending local truncate shadow was removed")
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("opened handle missing")
	}
	if fh.BaseRev != 7 {
		t.Fatalf("handle base revision = %d, want 7", fh.BaseRev)
	}
	if !fh.ZeroBase || fh.Dirty == nil || fh.Dirty.Size() != 0 {
		t.Fatalf("handle did not adopt zero-base dirty buffer: zero=%t dirty=%v", fh.ZeroBase, fh.Dirty)
	}

	inFlight := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	fs.commitQueue = &CommitQueue{
		queue:        []*CommitEntry{inFlight},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{path: inFlight},
	}
	fs.commitQueue.rebuildQueuedIndexLocked()
	if fs.canSupersedeQueuedPathTruncate(path) {
		t.Fatal("in-flight zero truncate was incorrectly superseded")
	}
	if inFlight.canceled {
		t.Fatal("in-flight zero truncate was canceled")
	}
}

func TestOpenWritableCancelsQueuedPathTruncateWithoutOTruncFlag(t *testing.T) {
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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
	path := "/file.bin"
	if err := shadow.WriteFull(path, nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 0, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	queued := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	fs.commitQueue = &CommitQueue{
		queue:        []*CommitEntry{queued},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
	}
	fs.commitQueue.rebuildQueuedIndexLocked()
	markCommitEntryDelayedForTest(t, fs.commitQueue, queued)

	ino := fs.inodes.Lookup(path, false, 0, time.Now())
	fs.inodes.UpdateRevision(ino, 7)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_WRONLY),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if !queued.canceled {
		t.Fatal("queued zero truncate was not canceled")
	}
	if fs.commitQueue.HasPath(path) {
		t.Fatal("queued zero truncate still blocks path")
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("opened handle missing")
	}
	if fh.BaseRev != 7 {
		t.Fatalf("handle base revision = %d, want 7", fh.BaseRev)
	}
	if !fh.ZeroBase {
		t.Fatal("handle did not preserve zero-base truncate state")
	}
	if fh.Dirty == nil {
		t.Fatal("handle dirty buffer missing")
	}
	if !fh.Dirty.HasDirtyParts() || fh.Dirty.Size() != 0 {
		t.Fatalf("handle dirty state = dirty:%v size:%d", fh.Dirty.HasDirtyParts(), fh.Dirty.Size())
	}
	if fh.DirtySeq == 0 {
		t.Fatal("handle dirty sequence was not marked")
	}
}

func TestWriteCancelsDelayedQueuedPathTruncateBeforeRemoteWait(t *testing.T) {
	var uploads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	path := "/file.bin"
	if err := shadow.WriteFull(path, nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 0, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.zeroTruncateDelay = time.Hour
	fs.commitQueue = cq
	defer cq.DrainAll()
	queued := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	if err := cq.Enqueue(queued); err != nil {
		t.Fatal(err)
	}
	if !cq.HasPath(path) {
		t.Fatal("delayed zero truncate should be visible before Write")
	}

	ino := fs.inodes.Lookup(path, false, 0, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		BaseRev:     7,
		Dirty:       fs.newWriteBuffer(path, 0, 0),
		ZeroBase:    true,
		WritePolicy: WritePolicyWriteBack,
	}
	fhID := fs.allocateFileHandle(fh)
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
		Offset:   0,
	}, []byte("new"))
	if st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if written != 3 {
		t.Fatalf("Write bytes = %d, want 3", written)
	}
	if !queued.canceled {
		t.Fatal("Write did not cancel delayed zero truncate")
	}
	if cq.HasPath(path) {
		t.Fatal("delayed zero truncate still blocks path after Write")
	}
	if got := uploads.Load(); got != 0 {
		t.Fatalf("uploads before drain = %d, want 0", got)
	}
	if !pending.HasPending(path) || !shadow.Has(path) {
		t.Fatal("Write supersede should preserve local pending/shadow state")
	}
}

func TestWriteDoesNotCancelDelayedPathTruncateForOldHandle(t *testing.T) {
	var uploads atomic.Int32
	allowUpload := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		<-allowUpload
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	path := "/file.bin"
	if err := shadow.WriteFull(path, nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 0, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.zeroTruncateDelay = time.Hour
	fs.commitQueue = cq
	defer cq.DrainAll()
	queued := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	if err := cq.Enqueue(queued); err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(path, false, 8, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		BaseRev:     7,
		Dirty:       fs.newWriteBuffer(path, 8, 0),
		WritePolicy: WritePolicyWriteBack,
	}
	if _, err := fh.Dirty.Write(0, []byte("old-tail")); err != nil {
		t.Fatal(err)
	}
	fhID := fs.allocateFileHandle(fh)
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
		}, []byte("x"))
		writeDone <- st
	}()

	select {
	case st := <-writeDone:
		t.Fatalf("Write returned after canceling zero truncate for old handle: %v", st)
	case <-time.After(75 * time.Millisecond):
	}
	if queued.canceled {
		t.Fatal("old handle write canceled delayed zero truncate")
	}
	if !cq.HasPath(path) {
		t.Fatal("delayed zero truncate disappeared before force")
	}
	close(allowUpload)
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not return after delayed zero truncate was forced")
	}
	if got := uploads.Load(); got != 1 {
		t.Fatalf("zero truncate uploads = %d, want 1", got)
	}
}

func TestWriteWaitsForInFlightPathTruncate(t *testing.T) {
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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
	path := "/file.bin"
	if err := shadow.WriteFull(path, nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 0, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	inFlight := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	fs.commitQueue = &CommitQueue{
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{path: inFlight},
	}

	ino := fs.inodes.Lookup(path, false, 0, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		BaseRev:     7,
		WritePolicy: WritePolicyWriteBack,
	}
	fhID := fs.allocateFileHandle(fh)
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
		}, []byte("new"))
		writeDone <- st
	}()

	select {
	case st := <-writeDone:
		t.Fatalf("Write returned while zero truncate was in-flight: %v", st)
	case <-time.After(75 * time.Millisecond):
	}
	if inFlight.canceled {
		t.Fatal("in-flight zero truncate was canceled")
	}
	fs.commitQueue.endInFlight(inFlight)
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not return after in-flight zero truncate completed")
	}
}

func TestOpenWritableRefreshesInodeAfterWaitingForInFlightZeroTruncate(t *testing.T) {
	path := "/tier-transition.bin"
	staleData := bytes.Repeat([]byte("s"), 8*1024*1024)
	statCh := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		statCh <- struct{}{}
		w.Header().Set("Content-Length", "0")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "8")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.readCache.Put(path, staleData, 7)
	inFlight := &CommitEntry{Path: path, Size: 0, Kind: PendingOverwrite, BaseRev: 7, CoalesceZeroTruncate: true}
	fs.commitQueue = &CommitQueue{
		queue:        nil,
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{path: inFlight},
	}

	ino := fs.inodes.Lookup(path, false, int64(len(staleData)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)

	openDone := make(chan gofuse.OpenOut, 1)
	statusDone := make(chan gofuse.Status, 1)
	go func() {
		var out gofuse.OpenOut
		statusDone <- fs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out)
		openDone <- out
	}()

	select {
	case st := <-statusDone:
		t.Fatalf("Open returned before in-flight zero truncate completed: %v", st)
	case <-time.After(75 * time.Millisecond):
	}
	fs.inodes.UpdateSize(ino, 0)
	fs.inodes.UpdateRevision(ino, 8)
	fs.commitQueue.endInFlight(inFlight)

	var st gofuse.Status
	select {
	case st = <-statusDone:
	case <-time.After(time.Second):
		t.Fatal("Open did not return after in-flight zero truncate completed")
	}
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	var out gofuse.OpenOut
	select {
	case out = <-openDone:
	case <-time.After(time.Second):
		t.Fatal("OpenOut missing")
	}
	select {
	case <-statCh:
	case <-time.After(time.Second):
		t.Fatal("Open did not stat zero-sized remote file after wait")
	}

	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("opened handle missing")
	}
	if fh.OrigSize != 0 || fh.BaseRev != 8 {
		t.Fatalf("handle orig/base = %d/%d, want 0/8", fh.OrigSize, fh.BaseRev)
	}
	if fh.Dirty == nil {
		t.Fatal("dirty buffer missing")
	}
	if got := fh.Dirty.Size(); got != 0 {
		t.Fatalf("dirty buffer size = %d, want 0; stale read cache was preloaded", got)
	}
}

func TestStageShadowReadyNonSpillRewritesShadowWithDirtyBuffer(t *testing.T) {
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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

	const baseRev int64 = 7
	path := "/tier-transition.bin"
	if err := shadow.WriteFull(path, nil, baseRev); err != nil {
		t.Fatal(err)
	}

	finalData := bytes.Repeat([]byte("x"), 10*1024)
	dirty := fs.newWriteBuffer(path, maxPreloadSize, 0)
	if _, err := dirty.Write(0, finalData); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:         42,
		Path:        path,
		Dirty:       dirty,
		BaseRev:     baseRev,
		ShadowReady: true,
		ShadowSpill: false,
		WritePolicy: WritePolicyWriteBack,
	}

	if err := fs.stageShadowForQueuedCommitLocked(fh, true); err != nil {
		t.Fatal(err)
	}
	defer fs.releaseHandleRemoteCommitPathLocked(fh)

	got, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, finalData) {
		t.Fatalf("shadow content mismatch after staged rewrite: got len=%d want len=%d", len(got), len(finalData))
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending metadata missing after staged rewrite")
	}
	if meta.Size != int64(len(finalData)) || meta.Kind != PendingOverwrite || meta.BaseRev != baseRev {
		t.Fatalf("pending meta = %+v, want size=%d kind=%v baseRev=%d", meta, len(finalData), PendingOverwrite, baseRev)
	}
}

func TestOpenTruncateResetShadowStagesDirtyBuffer(t *testing.T) {
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
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

	const baseRev int64 = 7
	path := "/tier-transition.bin"
	if err := shadow.WriteFull(path, bytes.Repeat([]byte{0x5a}, 64*1024), baseRev); err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup(path, false, 8*1024*1024, time.Now())
	fs.inodes.UpdateRevision(ino, baseRev)
	fs.inodes.UpdateSize(ino, 8*1024*1024)

	var out gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("opened handle missing")
	}
	if !fh.ShadowReady {
		t.Fatal("truncate-open did not reset shadow")
	}
	if fh.ShadowSpill {
		t.Fatal("truncate-open shadow must stay dirty-buffer backed, not ShadowSpill")
	}

	finalData := bytes.Repeat([]byte("x"), 10*1024)
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       out.Fh,
		Offset:   0,
	}, finalData); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if err := fs.stageShadowForQueuedCommitLocked(fh, true); err != nil {
		t.Fatal(err)
	}
	defer fs.releaseHandleRemoteCommitPathLocked(fh)

	got, err := shadow.ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, finalData) {
		t.Fatalf("shadow content mismatch after truncate-open staged write: got len=%d want len=%d", len(got), len(finalData))
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("pending metadata missing after truncate-open staged write")
	}
	if meta.Size != int64(len(finalData)) || meta.Kind != PendingOverwrite || meta.BaseRev != baseRev || meta.ShadowSpill {
		t.Fatalf("pending meta = %+v, want size=%d kind=%v baseRev=%d shadowSpill=false", meta, len(finalData), PendingOverwrite, baseRev)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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

func TestFlushHandle_AdoptsSameMountCommittedRevision(t *testing.T) {
	var gotExpected string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
			if gotExpected != "8" {
				http.Error(w, "bad expected revision", http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 9})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/wal.db-wal", false, 4, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision("/wal.db-wal", 8)

	fh := &FileHandle{
		Ino:     ino,
		Path:    "/wal.db-wal",
		Dirty:   NewWriteBuffer("/wal.db-wal", maxPreloadSize, 0),
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
	if gotExpected != "8" {
		t.Fatalf("X-Dat9-Expected-Revision = %q, want 8", gotExpected)
	}
	if fh.BaseRev != 9 {
		t.Fatalf("fh.BaseRev = %d, want 9", fh.BaseRev)
	}
}

func TestFlushHandle_RefreshesStartedStreamerRevision(t *testing.T) {
	data := bytes.Repeat([]byte("x"), s3client.PartSize+32)
	expectedRevision := int64(8)
	rec := newMultipartUploadRecorder(t, "/stream.db-wal", int64(len(data)), &expectedRevision)

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(rec.client(), opts)
	ino := fs.inodes.Lookup("/stream.db-wal", false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.recordCommittedRevision("/stream.db-wal", expectedRevision)

	fh := &FileHandle{
		Ino:      ino,
		Path:     "/stream.db-wal",
		Dirty:    NewWriteBuffer("/stream.db-wal", maxPreloadSize, 0),
		BaseRev:  7,
		Streamer: NewStreamUploader(rec.client(), "/stream.db-wal", 7),
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	if err := fh.Streamer.SubmitPart(context.Background(), 1, data[:s3client.PartSize], nil); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if got := rec.initiateCalls.Load(); got != 1 {
		t.Fatalf("initiate calls = %d, want 1", got)
	}
	if fh.BaseRev != expectedRevision+1 {
		t.Fatalf("fh.BaseRev = %d, want %d", fh.BaseRev, expectedRevision+1)
	}
}

func TestTruncateWritableHandleLockedZeroResetsStreamingStateBeforeContinuedWrite(t *testing.T) {
	const filePath = "/stream-reset.bin"
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())

	wb := NewWriteBuffer(filePath, streamingWriteMaxSize, 0)
	wb.uploadedParts = map[int]bool{0: true}
	var staleCallbackCalls atomic.Int32
	wb.OnPartFull = func(partIdx int, data []byte) {
		staleCallbackCalls.Add(1)
	}
	streamer := NewStreamUploader(nil, filePath, 0)
	streamer.started = true
	streamer.streamedParts[1] = true
	streamer.pendingParts[1] = []byte("stale")

	fh := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    wb,
		Streamer: streamer,
	}

	fh.Lock()
	abortStreamer, err := fs.truncateWritableHandleLocked(fh, 0)
	fh.Unlock()
	if err != nil {
		t.Fatalf("truncateWritableHandleLocked: %v", err)
	}
	if abortStreamer == nil {
		t.Fatal("abortStreamer is nil, want old streamer cleanup")
	}
	abortStreamer()

	if fh.Streamer != nil {
		t.Fatal("fh.Streamer should be cleared after zero truncate")
	}
	if wb.OnPartFull != nil {
		t.Fatal("OnPartFull should be cleared after non-shadow zero truncate")
	}
	if len(wb.uploadedParts) != 0 {
		t.Fatalf("uploadedParts len = %d, want 0", len(wb.uploadedParts))
	}
	if len(streamer.pendingParts) != 0 {
		t.Fatalf("old streamer pendingParts len = %d, want 0 after abort", len(streamer.pendingParts))
	}

	fullPart := bytes.Repeat([]byte("n"), int(wb.PartSize()))
	if _, err := wb.Write(0, fullPart); err != nil {
		t.Fatalf("continued write after truncate: %v", err)
	}
	if got := staleCallbackCalls.Load(); got != 0 {
		t.Fatalf("stale OnPartFull calls = %d, want 0", got)
	}
	if len(wb.uploadedParts) != 0 {
		t.Fatalf("uploadedParts len after continued write = %d, want 0", len(wb.uploadedParts))
	}
	if !wb.IsPartLoaded(0) {
		t.Fatal("continued write should retain part in memory when streaming is disabled")
	}
}

func TestTruncateWritableHandleLockedZeroRebindsShadowSpillEviction(t *testing.T) {
	const filePath = "/shadow-reset.bin"
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())

	wb := NewWriteBuffer(filePath, streamingWriteMaxSize, 0)
	wb.uploadedParts = map[int]bool{1: true}
	var staleCallbackCalls atomic.Int32
	wb.OnPartFull = func(partIdx int, data []byte) {
		staleCallbackCalls.Add(1)
	}
	streamer := NewStreamUploader(nil, filePath, 0)
	streamer.started = true
	streamer.streamedParts[1] = true
	streamer.pendingParts[1] = []byte("stale")

	fh := &FileHandle{
		Ino:         ino,
		Path:        filePath,
		Dirty:       wb,
		Streamer:    streamer,
		ShadowReady: true,
		ShadowSpill: true,
	}

	fh.Lock()
	abortStreamer, err := fs.truncateWritableHandleLocked(fh, 0)
	fh.Unlock()
	if err != nil {
		t.Fatalf("truncateWritableHandleLocked: %v", err)
	}
	if abortStreamer == nil {
		t.Fatal("abortStreamer is nil, want old streamer cleanup")
	}
	abortStreamer()

	if fh.Streamer != nil {
		t.Fatal("fh.Streamer should be cleared after zero truncate")
	}
	if wb.OnPartFull == nil {
		t.Fatal("OnPartFull should be rebound for ShadowSpill eviction")
	}
	if len(wb.uploadedParts) != 0 {
		t.Fatalf("uploadedParts len = %d, want 0", len(wb.uploadedParts))
	}

	fullPart := bytes.Repeat([]byte("s"), int(wb.PartSize()))
	if _, err := wb.Write(0, fullPart); err != nil {
		t.Fatalf("continued shadow write after truncate: %v", err)
	}
	if got := staleCallbackCalls.Load(); got != 0 {
		t.Fatalf("stale OnPartFull calls = %d, want 0", got)
	}
	if !wb.uploadedParts[0] {
		t.Fatal("ShadowSpill OnPartFull should evict and mark new part 0")
	}
	if wb.IsPartLoaded(0) {
		t.Fatal("ShadowSpill continued write should evict full part from memory")
	}
}

func TestAdoptPathTruncateZeroResetsStartedStreamerBeforeFlush(t *testing.T) {
	const (
		filePath  = "/created-stream.bin"
		callerPID = 4242
	)
	var (
		createCalls atomic.Int32
		otherCalls  atomic.Int32
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+filePath && r.URL.Query().Get("create") == "1" {
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(1)})
			return
		}
		otherCalls.Add(1)
		http.Error(w, "unexpected request", http.StatusTeapot)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())

	wb := NewWriteBuffer(filePath, streamingWriteMaxSize, 0)
	if _, err := wb.Write(0, []byte("stale streamed bytes")); err != nil {
		t.Fatal(err)
	}
	wb.uploadedParts = map[int]bool{0: true}
	streamer := NewStreamUploader(fs.client, filePath, 0)
	streamer.started = true
	streamer.streamedParts[1] = true
	streamer.pendingParts[1] = []byte("stale")

	fh := &FileHandle{
		Ino:     ino,
		Path:    filePath,
		Dirty:   wb,
		OpenPID: callerPID,
		IsNew:   true,
		BaseRev: 0,
		// Started streamer state must not survive path truncate adoption;
		// otherwise flushHandle chooses FinishStreaming instead of empty create.
		Streamer: streamer,
	}
	fh.DirtySeq = fs.markDirtySize(ino, wb.Size())
	fs.openHandles.Add(fh)
	defer fs.openHandles.Remove(fh)

	if !fs.adoptSingleCallerPathTruncate(filePath, callerPID) {
		t.Fatal("path truncate was not adopted by same-caller handle")
	}
	if fh.Streamer != nil {
		t.Fatal("fh.Streamer should be cleared after adopted path truncate")
	}
	if len(wb.uploadedParts) != 0 {
		t.Fatalf("uploadedParts len = %d, want 0", len(wb.uploadedParts))
	}

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("empty create calls = %d, want 1", got)
	}
	if got := otherCalls.Load(); got != 0 {
		t.Fatalf("unexpected non-create calls = %d, want 0", got)
	}
}

func TestFlushHandle_SerializesSamePathRemoteCommits(t *testing.T) {
	var (
		mu         sync.Mutex
		revision   int64 = 7
		handlerErr error
		putCalls   atomic.Int32
		inFlight   atomic.Int32
		gotHeaders []string
	)
	recordHandlerErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if handlerErr == nil {
			handlerErr = err
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		putCalls.Add(1)
		if got := inFlight.Add(1); got != 1 {
			recordHandlerErr(fmt.Errorf("concurrent remote PUTs in flight = %d, want serialized", got))
		}
		defer inFlight.Add(-1)
		time.Sleep(25 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		expected := r.Header.Get("X-Dat9-Expected-Revision")
		gotHeaders = append(gotHeaders, expected)
		if expected != strconv.FormatInt(revision, 10) {
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
			return
		}
		revision++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": revision})
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/wal.db-wal", false, 4, time.Now())
	fs.inodes.UpdateRevision(ino, 7)

	makeHandle := func(data string) *FileHandle {
		fh := &FileHandle{
			Ino:     ino,
			Path:    "/wal.db-wal",
			Dirty:   NewWriteBuffer("/wal.db-wal", maxPreloadSize, 0),
			BaseRev: 7,
		}
		if _, err := fh.Dirty.Write(0, []byte(data)); err != nil {
			t.Fatal(err)
		}
		fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
		return fh
	}

	start := make(chan struct{})
	errCh := make(chan error, 2)
	for _, fh := range []*FileHandle{makeHandle("aaaa"), makeHandle("bbbb")} {
		go func(fh *FileHandle) {
			<-start
			fh.Lock()
			st := fs.flushHandle(context.Background(), fh)
			fh.Unlock()
			if st != gofuse.OK {
				errCh <- fmt.Errorf("flushHandle status = %v, want OK", st)
				return
			}
			errCh <- nil
		}(fh)
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if got := putCalls.Load(); got != 2 {
		t.Fatalf("PUT calls = %d, want 2", got)
	}
	mu.Lock()
	err := handlerErr
	headers := append([]string(nil), gotHeaders...)
	finalRevision := revision
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) != 2 || headers[0] != "7" || headers[1] != "8" {
		t.Fatalf("expected revision headers = %v, want [7 8]", headers)
	}
	if finalRevision != 9 {
		t.Fatalf("server revision = %d, want 9", finalRevision)
	}
	if got := fs.latestCommittedRevision("/wal.db-wal"); got != 9 {
		t.Fatalf("latest committed revision = %d, want 9", got)
	}
}

func TestCommittedRevisionTrackerForgetClearsPath(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	fs.recordCommittedRevision("/wal.db-wal", 8)
	fs.forgetCommittedRevision("/wal.db-wal")

	if got := fs.latestCommittedRevision("/wal.db-wal"); got != 0 {
		t.Fatalf("latest committed revision after forget = %d, want 0", got)
	}
}

func TestCommittedRevisionTrackerReplaceAllowsNewEpoch(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	fs.recordCommittedRevision("/wal.db-wal", 8)
	fs.replaceCommittedRevision("/wal.db-wal", 1)

	if got := fs.latestCommittedRevision("/wal.db-wal"); got != 1 {
		t.Fatalf("latest committed revision after replace = %d, want 1", got)
	}
}

func TestFinishLocalRenameClearsCommittedRevisionEpochs(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	oldDir := fs.inodes.Lookup("/old", true, 0, time.Now())
	newDir := fs.inodes.Lookup("/new", true, 0, time.Now())
	fs.inodes.Lookup("/old/workload.db-wal", false, 1, time.Now())
	fs.inodes.Lookup("/new/workload.db-wal", false, 1, time.Now())

	fs.recordCommittedRevision("/old/workload.db-wal", 8)
	fs.recordCommittedRevision("/new/workload.db-wal", 5)
	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: oldDir},
		Newdir:   newDir,
	}, "/old/workload.db-wal", "/new/workload.db-wal")

	if got := fs.latestCommittedRevision("/old/workload.db-wal"); got != 0 {
		t.Fatalf("old path committed revision = %d, want 0", got)
	}
	if got := fs.latestCommittedRevision("/new/workload.db-wal"); got != 0 {
		t.Fatalf("new path committed revision = %d, want 0", got)
	}
}

func TestReleaseNewEmptyFileUsesCreateAction(t *testing.T) {
	var (
		mu          sync.Mutex
		handlerErr  error
		createCalls atomic.Int32
		putCalls    atomic.Int32
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
		case http.MethodPost:
			if r.URL.Path != "/v1/fs/empty.txt" {
				recordHandlerErr(fmt.Errorf("path = %s, want /v1/fs/empty.txt", r.URL.Path))
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
			if r.URL.Query().Has("chmod") {
				w.WriteHeader(http.StatusOK)
				return
			}
			if !r.URL.Query().Has("create") {
				recordHandlerErr(fmt.Errorf("query = %q, want create action", r.URL.RawQuery))
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": int64(1)})
		case http.MethodPut:
			putCalls.Add(1)
			http.Error(w, "unexpected PUT", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "empty.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})

	mu.Lock()
	err := handlerErr
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("create calls = %d, want 1", got)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("PUT calls = %d, want 0", got)
	}
	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("created inode entry not found")
	}
	if entry.Size != 0 {
		t.Fatalf("entry size = %d, want 0", entry.Size)
	}
	if entry.Revision != 1 {
		t.Fatalf("entry revision = %d, want 1", entry.Revision)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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

// TestFlushHandle_Path2_SnapshotIsolation verifies that Path 2 of flushHandle
// creates an immutable snapshot of dirty data before releasing fh.mu, so that
// concurrent writes to the same handle during the HTTP upload do not pollute
// the already-committed upload payload.
//
// Regression test for issue #579: without the snapshot+unlock fix, bytesView()
// returned a mutable slice into WriteBuffer.smallFileData and the lock was
// held for the entire HTTP round-trip, blocking FUSE Read() on the same handle.
func TestFlushHandle_Path2_SnapshotIsolation(t *testing.T) {
	original := []byte("ORIGINAL-PAYLOAD-BEFORE-FLUSH")

	// uploadStarted signals the test goroutine that the HTTP handler has
	// received the upload body — the lock has been released and a concurrent
	// write can proceed.
	uploadStarted := make(chan struct{})
	// uploadResume lets the test control when the HTTP handler returns,
	// keeping the upload in-flight while the concurrent write happens.
	uploadResume := make(chan struct{})

	var uploadedBody atomic.Value // stores []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read PUT body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			uploadedBody.Store(body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/snapshot-test.txt", false, int64(len(original)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.inodes.UpdateSize(ino, int64(len(original)))

	fh := &FileHandle{
		Ino:     ino,
		Path:    "/snapshot-test.txt",
		Dirty:   NewWriteBuffer("/snapshot-test.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	if _, err := fh.Dirty.Write(0, original); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())

	// Start flush in a goroutine — it will release fh.mu during upload.
	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	// Wait for the HTTP handler to receive the upload (lock released).
	<-uploadStarted

	// Mutate the WriteBuffer while upload is in-flight, simulating what
	// a real fs.Write() would do: write data + advance DirtySeq. If the
	// fix is correct, the upload uses the pre-unlock snapshot and this
	// write does not affect the committed data. The generation guard must
	// also preserve the dirty state so the new data is flushed next time.
	mutated := []byte("MUTATED-WHILE-UPLOAD-INFLIGHT!")
	fh.Lock()
	if _, err := fh.Dirty.Write(0, mutated); err != nil {
		t.Fatal(err)
	}
	// Advance DirtySeq like fs.Write() does via markDirtySize.
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fh.Unlock()

	// Let the upload complete.
	close(uploadResume)

	st := <-flushDone
	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}

	// Verify the uploaded body matches the ORIGINAL snapshot, not the
	// mutated data written while the upload was in-flight.
	got, ok := uploadedBody.Load().([]byte)
	if !ok {
		t.Fatal("no upload body recorded")
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("uploaded body = %q, want %q (concurrent write leaked into upload)", got, original)
	}

	// Verify read cache was seeded with the snapshot, not the mutated buffer.
	cached, ok := fs.readCache.Get("/snapshot-test.txt", 10)
	if !ok {
		t.Fatal("readCache miss after flush")
	}
	if !bytes.Equal(cached, original) {
		t.Fatalf("readCache content = %q, want %q", cached, original)
	}

	// Verify the concurrent write kept the handle dirty (generation guard).
	// If ClearDirty ran unconditionally, DirtySeq would be 0 and the new
	// data would be silently lost on the next flush.
	fh.Lock()
	dirtySeqAfter := fh.DirtySeq
	hasDirty := fh.Dirty.HasDirtyParts()
	fh.Unlock()
	if dirtySeqAfter == 0 {
		t.Fatal("DirtySeq = 0 after flush with concurrent write — new write data would be lost")
	}
	if !hasDirty {
		t.Fatal("Dirty.HasDirtyParts() = false after flush with concurrent write — new write not preserved")
	}

	// Verify inode size reflects the concurrent write, not the old snapshot.
	// Without this guard, UpdateSize(ino, size) would roll back to the
	// uploaded snapshot size, making getattr report a stale file length.
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found after flush")
	}
	if entry.Size != int64(len(mutated)) {
		t.Fatalf("inode size = %d, want %d (concurrent write size); stale snapshot size would be %d",
			entry.Size, len(mutated), len(original))
	}
}

// TestFlushHandle_Path2_NoDeadlockWithConcurrentWrite verifies that the lock
// ordering between fh.mu and remoteCommitLock does not deadlock. Before the
// fix, flushHandle held remoteCommitLock while waiting to reacquire fh.mu,
// while Write() held fh.mu waiting for remoteCommitLock → deadlock.
//
// After the fix, flush releases fh.mu before uploading (unblocking Read)
// while keeping remoteCommitLock held for same-path serialization. After the
// upload, remoteCommitLock is released BEFORE re-acquiring fh.mu — so flush
// never holds remoteCommitLock while waiting for fh.mu. Concurrent Write()
// acquires fh.mu then blocks on remoteCommitLock until upload finishes —
// linear wait, no deadlock. This test exercises the actual B3 scenario:
//
// 1. Flush starts, upload blocks (remoteCommitLock held, fh.mu released)
// 2. Write() starts DURING upload — acquires fh.mu, blocks on remoteCommitLock
// 3. Upload resumes → remoteCommitLock released → Write() completes
// 4. Both flush and Write succeed, dirty state preserved
func TestFlushHandle_Path2_NoDeadlockWithConcurrentWrite(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/deadlock-test.txt", false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	fh := &FileHandle{
		Ino:     ino,
		Path:    "/deadlock-test.txt",
		Dirty:   NewWriteBuffer("/deadlock-test.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	if _, err := fh.Dirty.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fhID := fs.fileHandles.Allocate(fh)

	// Start flush in background.
	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	// Wait for upload to start (fh.mu released by Path 2).
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start within timeout — fh.mu not released during Path 2 upload")
	}

	// Issue Write() DURING upload — this is the B3 deadlock scenario.
	// Write() acquires fh.mu (released by flush), then blocks on
	// remoteCommitLock (held by flush for upload serialization).
	// Before the fix: flush held remoteCommitLock and waited for fh.mu
	// to relock → circular wait → deadlock.
	// After the fix: flush releases remoteCommitLock before fh.Lock(),
	// so Write() completes after upload finishes → no deadlock.
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len("WORLD")),
		}, []byte("WORLD"))
		writeDone <- st
	}()

	// Give Write() a moment to acquire fh.mu and block on remoteCommitLock.
	// If this were the old code, both goroutines would be stuck forever.
	time.Sleep(50 * time.Millisecond)

	// Resume upload — this releases remoteCommitLock, unblocking Write().
	close(uploadResume)

	// Both flush and Write must complete without deadlock.
	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle did not complete — deadlock?")
	}
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("concurrent Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write did not complete after upload released remoteCommitLock — deadlock?")
	}

	// The concurrent write must have kept the handle dirty for next flush.
	fh.Lock()
	if fh.DirtySeq == 0 {
		fh.Unlock()
		t.Fatal("DirtySeq = 0 after concurrent Write — data loss")
	}
	fh.Unlock()

	_ = fhID // keep allocated for the duration of the test
}

// TestFlushHandle_Path2_RenameRetargetDuringUpload verifies that when
// retargetOpenHandlesForRename changes fh.Path while a Path 2 upload is
// in-flight, the committed revision is NOT misattributed to the new path.
func TestFlushHandle_Path2_RenameRetargetDuringUpload(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 20})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	oldPath := "/rename-old.txt"
	newPath := "/rename-new.txt"
	ino := fs.inodes.Lookup(oldPath, false, 4, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:     ino,
		Path:    oldPath,
		Dirty:   NewWriteBuffer(oldPath, maxPreloadSize, 0),
		BaseRev: 5,
	}
	if _, err := fh.Dirty.Write(0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.openHandles.Add(fh)

	// Start flush in background.
	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	// Wait for upload to start.
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Simulate rename retarget while upload is in-flight.
	fs.retargetOpenHandlesForRename(oldPath, newPath)

	// Let the upload complete.
	close(uploadResume)
	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle did not complete")
	}

	// The committed revision (20) should NOT be recorded on the new path's
	// handle state, because the upload went to the old path.
	fh.Lock()
	if fh.BaseRev == 20 {
		fh.Unlock()
		t.Fatal("markHandleRemoteCommittedLocked ran on retargeted handle — committed old-path upload data to new path")
	}
	// Verify the handle was actually retargeted to the new path.
	if fh.Path != newPath {
		fh.Unlock()
		t.Fatalf("fh.Path = %q after retarget, want %q", fh.Path, newPath)
	}
	// B4 dirty-preservation: the upload went to old path, but the handle
	// is now retargeted to new path. The dirty data in the buffer belongs
	// to the new path and must NOT have been cleared by the old-path
	// upload's success. DirtySeq must still be non-zero so the next flush
	// picks up this data for the new path.
	if fh.DirtySeq == 0 {
		fh.Unlock()
		t.Fatal("DirtySeq cleared after retargeted upload — new path data is no longer scheduled for flush")
	}
	if fh.Dirty.Size() == 0 {
		fh.Unlock()
		t.Fatal("Dirty buffer cleared after retargeted upload — new path data lost")
	}
	fh.Unlock()

	// Verify the old path got cached with the upload data.
	cached, ok := fs.readCache.Get(oldPath, 20)
	if !ok {
		t.Fatal("readCache miss for old path after flush")
	}
	if !bytes.Equal(cached, []byte("data")) {
		t.Fatalf("readCache for old path = %q, want %q", cached, "data")
	}

	// The new path should NOT have cached data from the old-path upload.
	if _, ok := fs.readCache.Get(newPath, 20); ok {
		t.Fatal("readCache hit for new path — old-path upload data leaked to new path")
	}
}

// TestFlushHandle_Path2_SidecarCacheUsesSnapshotOnRetarget verifies that the
// SQLite persistent journal sidecar cache uses snapshot path+data (handlePath,
// dataCopy) after the unlock window, not live fh.Path/fh.Dirty which may be
// retargeted or mutated by concurrent writes.
//
// Uses a -wal path to trigger the isSQLitePersistentJournalPath branch.
// During upload:
//   - rename retarget changes fh.Path to a new -wal path
//   - concurrent write mutates fh.Dirty with different data
//
// After flush: asserts sidecar cache contains old path + original snapshot
// data, NOT the retargeted new path or the mutated dirty buffer.
func TestFlushHandle_Path2_SidecarCacheUsesSnapshotOnRetarget(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			// Return revision 0 so committedRev == 0, forcing the
			// sidecar cache branch (sidecarCached) instead of the
			// committedRev > 0 branch.
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 0})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	oldPath := "/workload.db-wal"
	newPath := "/renamed.db-wal"
	ino := fs.inodes.Lookup(oldPath, false, 8, time.Now())
	fs.inodes.UpdateRevision(ino, 3)

	fh := &FileHandle{
		Ino:     ino,
		Path:    oldPath,
		Dirty:   NewWriteBuffer(oldPath, maxPreloadSize, 0),
		BaseRev: 3,
	}
	walData := []byte("wal-snapshot-data")
	if _, err := fh.Dirty.Write(0, walData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.openHandles.Add(fh)

	// Start flush in background.
	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	// Wait for upload to start.
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Rename retarget during upload — fh.Path changes from old to new.
	fs.retargetOpenHandlesForRename(oldPath, newPath)

	// Mutate fh.Dirty during upload — simulates concurrent Write() while
	// fh.mu is released. The sidecar cache must still use dataCopy
	// (snapshot before upload), not live fh.Dirty.bytesView().
	fh.Lock()
	mutatedData := []byte("MUTATED-WAL-DATA!")
	if _, err := fh.Dirty.Write(0, mutatedData); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.Unlock()

	// Let the upload complete.
	close(uploadResume)
	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle did not complete")
	}

	// The sidecar cache MUST have been seeded under the old path
	// (handlePath snapshot) with the snapshot data (dataCopy), NOT
	// under the retargeted new path.
	// expectedRevision for BaseRev=3 is 3 (expectedRevisionForHandle).
	sidecarRev := int64(4) // sqliteCommittedRevision(0, 3) = 3+1 = 4
	cached, ok := fs.readCache.Get(oldPath, sidecarRev)
	if !ok {
		t.Fatal("readCache miss for old -wal path — sidecar cache did not use snapshot handlePath")
	}
	if !bytes.Equal(cached, walData) {
		t.Fatalf("readCache for old path = %q, want %q — sidecar cache did not use dataCopy", cached, walData)
	}
	// Explicitly verify the cached data is NOT the mutated dirty buffer.
	if bytes.Equal(cached, mutatedData) {
		t.Fatal("readCache contains mutated dirty bytes — sidecar cache used live fh.Dirty.bytesView() instead of dataCopy")
	}

	// The new path must NOT have sidecar cache from the old-path upload.
	if _, ok := fs.readCache.Get(newPath, sidecarRev); ok {
		t.Fatal("readCache hit for new -wal path — sidecar cache leaked to retargeted path")
	}
}

// TestFlushHandle_Path2_ConcurrentWriteDoesNotCorruptOrigSize verifies that
// when a concurrent Write() grows the file during the Path 2 unlock window,
// OrigSize is set to the uploaded snapshot size, not the live dirty buffer
// size. This ensures the next flush selects the correct upload path (direct
// PUT vs patch) based on the actual committed base size. (B7 regression test)
func TestFlushHandle_Path2_ConcurrentWriteDoesNotCorruptOrigSize(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/origsize-test.txt", false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	// Start with a small file (5 bytes).
	fh := &FileHandle{
		Ino:     ino,
		Path:    "/origsize-test.txt",
		Dirty:   NewWriteBuffer("/origsize-test.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	smallData := []byte("hello")
	if _, err := fh.Dirty.Write(0, smallData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fhID := fs.fileHandles.Allocate(fh)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Concurrent Write during upload — grow file significantly.
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len(largeData)),
		}, largeData)
		writeDone <- st
	}()

	time.Sleep(50 * time.Millisecond)
	close(uploadResume)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write timed out")
	}

	// B7 assertion: OrigSize must NOT be the large post-write size.
	// The uploaded snapshot was 5 bytes, so the committed revision's
	// OrigSize should reflect the snapshot, not the concurrent write.
	fh.Lock()
	origSize := fh.OrigSize
	dirtySize := fh.Dirty.Size()
	dirtySeq := fh.DirtySeq
	fh.Unlock()

	// Dirty must still be pending (DirtySeq != 0) because concurrent write
	// changed it during upload.
	if dirtySeq == 0 {
		t.Fatal("DirtySeq = 0 after concurrent Write — dirty data lost")
	}
	// OrigSize must equal the uploaded snapshot size (5 bytes), not the
	// large dirty buffer size from the concurrent write.
	snapshotSize := int64(len(smallData))
	if origSize != snapshotSize {
		t.Fatalf("OrigSize = %d, want %d (uploaded snapshot size); dirty buffer size = %d — B7: revision-only path must set OrigSize from snapshot",
			origSize, snapshotSize, dirtySize)
	}

	_ = fhID
}

// TestFlushHandle_Path2_NoCommittedRevConcurrentWriteOrigSize verifies
// that when a Path 2 flush succeeds without the server returning a
// committed revision (e.g. PatchFile / WriteStreamConditional), and a
// concurrent Write() grows the file during the unlock window, OrigSize
// is set to the uploaded snapshot size (not the live dirty buffer size).
// This is the N1 regression test: without the DirtySeq guard in the
// non-committedRev finalization paths, finalizeHandleFlushLocked would
// derive a synthetic revision from expectedRevision and read
// fh.Dirty.Size() for OrigSize, associating the wrong size.
func TestFlushHandle_Path2_NoCommittedRevConcurrentWriteOrigSize(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			// Server returns OK but WITHOUT a revision field —
			// simulates PatchFile/WriteStreamConditional behavior.
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/n1-origsize-test.txt", false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	// Start with a small file (5 bytes).
	fh := &FileHandle{
		Ino:     ino,
		Path:    "/n1-origsize-test.txt",
		Dirty:   NewWriteBuffer("/n1-origsize-test.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	smallData := []byte("hello")
	if _, err := fh.Dirty.Write(0, smallData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fhID := fs.fileHandles.Allocate(fh)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Concurrent Write during upload — grow file significantly.
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len(largeData)),
		}, largeData)
		writeDone <- st
	}()

	time.Sleep(50 * time.Millisecond)
	close(uploadResume)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write timed out")
	}

	// N1 assertion: OrigSize must NOT be the large post-write size.
	// The uploaded snapshot was 5 bytes; the server did NOT return a
	// revision, so the finalization uses a synthetic revision. OrigSize
	// should reflect the snapshot, not the concurrent write.
	fh.Lock()
	origSize := fh.OrigSize
	dirtySize := fh.Dirty.Size()
	dirtySeq := fh.DirtySeq
	fh.Unlock()

	// Dirty must still be pending (DirtySeq != 0) because concurrent write
	// changed it during upload.
	if dirtySeq == 0 {
		t.Fatal("DirtySeq = 0 after concurrent Write — dirty data lost")
	}
	// OrigSize must equal the uploaded snapshot size (5 bytes), not the
	// large dirty buffer size from the concurrent write.
	snapshotSize := int64(len(smallData))
	if origSize != snapshotSize {
		t.Fatalf("OrigSize = %d, want %d (uploaded snapshot size); dirty buffer size = %d — N1: non-committedRev path must set OrigSize from snapshot",
			origSize, snapshotSize, dirtySize)
	}

	_ = fhID
}

// TestFlushHandle_Path2_LargeBaseSmallSnapshotOrigSize verifies the
// old-large-base → small snapshot → concurrent write scenario. OrigSize
// must be set to the uploaded snapshot size (small), not the stale large
// base size. Otherwise the next flush would incorrectly select patch path
// against a small remote object. (B7 regression test, adversary scenario)
func TestFlushHandle_Path2_LargeBaseSmallSnapshotOrigSize(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 20})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/large-base-test.txt", false, 100*1024, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	// Start with a large OrigSize (simulating a previously-large file that
	// was truncated and rewritten to a small size before this flush).
	fh := &FileHandle{
		Ino:      ino,
		Path:     "/large-base-test.txt",
		Dirty:    NewWriteBuffer("/large-base-test.txt", maxPreloadSize, 0),
		BaseRev:  5,
		OrigSize: 100 * 1024, // old large base
	}
	// Write small data (simulating truncate + small rewrite).
	smallData := []byte("tiny")
	if _, err := fh.Dirty.Write(0, smallData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fhID := fs.fileHandles.Allocate(fh)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Concurrent Write during upload — grow file to 64KB.
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len(largeData)),
		}, largeData)
		writeDone <- st
	}()

	time.Sleep(50 * time.Millisecond)
	close(uploadResume)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write timed out")
	}

	fh.Lock()
	origSize := fh.OrigSize
	dirtySize := fh.Dirty.Size()
	fh.Unlock()

	// OrigSize must equal the uploaded snapshot size (4 bytes = "tiny"),
	// NOT the old large base (100KB) and NOT the post-write dirty size (64KB).
	snapshotSize := int64(len(smallData))
	if origSize != snapshotSize {
		t.Fatalf("OrigSize = %d, want %d (uploaded snapshot size); old base = %d, dirty = %d — B7: stale large OrigSize not corrected",
			origSize, snapshotSize, 100*1024, dirtySize)
	}

	_ = fhID
}

// TestFlushHandle_Path2_CleanSiblingOrigSizeFromSnapshot verifies that a
// clean sibling handle refreshed by markHandleRevisionOnlyLocked gets
// OrigSize from the uploaded snapshot size, not from the post-concurrent-write
// inode size. Without this fix, committedHandleSizeLocked would read the
// inode entry (already updated by the concurrent Write) and rebind the
// sibling with the wrong OrigSize. (B7 sibling regression test)
func TestFlushHandle_Path2_CleanSiblingOrigSizeFromSnapshot(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 30})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ino := fs.inodes.Lookup("/sibling-test.txt", false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 1)

	// Primary handle: small file (5 bytes), dirty.
	fh := &FileHandle{
		Ino:     ino,
		Path:    "/sibling-test.txt",
		Dirty:   NewWriteBuffer("/sibling-test.txt", maxPreloadSize, 0),
		BaseRev: 1,
	}
	smallData := []byte("hello")
	if _, err := fh.Dirty.Write(0, smallData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fhID := fs.fileHandles.Allocate(fh)

	// Clean sibling handle: same path, no dirty parts, DirtySeq == 0.
	sibling := &FileHandle{
		Ino:      ino,
		Path:     "/sibling-test.txt",
		Dirty:    NewWriteBuffer("/sibling-test.txt", maxPreloadSize, 0),
		BaseRev:  1,
		OrigSize: 5,
	}
	// Set totalSize/remoteSize to match a clean read buffer.
	sibling.Dirty.totalSize = 5
	sibling.Dirty.remoteSize = 5
	siblingID := fs.fileHandles.Allocate(sibling)
	// Register both handles so refreshCommittedRevisionForOpenHandlesWithSize finds the sibling.
	if fs.openHandles == nil {
		fs.openHandles = NewOpenHandleIndex()
	}
	fs.openHandles.Add(fh)
	fs.openHandles.Add(sibling)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Concurrent Write on the primary handle during upload.
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len(largeData)),
		}, largeData)
		writeDone <- st
	}()

	time.Sleep(50 * time.Millisecond)
	close(uploadResume)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}
	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write timed out")
	}

	snapshotSize := int64(len(smallData))

	// Primary handle: OrigSize must equal snapshot size.
	fh.Lock()
	primaryOrigSize := fh.OrigSize
	fh.Unlock()
	if primaryOrigSize != snapshotSize {
		t.Fatalf("primary OrigSize = %d, want %d", primaryOrigSize, snapshotSize)
	}

	// Sibling handle: OrigSize must also equal snapshot size, NOT the
	// post-concurrent-write dirty size (64KB).
	sibling.Lock()
	siblingOrigSize := sibling.OrigSize
	siblingBaseRev := sibling.BaseRev
	sibling.Unlock()
	if siblingBaseRev != 30 {
		t.Fatalf("sibling BaseRev = %d, want 30", siblingBaseRev)
	}
	if siblingOrigSize != snapshotSize {
		t.Fatalf("sibling OrigSize = %d, want %d (snapshot size) — clean sibling rebound from post-write inode size",
			siblingOrigSize, snapshotSize)
	}

	_ = fhID
	_ = siblingID
}

// TestFlushHandle_Path2_RetargetSkipsDirCacheForOldPath verifies that when
// a handle is retargeted by rename during the Path 2 unlock window,
// cacheFileForPath is NOT called for the old handlePath. Otherwise it
// would re-add the pre-rename path to the directory cache after
// finishLocalRename already removed it. (B8 regression test)
func TestFlushHandle_Path2_RetargetSkipsDirCacheForOldPath(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 20})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	oldPath := "/rename-dircache-old.txt"
	newPath := "/rename-dircache-new.txt"
	ino := fs.inodes.Lookup(oldPath, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 3)

	fh := &FileHandle{
		Ino:     ino,
		Path:    oldPath,
		Dirty:   NewWriteBuffer(oldPath, maxPreloadSize, 0),
		BaseRev: 3,
	}
	if _, err := fh.Dirty.Write(0, []byte("data1")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	// Retarget handle during upload (simulates rename).
	fh.Lock()
	fh.Path = newPath
	fh.Unlock()

	// Negative-cache the old path (simulates finishLocalRename).
	fs.cacheNegativePath(oldPath)

	close(uploadResume)
	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}

	// B8 assertion: the old path must NOT have been re-added to dir cache
	// as a positive entry. After rename, finishLocalRename negatively cached
	// oldPath. If cacheFileForPath ran for the retargeted handle, it would
	// resurrect oldPath with a positive entry (size/revision from upload).
	parentPath, name := cacheParentName(oldPath)
	result := fs.dirCache.Lookup(parentPath, name)
	if result.kind == namespaceLookupPositive {
		t.Fatalf("dir cache has positive entry for old path %q after retarget — B8 bug: cacheFileForPath should be skipped for retargeted handles (size=%d)",
			oldPath, result.item.Size)
	}
}

// TestFlushHandle_Path2_PatchPathNoFullBufferCopy verifies that the patch
// path (existing large file with dirty parts) does NOT build a full-buffer
// dataCopy. Before the B9 fix, dataCopy was unconditionally allocated for
// all Path 2 flushes, causing 2x memory use for small edits to large files.
// This test verifies the patch path works correctly without dataCopy by
// confirming it uses PATCH (not PUT) and completes successfully.
func TestFlushHandle_Path2_PatchPathNoFullBufferCopy(t *testing.T) {
	var patchReceived atomic.Bool
	var putReceived atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patchReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			// Return a patch plan with no parts to upload (all clean).
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"upload_id":    "test-upload-b9",
				"upload_parts": []interface{}{},
				"copied_parts": []interface{}{},
			})
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/complete") {
			// Complete upload endpoint.
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		if r.Method == http.MethodPut {
			putReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// Set inline threshold low so the file is treated as "large".
	var threshold int64 = 100
	fs.smallFileMax.Store(threshold)

	path := "/b9-patch-no-full-copy.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	// Create a handle with OrigSize above threshold (existing large file).
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    NewWriteBuffer(path, maxPreloadSize, 0),
		BaseRev:  5,
		OrigSize: 200, // above threshold → patch path
	}

	// Write some data to make it dirty.
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()

	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}

	// B9 assertion: The flush should have used PATCH (not PUT) for this
	// existing large file, confirming the patch path was taken. The patch
	// path uses partSnapshots (per-dirty-part copies) instead of a full
	// dataCopy, avoiding 2x memory for large files with small edits.
	if !patchReceived.Load() {
		t.Fatal("expected PATCH request for existing large file above threshold — patch path not taken")
	}
	if putReceived.Load() {
		t.Fatal("received PUT for existing large file — should use patch path, not direct PUT")
	}
}

// TestFlushHandle_Path2_GrowthBypassesPatch verifies that when a large
// existing file grows beyond OrigSize, Path 2 uses WriteStreamConditional
// (full upload) instead of PatchFile. Without the B11 guard
// (size <= handleOrigSize), PatchFile would be selected for a grown file,
// but new parts beyond the original size have no server-side data, and the
// patch callback cannot construct correct content for them.
func TestFlushHandle_Path2_GrowthBypassesPatch(t *testing.T) {
	var patchReceived atomic.Bool
	var writeStreamReceived atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patchReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"upload_id":    "test-upload-b11",
				"upload_parts": []interface{}{},
				"copied_parts": []interface{}{},
			})
			return
		}
		if r.Method == http.MethodPost {
			// Multipart initiate or complete.
			if strings.Contains(r.URL.Path, "/complete") {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
				return
			}
			// Initiate — WriteStreamConditional starts with POST.
			writeStreamReceived.Store(true)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			writeStreamReceived.Store(true)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var threshold int64 = 100
	fs.smallFileMax.Store(threshold)

	path := "/b11-growth-no-patch.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	// Existing large file (OrigSize=200), but write grows it to 400.
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    NewWriteBuffer(path, maxPreloadSize, 0),
		BaseRev:  5,
		OrigSize: 200, // above threshold
	}

	// Write 400 bytes — grows beyond OrigSize.
	data := make([]byte, 400)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := fh.Dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	fh.Lock()
	_ = fs.flushHandle(context.Background(), fh)
	fh.Unlock()

	// B11 assertion: growth must NOT use PatchFile. It should use
	// WriteStreamConditional (full upload) because new parts beyond
	// OrigSize have no server-side original data.
	if patchReceived.Load() {
		t.Fatal("received PATCH for grown file — B11: growth beyond OrigSize must bypass patch path and use full upload")
	}
}

// TestFlushHandle_Path2_SyntheticRevRecordedBeforeUnlock verifies that when
// a Path 2 flush succeeds without the server returning a committed revision
// (e.g. PatchFile / WriteStreamConditional), the synthetic revision
// (expectedRevision + 1) is recorded to committedRev BEFORE releasing
// the per-path remote commit lock. Without this (B10), a same-path flush
// could acquire the lock, snapshot a stale expectedRevision, and issue a
// conditional upload that conflicts with the just-completed one.
func TestFlushHandle_Path2_SyntheticRevRecordedBeforeUnlock(t *testing.T) {
	uploadStarted := make(chan struct{})
	uploadResume := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.ReadAll(r.Body)
			close(uploadStarted)
			<-uploadResume
			// Return OK WITHOUT a revision — simulates WriteStreamConditional.
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	path := "/b10-synthetic-rev.txt"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:     ino,
		Path:    path,
		Dirty:   NewWriteBuffer(path, maxPreloadSize, 0),
		BaseRev: 5,
	}
	if _, err := fh.Dirty.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		st := fs.flushHandle(context.Background(), fh)
		fh.Unlock()
		flushDone <- st
	}()

	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}

	close(uploadResume)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("flushHandle status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flushHandle timed out")
	}

	// B10 assertion: The synthetic revision (expectedRevision + 1 = 6)
	// must be recorded in committedRev. Without the B10 fix, committedRev
	// was only recorded for server-returned revisions (committedRev > 0),
	// leaving a window where a same-path flush could snapshot stale state.
	fs.committedMu.Lock()
	recordedRev := fs.committedRev[path]
	fs.committedMu.Unlock()

	expectedSyntheticRev := int64(6) // BaseRev(5) + 1
	if recordedRev != expectedSyntheticRev {
		t.Fatalf("committedRev[%q] = %d, want %d — B10: synthetic revision must be recorded before releasing remote commit lock",
			path, recordedRev, expectedSyntheticRev)
	}
}

// TestFlushHandle_Path2_LazyLoadedGrowthReturnsEIO verifies that when a
// lazy-loaded existing large file grows beyond OrigSize and enters the
// WriteStreamConditional path, flushHandle returns EIO if the buffer
// cannot materialize all parts. Without the B12 fix, bytesView() would
// zero-fill unloaded remote-backed parts, overwriting remote data.
func TestFlushHandle_Path2_LazyLoadedGrowthReturnsEIO(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("should not reach server — flush should return EIO before upload")
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var threshold int64 = 100
	fs.smallFileMax.Store(threshold)

	path := "/b12-lazy-growth.dat"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	// Create a part-mode WriteBuffer with remoteSize > 0 (simulating
	// a lazy-loaded existing large file where some parts are NOT loaded).
	wb := NewWriteBuffer(path, maxPreloadSize, 256)
	wb.remoteSize = 512 // remote has 512 bytes that are NOT loaded locally
	wb.totalSize = 512

	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    wb,
		BaseRev:  5,
		OrigSize: 200, // above threshold
	}

	// Write ONLY at offset 512 (append), growing file to 600.
	// Parts 0 and 1 (covering bytes 0-511) are NOT loaded.
	appendData := make([]byte, 88)
	for i := range appendData {
		appendData[i] = byte(i + 1)
	}
	if _, err := fh.Dirty.Write(512, appendData); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	// Verify CanMaterializeFull is false (parts 0,1 not loaded).
	if fh.Dirty.CanMaterializeFull() {
		t.Fatal("expected CanMaterializeFull() = false for lazy-loaded buffer")
	}

	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()

	// B12 assertion: must return EIO, not attempt upload with zero-filled data.
	if st == gofuse.OK {
		t.Fatal("expected EIO for lazy-loaded growth path — B12: must not materialize unloaded remote parts as zeroes")
	}
	if st != gofuse.EIO {
		t.Fatalf("expected EIO, got %v", st)
	}
}

// TestFlushHandle_Path2_DebounceCancelledBeforeUnlock verifies that the
// debouncer is cancelled for the flush path before releasing fh.mu.
// Without the B13 fix, a debounced upload could fire while fh.mu is
// released, racing with the forced flush upload using the same
// expectedRevision and causing a CAS conflict.
func TestFlushHandle_Path2_DebounceCancelledBeforeUnlock(t *testing.T) {
	var uploadCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			uploadCount.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	opts.FlushDebounce = 50 * time.Millisecond
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	path := "/b13-debounce-cancel.txt"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:     ino,
		Path:    path,
		Dirty:   NewWriteBuffer(path, maxPreloadSize, 0),
		BaseRev: 5,
	}
	if _, err := fh.Dirty.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	// Schedule a debounced flush first.
	fh.Lock()
	_ = fs.flushHandleDebounced(context.Background(), fh, false)
	fh.Unlock()

	// Now do a forced flush (flushHandle directly). This should cancel
	// the pending debounce before releasing fh.mu.
	fh.Lock()
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()

	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}

	// Wait for debounce timer to expire. If the debounce was properly
	// cancelled, only the forced flush upload should have occurred.
	time.Sleep(200 * time.Millisecond)

	count := uploadCount.Load()
	if count != 1 {
		t.Fatalf("upload count = %d, want 1 — B13: debounced upload should be cancelled before forced flush", count)
	}
}

// TestFlushHandle_Path2_DebounceAlreadyFiredSkipsUpload verifies that when a
// debounced callback has already fired and is blocked waiting on handle.Lock(),
// it acquires remoteCommitLock and re-checks expectedRevision, skipping the
// upload if a forced flush (Path 2) already uploaded successfully.
//
// Interleaving under test:
//
//	1. Hold fh.Lock() → schedule debounce (short delay) → keep lock held
//	2. Timer fires → callback blocks on handle.Lock() (we hold it)
//	3. Call flushHandle (Path 2) while holding lock — Path 2 internally
//	   releases fh.mu, uploads, records committed revision, releases
//	   remoteCommitLock, reacquires fh.mu, returns
//	4. fh.Unlock() → callback acquires handle.Lock() → acquires
//	   remoteCommitLock → re-checks expectedRevision → skip (no double PUT)
func TestFlushHandle_Path2_DebounceAlreadyFiredSkipsUpload(t *testing.T) {
	var uploadCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			uploadCount.Add(1)
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": 10})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	// Very short debounce so the timer fires while we hold the lock.
	opts.FlushDebounce = 5 * time.Millisecond
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	path := "/b13-already-fired.txt"
	ino := fs.inodes.Lookup(path, false, 5, time.Now())
	fs.inodes.UpdateRevision(ino, 5)

	fh := &FileHandle{
		Ino:     ino,
		Path:    path,
		Dirty:   NewWriteBuffer(path, maxPreloadSize, 0),
		BaseRev: 5,
	}
	if _, err := fh.Dirty.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	_ = fs.fileHandles.Allocate(fh)

	// Step 1: Hold lock, schedule debounce, keep lock held.
	fh.Lock()
	_ = fs.flushHandleDebounced(context.Background(), fh, false)
	// Do NOT unlock — we keep fh.mu held so the callback blocks.

	// Step 2: Wait for the debounce timer to fire. The callback removes
	// itself from the pending map and then blocks on handle.Lock().
	time.Sleep(50 * time.Millisecond)
	// At this point: callback has fired, removed from pending map,
	// blocked on handle.Lock(). Cancel() would be a no-op.

	// Step 3: Force flush (Path 2) while holding fh.mu.
	// flushHandle internally: acquires remoteCommitLock → releases fh.mu →
	// uploads → records committed revision → releases remoteCommitLock →
	// reacquires fh.mu → returns.
	//
	// During the fh.Unlock() window inside flushHandle, the debounced
	// callback acquires handle.Lock() but then blocks on remoteCommitLock
	// (which Path 2 still holds). After Path 2 finishes, callback acquires
	// remoteCommitLock, re-checks expectedRevision, sees it changed, skips.
	st := fs.flushHandle(context.Background(), fh)
	fh.Unlock()

	if st != gofuse.OK {
		t.Fatalf("flushHandle status = %v, want OK", st)
	}

	// Step 4: Wait for debounced callback to finish (it should skip).
	time.Sleep(100 * time.Millisecond)

	count := uploadCount.Load()
	if count != 1 {
		t.Fatalf("upload count = %d, want 1 — B13: already-fired debounced callback should be serialized by remoteCommitLock and skip when expectedRevision advanced", count)
	}
}

func TestFinalizeHandleFlushLocked_ResetsStreamerToCommittedRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
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

func TestFinalizeHandleFlushLocked_RecordsAndRefreshesSamePathRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	ino := fs.inodes.Lookup("/stream.db-wal", false, 4, time.Now())
	writer := &FileHandle{
		Ino:     ino,
		Path:    "/stream.db-wal",
		Dirty:   NewWriteBuffer("/stream.db-wal", maxPreloadSize, 0),
		BaseRev: 11,
	}
	sibling := &FileHandle{
		Ino:      ino,
		Path:     "/stream.db-wal",
		Dirty:    NewWriteBuffer("/stream.db-wal", maxPreloadSize, 0),
		BaseRev:  11,
		Streamer: NewStreamUploader(nil, "/stream.db-wal", 11),
	}
	fs.openHandles.Add(writer)
	fs.openHandles.Add(sibling)

	writer.Lock()
	fs.finalizeHandleFlushLocked(writer, 11)
	writer.Unlock()

	if got := fs.latestCommittedRevision("/stream.db-wal"); got != 12 {
		t.Fatalf("latest committed revision = %d, want 12", got)
	}
	if writer.BaseRev != 12 {
		t.Fatalf("writer BaseRev = %d, want 12", writer.BaseRev)
	}
	if sibling.BaseRev != 12 {
		t.Fatalf("sibling BaseRev = %d, want 12", sibling.BaseRev)
	}
	if got := sibling.Streamer.ExpectedRevision(); got != 12 {
		t.Fatalf("sibling streamer expected revision = %d, want 12", got)
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
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
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
	if fh.Streamer != nil {
		t.Fatal("stream uploader should be reset after adopted zero truncate")
	}
	if fh.Dirty.OnPartFull != nil {
		t.Fatal("streaming callback should be reset after adopted zero truncate")
	}
	if len(fh.Dirty.uploadedParts) != 0 {
		t.Fatalf("uploadedParts len after path truncate = %d, want 0", len(fh.Dirty.uploadedParts))
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

func TestSetAttr_PathTruncateReadModifyWrite(t *testing.T) {
	var (
		mu       sync.Mutex
		revision int64 = 1
		content        = []byte("abcdef")
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "active",
				"max_upload_bytes": maxPathTruncateInMemoryBytes,
			})
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file.bin":
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/file.bin":
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write(content)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/file.bin":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			content = append([]byte(nil), body...)
			revision++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(content)), time.Now())
	fs.inodes.UpdateRevision(ino, revision)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     3,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr shrink status = %v, want OK", st)
	}
	if got, want := string(content), "abc"; got != want {
		t.Fatalf("content after shrink = %q, want %q", got, want)
	}
	if out.Size != 3 {
		t.Fatalf("out.Size after shrink = %d, want 3", out.Size)
	}

	st = fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     5,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr extend status = %v, want OK", st)
	}
	if want := []byte{'a', 'b', 'c', 0, 0}; !bytes.Equal(content, want) {
		t.Fatalf("content after extend = %v, want %v", content, want)
	}
	if out.Size != 5 {
		t.Fatalf("out.Size after extend = %d, want 5", out.Size)
	}
}

func TestSetAttr_PathTruncateShrinkUsesRangeForRedirectedObject(t *testing.T) {
	const existingSize int64 = 1 << 30

	var (
		mu         sync.Mutex
		revision   int64 = 1
		content          = []byte("abc")
		rangeCalls int
		fullReads  int
	)

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "active",
				"max_upload_bytes": maxPathTruncateInMemoryBytes,
			})
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/large.bin":
			mu.Lock()
			size := existingSize
			if revision > 1 {
				size = int64(len(content))
			}
			rev := revision
			mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/large.bin":
			w.Header().Set("Location", ts.URL+"/object/large.bin")
			w.WriteHeader(http.StatusFound)
		case r.Method == http.MethodGet && r.URL.Path == "/object/large.bin":
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				mu.Lock()
				fullReads++
				mu.Unlock()
				http.Error(w, "missing range", http.StatusInternalServerError)
				return
			}
			if rangeHeader != "bytes=0-2" {
				http.Error(w, "bad range "+rangeHeader, http.StatusRequestedRangeNotSatisfiable)
				return
			}
			mu.Lock()
			rangeCalls++
			body := append([]byte(nil), content...)
			mu.Unlock()
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-2/%d", existingSize))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/large.bin":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			content = append([]byte(nil), body...)
			revision++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/large.bin", false, existingSize, time.Now())
	fs.inodes.UpdateRevision(ino, revision)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     3,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr shrink status = %v, want OK", st)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := string(content); got != "abc" {
		t.Fatalf("content after shrink = %q, want abc", got)
	}
	if rangeCalls != 1 {
		t.Fatalf("range calls = %d, want 1", rangeCalls)
	}
	if fullReads != 0 {
		t.Fatalf("full object reads = %d, want 0", fullReads)
	}
	if out.Size != 3 {
		t.Fatalf("out.Size after shrink = %d, want 3", out.Size)
	}
}

func TestSetAttr_PathTruncateExtendUsesMultipartWhenAboveInlineThreshold(t *testing.T) {
	const fileSize = 1234567

	var (
		mu            sync.Mutex
		content       []byte
		revision      int64 = 1
		initiateTotal int64
		completeParts int
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":           "active",
				"max_upload_bytes": int64(1024),
				"inline_threshold": int64(1024),
			})
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file.bin":
			mu.Lock()
			defer mu.Unlock()
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad initiate body", http.StatusBadRequest)
				return
			}
			if req.Path != "/file.bin" {
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
			initiateTotal = req.TotalSize
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
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			content = append([]byte(nil), body...)
			mu.Unlock()
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
			mu.Lock()
			revision++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.Warm(context.Background())
	fs := NewDat9FS(c, opts)
	ino := fs.inodes.Lookup("/file.bin", false, 0, time.Now())
	fs.inodes.UpdateRevision(ino, revision)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     fileSize,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if initiateTotal != fileSize {
		t.Fatalf("multipart initiate total_size = %d, want %d", initiateTotal, fileSize)
	}
	if completeParts != 1 {
		t.Fatalf("multipart complete parts = %d, want 1", completeParts)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(content) != fileSize {
		t.Fatalf("uploaded content size = %d, want %d", len(content), fileSize)
	}
	if !bytes.Equal(content[:16], make([]byte, 16)) {
		t.Fatal("extended content prefix should be zero-filled")
	}
	if out.Size != fileSize {
		t.Fatalf("out.Size = %d, want %d", out.Size, fileSize)
	}
}

func TestSetAttr_PathTruncateOverflowSizeReturnsEFBIG(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/huge.bin", false, 0, time.Now())

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     uint64(1 << 63),
		},
	}, &out)
	if st != gofuse.Status(syscall.EFBIG) {
		t.Fatalf("SetAttr status = %v, want EFBIG", st)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("remote write calls = %d, want 0", got)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

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
// time. Local mutation handlers should finish their own bookkeeping without
// depending on kernel notify callbacks.
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
			name: "Symlink",
			fn: func() gofuse.Status {
				var out gofuse.EntryOut
				return fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "target.txt", "newlink", &out)
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
				t.Fatalf("%s timed out", tc.name)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(serverURL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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

	// Flush (will debounce since file < defaultSmallFileThreshold and debounce > 0)
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
		{0, 60 * time.Second, 60 * time.Second},         // small file: floor
		{10 << 20, 60 * time.Second, 60 * time.Second},  // 10 MB: still floor
		{1 << 30, 200 * time.Second, 220 * time.Second}, // 1 GiB: ~205s
		{100 << 30, 15 * time.Minute, 15 * time.Minute}, // 100 GiB: capped at 15min
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
		mu       sync.Mutex
		uploaded bool // set true after /complete
		etagSeq  int
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
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
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
//  1. Create + Write 10 MiB
//  2. Flush (was: returned OK without uploading)
//  3. Lookup the path → must succeed
func TestFlushLargeFile_StrictUploadsBeforeReturning(t *testing.T) {
	const fileSize = int64(writeBackThreshold) // 10 MiB — minimal trigger of the large-file path

	completeCh := make(chan struct{}, 1)
	ts := largeFlushStreamingMock(t, fileSize, completeCh)
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
//  1. Flush returns quickly without contacting the upload endpoints.
//  2. A subsequent Lookup hits pendingIndex (no remote stat needed).
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
	c := newTestClient(ts.URL)
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

func TestRenamePendingNewCommitSyncCommitsGitLooseObjectFinalPath(t *testing.T) {
	oldP := "/repo/.git/objects/70/tmp_obj_test"
	newP := "/repo/.git/objects/70/24234d93f61104585962ac664bc5a7ed1d241d"
	data := []byte("loose object data")

	oldPutStarted := make(chan struct{})
	var oldPutOnce sync.Once
	var oldPuts atomic.Int32
	var finalPuts atomic.Int32
	var renameCalls atomic.Int32
	var mu sync.Mutex
	var finalBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+oldP:
			oldPuts.Add(1)
			oldPutOnce.Do(func() { close(oldPutStarted) })
			select {
			case <-r.Context().Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
			case <-time.After(200 * time.Millisecond):
				http.Error(w, "old temp upload should have been canceled", http.StatusServiceUnavailable)
			}
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			finalPuts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			finalBody = append([]byte(nil), body...)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
		case r.Method == http.MethodPost && r.URL.RawQuery == "rename":
			renameCalls.Add(1)
			http.Error(w, "server rename should not be used for pending-new temp files", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer cq.DrainAll()
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	oldIno := fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())
	dirIno := fs.inodes.Lookup("/repo/.git/objects/70", true, 0, time.Now())

	if err := cq.Enqueue(&CommitEntry{
		Path:  oldP,
		Inode: oldIno,
		Size:  int64(len(data)),
		Kind:  PendingNew,
	}); err != nil {
		t.Fatalf("enqueue old temp upload: %v", err)
	}
	select {
	case <-oldPutStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("old temp upload did not start")
	}

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "tmp_obj_test", "24234d93f61104585962ac664bc5a7ed1d241d")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	cq.DrainAll()
	if got := finalPuts.Load(); got != 1 {
		t.Fatalf("final path PUTs = %d, want 1", got)
	}
	if got := renameCalls.Load(); got != 0 {
		t.Fatalf("remote rename calls = %d, want 0", got)
	}
	mu.Lock()
	gotBody := string(finalBody)
	mu.Unlock()
	if gotBody != string(data) {
		t.Fatalf("final upload body = %q, want %q", gotBody, string(data))
	}
	if pending.HasPending(oldP) {
		t.Fatal("old temp path still pending")
	}
	if pending.HasPending(newP) {
		t.Fatal("final path still pending after upload")
	}
	if shadow.Has(oldP) {
		t.Fatal("old temp shadow still exists")
	}
	if shadow.Has(newP) {
		t.Fatal("final shadow still exists after upload")
	}
	if oldPuts.Load() == 0 {
		t.Fatal("old temp upload was never exercised")
	}
}

func TestRenamePendingNewCommitGitLooseObjectSyncFailureKeepsRecoverableShadow(t *testing.T) {
	oldP := "/repo/.git/objects/7e/tmp_obj_test"
	newP := "/repo/.git/objects/7e/b689963134a158b392aca0dc75f94d3cee15f6"
	finalName := "b689963134a158b392aca0dc75f94d3cee15f6"
	data := []byte("loose object data that must survive upload failure")

	var finalPuts atomic.Int32
	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			finalPuts.Add(1)
			http.Error(w, "storage backend unavailable", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.RawQuery == "rename":
			renameCalls.Add(1)
			http.Error(w, "server rename should not be used for pending git objects", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	dirIno := fs.inodes.Lookup("/repo/.git/objects/7e", true, 0, time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "tmp_obj_test", finalName)
	if st != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("Rename status = %v, want EAGAIN", st)
	}
	cq.DrainAll()
	shadow.Close()

	if got := finalPuts.Load(); got != 1 {
		t.Fatalf("final path PUTs = %d, want 1", got)
	}
	if got := renameCalls.Load(); got != 0 {
		t.Fatalf("remote rename calls = %d, want 0", got)
	}
	if pending.HasPending(oldP) {
		t.Fatal("old temp path still pending")
	}
	if !pending.HasPending(newP) {
		t.Fatal("final path should stay pending after sync upload failure")
	}

	// Simulate a remount against a server that still has no object. The local
	// durable shadow/pending overlay must make the final object readable, so a
	// transient backend outage cannot turn a Git object into a permanent ENOENT.
	pending2, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending2.RecoverFromDisk(); err != nil {
		t.Fatalf("RecoverFromDisk: %v", err)
	}
	shadow2, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow2.Close()

	fs2 := NewDat9FS(c, opts)
	fs2.shadowStore = shadow2
	fs2.pendingIndex = pending2
	dirIno2 := fs2.inodes.Lookup("/repo/.git/objects/7e", true, 0, time.Now())
	var entryOut gofuse.EntryOut
	st = fs2.Lookup(nil, &gofuse.InHeader{NodeId: dirIno2}, finalName, &entryOut)
	if st != gofuse.OK {
		t.Fatalf("Lookup recovered object: %v, want OK", st)
	}
	if entryOut.Size != uint64(len(data)) {
		t.Fatalf("Lookup size = %d, want %d", entryOut.Size, len(data))
	}

	var openOut gofuse.OpenOut
	st = fs2.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: entryOut.NodeId},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open recovered object: %v", st)
	}
	buf := make([]byte, len(data)+8)
	result, st := fs2.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: entryOut.NodeId},
		Fh:       openOut.Fh,
		Size:     uint32(len(buf)),
	}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read recovered object: %v", st)
	}
	got, _ := result.Bytes(buf)
	if string(got) != string(data) {
		t.Fatalf("Read recovered object = %q, want %q", string(got), string(data))
	}
}

func TestRenamePendingNewCommitFallsBackWhenFinalTargetExists(t *testing.T) {
	oldP := "/repo/.git/config.lock"
	newP := "/repo/.git/config"
	data := []byte("[core]\n\trepositoryformatversion = 0\n[remote \"origin\"]\n\turl = https://github.com/mem9-ai/drive9.git\n")

	var oldPuts atomic.Int32
	var finalPuts atomic.Int32
	var renameCalls atomic.Int32
	var renameSource string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			w.Header().Set("Content-Length", "36")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "4")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+oldP:
			oldPuts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if string(body) != string(data) {
				http.Error(w, "unexpected temp upload body", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 5})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			finalPuts.Add(1)
			http.Error(w, "final path should not be uploaded directly when target exists", http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			renameCalls.Add(1)
			mu.Lock()
			renameSource = r.Header.Get("X-Dat9-Rename-Source")
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer cq.DrainAll()
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	oldIno := fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())
	dirIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())
	fs.inodes.Lookup(newP, false, 36, time.Now())

	if err := cq.Enqueue(&CommitEntry{
		Path:  oldP,
		Inode: oldIno,
		Size:  int64(len(data)),
		Kind:  PendingNew,
	}); err != nil {
		t.Fatalf("enqueue old temp upload: %v", err)
	}

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "config.lock", "config")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	cq.DrainAll()
	if got := oldPuts.Load(); got != 1 {
		t.Fatalf("old temp PUTs = %d, want 1", got)
	}
	if got := finalPuts.Load(); got != 0 {
		t.Fatalf("final path PUTs = %d, want 0", got)
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("remote rename calls = %d, want 1", got)
	}
	mu.Lock()
	gotRenameSource := renameSource
	mu.Unlock()
	if gotRenameSource != oldP {
		t.Fatalf("rename source = %q, want %q", gotRenameSource, oldP)
	}
	if pending.HasPending(oldP) {
		t.Fatal("old temp path still pending")
	}
	if shadow.Has(oldP) {
		t.Fatal("old temp shadow still exists")
	}
}

func TestRenamePendingNewCommitUsesRemoteRenameWhenCanceledUploadBecameVisible(t *testing.T) {
	oldP := "/repo/.git/config.lock"
	newP := "/repo/.git/config"
	data := []byte("[core]\n\trepositoryformatversion = 0\n")

	oldPutStarted := make(chan struct{})
	var oldPutOnce sync.Once
	var oldPuts atomic.Int32
	var finalPuts atomic.Int32
	var renameCalls atomic.Int32
	var oldRemoteExists atomic.Bool
	var newRemoteExists atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			if newRemoteExists.Load() {
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "2")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+oldP:
			if oldRemoteExists.Load() {
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "1")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+oldP:
			oldPuts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if string(body) != string(data) {
				http.Error(w, "unexpected temp upload body", http.StatusBadRequest)
				return
			}
			oldRemoteExists.Store(true)
			oldPutOnce.Do(func() { close(oldPutStarted) })
			<-r.Context().Done()
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			finalPuts.Add(1)
			http.Error(w, "final path should not be uploaded directly when old temp is already remote-visible", http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if got := r.Header.Get("X-Dat9-Rename-Source"); got != oldP {
				http.Error(w, "unexpected rename source "+got, http.StatusBadRequest)
				return
			}
			renameCalls.Add(1)
			oldRemoteExists.Store(false)
			newRemoteExists.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer cq.DrainAll()
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	oldIno := fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())
	dirIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())

	if err := cq.Enqueue(&CommitEntry{
		Path:  oldP,
		Inode: oldIno,
		Size:  int64(len(data)),
		Kind:  PendingNew,
	}); err != nil {
		t.Fatalf("enqueue old temp upload: %v", err)
	}

	select {
	case <-oldPutStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for old temp upload to start")
	}

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "config.lock", "config")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}

	if got := oldPuts.Load(); got != 1 {
		t.Fatalf("old temp PUTs = %d, want 1", got)
	}
	if got := finalPuts.Load(); got != 0 {
		t.Fatalf("final path PUTs = %d, want 0", got)
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("remote rename calls = %d, want 1", got)
	}
	if oldRemoteExists.Load() {
		t.Fatal("old temp path still exists remotely")
	}
	if !newRemoteExists.Load() {
		t.Fatal("final path was not made visible by remote rename")
	}
	if pending.HasPending(oldP) {
		t.Fatal("old temp path still pending")
	}
	if shadow.Has(oldP) {
		t.Fatal("old temp shadow still exists")
	}
	if _, ok := fs.inodes.GetInode(oldP); ok {
		t.Fatal("old temp inode still exists after rename")
	}
	if _, ok := fs.inodes.GetInode(newP); !ok {
		t.Fatal("final inode missing after rename")
	}
}

func TestAtomicTempWriteFsyncReleaseRenamePreservesMultipartBody(t *testing.T) {
	oldP := "/work/final/w0/file-000.txt.tmp.123.456"
	newP := "/work/final/w0/file-000.txt"
	data := bytes.Repeat([]byte("drive9-concurrency-final-000\n"), 2048)

	type uploadState struct {
		path string
		size int64
		body []byte
	}
	var (
		mu           sync.Mutex
		remote       = make(map[string][]byte)
		uploads      = make(map[string]*uploadState)
		nextUploadID int
		renameCalls  int
		finalUploads int
	)
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "active",
				"limits": map[string]any{"inline_threshold": 1},
			})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.Path != oldP && req.Path != newP {
				t.Errorf("initiate path = %q, want %q or %q", req.Path, oldP, newP)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			nextUploadID++
			uploadID := fmt.Sprintf("upload-%d", nextUploadID)
			uploads[uploadID] = &uploadState{path: req.Path, size: req.TotalSize}
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   uploadID,
				"key":         "object-key",
				"part_size":   req.TotalSize,
				"total_parts": 1,
			})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			mu.Lock()
			state := uploads[uploadID]
			mu.Unlock()
			if state == nil {
				t.Errorf("presign unknown upload_id %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/" + uploadID + "/1",
					"size":   state.size,
				}},
			})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/"):
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
			if len(parts) != 2 {
				t.Errorf("bad s3 path %q", r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			if uploads[parts[0]] == nil {
				t.Errorf("put unknown upload_id %q", parts[0])
			} else {
				uploads[parts[0]].body = append([]byte(nil), body...)
			}
			mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/complete")
			mu.Lock()
			state := uploads[uploadID]
			if state == nil || len(state.body) == 0 {
				mu.Unlock()
				t.Errorf("complete before body for %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote[state.path] = append([]byte(nil), state.body...)
			if state.path == newP {
				finalUploads++
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/abort"):
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !bytes.Equal(body, data) {
				http.Error(w, "unexpected final path body", http.StatusBadRequest)
				return
			}
			mu.Lock()
			finalUploads++
			remote[newP] = append([]byte(nil), body...)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
			return
		case r.Method == http.MethodHead && (r.URL.Path == "/v1/fs"+oldP || r.URL.Path == "/v1/fs"+newP):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			mu.Lock()
			body, ok := remote[path]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if got := r.Header.Get("X-Dat9-Rename-Source"); got != oldP {
				t.Errorf("rename source = %q, want %q", got, oldP)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			body, ok := remote[oldP]
			if !ok {
				mu.Unlock()
				t.Errorf("remote rename before temp path visible")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			remote[newP] = append([]byte(nil), body...)
			delete(remote, oldP)
			renameCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+newP:
			mu.Lock()
			body, ok := remote[newP]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			return
		case r.Method == http.MethodGet && r.URL.RawQuery == "list=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
			return
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncInteractive}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)
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
	queue := NewCommitQueue(c, shadow, pending, nil, 1, 8, fs.remoteRoot())
	queue.PathLock = fs.lockRemoteCommitPath
	queue.OnSuccess = fs.onCommitQueueSuccess
	queue.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = queue
	defer queue.DrainAll()

	fs.inodes.Lookup("/work", true, 0, time.Now())
	fs.inodes.Lookup("/work/final", true, 0, time.Now())
	parentIno := fs.inodes.Lookup("/work/final/w0", true, 0, time.Now())
	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: parentIno},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC),
		Mode:     defaultRegularFileMode,
	}, "file-000.txt.tmp.123.456", &createOut); st != gofuse.OK {
		t.Fatalf("Create temp: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, data); st != gofuse.OK {
		t.Fatalf("Write temp: %v", st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Fsync temp: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	beforeRenameNotify := fs.notifyCount.Load()
	if st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: parentIno},
		Newdir:   parentIno,
	}, "file-000.txt.tmp.123.456", "file-000.txt"); st != gofuse.OK {
		t.Fatalf("Rename temp to final: %v", st)
	}
	if got := fs.notifyCount.Load() - beforeRenameNotify; got != 2 {
		t.Fatalf("rename notify count = %d, want 2", got)
	}
	queue.DrainAll()

	mu.Lock()
	gotFinal := append([]byte(nil), remote[newP]...)
	_, oldExists := remote[oldP]
	gotRenameCalls := renameCalls
	gotFinalUploads := finalUploads
	mu.Unlock()
	if !bytes.Equal(gotFinal, data) {
		t.Fatalf("final remote body sha=%x size=%d, want sha=%x size=%d", sha256.Sum256(gotFinal), len(gotFinal), sha256.Sum256(data), len(data))
	}
	if oldExists {
		t.Fatal("old temp path still exists after remote rename")
	}
	if gotRenameCalls+gotFinalUploads != 1 {
		t.Fatalf("finalization count: remote renames=%d direct uploads=%d, want exactly one", gotRenameCalls, gotFinalUploads)
	}
	if pending.HasPending(oldP) {
		t.Fatal("old temp path still pending")
	}
	if shadow.Has(oldP) {
		t.Fatal("old temp shadow still exists")
	}
	newIno, ok := fs.inodes.GetInode(newP)
	if !ok {
		t.Fatal("final inode missing after rename")
	}
	entry, ok := fs.inodes.GetEntry(newIno)
	if !ok {
		t.Fatal("final inode entry missing after rename")
	}
	if entry.Size != int64(len(data)) {
		t.Fatalf("final inode size = %d, want %d", entry.Size, len(data))
	}
	if cached := fs.dirCache.Lookup("/work/final/w0", "file-000.txt"); cached.kind != namespaceLookupPositive || cached.item.Size != int64(len(data)) {
		t.Fatalf("final dirCache entry = %+v, want positive size %d", cached, len(data))
	}

	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: newIno},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open final: %v", st)
	}
	result, st := fs.Read(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: newIno},
		Fh:       openOut.Fh,
		Size:     uint32(len(data)),
	}, make([]byte, len(data)))
	if st != gofuse.OK {
		t.Fatalf("Read final: %v", st)
	}
	gotRead, readStatus := result.Bytes(make([]byte, len(data)))
	if readStatus != gofuse.OK {
		t.Fatalf("Read final bytes: %v", readStatus)
	}
	if !bytes.Equal(gotRead, data) {
		t.Fatalf("final mounted read sha=%x size=%d, want sha=%x size=%d", sha256.Sum256(gotRead), len(gotRead), sha256.Sum256(data), len(data))
	}
}

func TestFsyncStrictShadowSpillRefreshesStaleInodeSize(t *testing.T) {
	filePath := "/work/final/w0/file-000.txt.tmp.123.456"
	data := bytes.Repeat([]byte("drive9-concurrency-final-000\n"), 128)

	type uploadState struct {
		path string
		size int64
		body []byte
	}
	var (
		mu           sync.Mutex
		uploads      = make(map[string]*uploadState)
		nextUploadID int
	)
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if req.Path != filePath {
				t.Errorf("initiate path = %q, want %q", req.Path, filePath)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			nextUploadID++
			uploadID := fmt.Sprintf("upload-%d", nextUploadID)
			uploads[uploadID] = &uploadState{path: req.Path, size: req.TotalSize}
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   uploadID,
				"key":         "object-key",
				"part_size":   req.TotalSize,
				"total_parts": 1,
			})
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/presign-batch")
			mu.Lock()
			state := uploads[uploadID]
			mu.Unlock()
			if state == nil {
				t.Errorf("presign unknown upload_id %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/" + uploadID + "/1",
					"size":   state.size,
				}},
			})
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/"):
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
			if len(parts) != 2 {
				t.Errorf("bad s3 path %q", r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			state := uploads[parts[0]]
			if state == nil {
				t.Errorf("put unknown upload_id %q", parts[0])
			} else {
				state.body = append([]byte(nil), body...)
			}
			mu.Unlock()
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/uploads/") && strings.HasSuffix(r.URL.Path, "/complete"):
			uploadID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/uploads/"), "/complete")
			mu.Lock()
			state := uploads[uploadID]
			if state == nil || !bytes.Equal(state.body, data) {
				mu.Unlock()
				t.Errorf("complete body mismatch for %q", uploadID)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
			return
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0, SyncMode: SyncStrict}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	fs := NewDat9FS(c, opts)
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

	parentIno := fs.inodes.Lookup("/work/final/w0", true, 0, time.Now())
	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: parentIno},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC),
		Mode:     defaultRegularFileMode,
	}, "file-000.txt.tmp.123.456", &createOut); st != gofuse.OK {
		t.Fatalf("Create temp: %v", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
	}, data); st != gofuse.OK {
		t.Fatalf("Write temp: %v", st)
	}

	fs.inodes.UpdateSize(createOut.NodeId, 0)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Fsync temp: %v", st)
	}
	entry, ok := fs.inodes.GetEntry(createOut.NodeId)
	if !ok {
		t.Fatal("temp inode entry missing after fsync")
	}
	if entry.Size != int64(len(data)) {
		t.Fatalf("temp inode size after fsync = %d, want %d", entry.Size, len(data))
	}
}

func TestFinishLocalRenameInvalidatesTargetParentWhenSourceInodeGone(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	parentPath := "/work/final/w0"
	oldP := parentPath + "/file-000.txt.tmp.123.456"
	newP := parentPath + "/file-000.txt"
	parentIno := fs.inodes.Lookup(parentPath, true, 0, time.Now())
	fs.dirCache.Put(parentPath, []CachedFileInfo{{
		Name:  path.Base(newP),
		Size:  0,
		IsDir: false,
		Mtime: time.Now(),
	}})

	fs.finishLocalRename(&gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: parentIno},
		Newdir:   parentIno,
	}, oldP, newP)

	if cached := fs.dirCache.Lookup(parentPath, path.Base(newP)); cached.kind == namespaceLookupPositive {
		t.Fatalf("target parent cache kept stale positive entry after source inode was gone: %+v", cached.item)
	}
}

func TestRenamePendingNewCommitSyncCommitsWhenQueueStopped(t *testing.T) {
	oldP := "/repo/.git/objects/70/tmp_obj_sync"
	newP := "/repo/.git/objects/70/24234d93f61104585962ac664bc5a7ed1d241d"
	data := []byte("loose object")

	var finalPuts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			finalPuts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if string(body) != string(data) {
				http.Error(w, "unexpected final upload body", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 7})
		case r.Method == http.MethodPost && r.URL.RawQuery == "rename":
			http.Error(w, "server rename should not be used for absent final target", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

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
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	cq.DrainAll()
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	oldIno := fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())
	dirIno := fs.inodes.Lookup("/repo/.git/objects/70", true, 0, time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "tmp_obj_sync", "24234d93f61104585962ac664bc5a7ed1d241d")
	if st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}
	if got := finalPuts.Load(); got != 1 {
		t.Fatalf("final path PUTs = %d, want 1", got)
	}
	if pending.HasPending(newP) {
		t.Fatal("final path still pending after sync commit")
	}
	if shadow.Has(newP) {
		t.Fatal("final shadow still exists after sync commit")
	}
	if _, ok := fs.inodes.GetInode(newP); !ok {
		t.Fatal("final inode missing after local rename")
	}
	if _, ok := fs.inodes.GetEntry(oldIno); !ok {
		t.Fatal("renamed inode should still exist")
	}
}

func TestUnlinkRemoteDeleteDoesNotRetryRecreatedPathAfterInterrupt(t *testing.T) {
	path := "/repo/.github/workflows/local-e2e.yml"

	firstDeleteStarted := make(chan struct{})
	var firstDeleteOnce sync.Once
	var recreated atomic.Bool
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+path:
			if deleteCalls.Add(1) == 1 {
				recreated.Store(true)
				firstDeleteOnce.Do(func() { close(firstDeleteStarted) })
				<-r.Context().Done()
				return
			}
			http.Error(w, "must not delete recreated path", http.StatusInternalServerError)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+path:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "6172")
			w.Header().Set("X-Dat9-IsDir", "false")
			if recreated.Load() {
				w.Header().Set("X-Dat9-Revision", "2")
			} else {
				w.Header().Set("X-Dat9-Revision", "1")
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	parentIno := fs.inodes.Lookup("/repo/.github/workflows", true, 0, time.Now())
	fs.inodes.Lookup(path, false, 6172, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Unlink(cancel, &gofuse.InHeader{NodeId: parentIno}, "local-e2e.yml")
	}()

	select {
	case <-firstDeleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first delete request")
	}

	select {
	case st := <-done:
		if st != gofuse.EAGAIN {
			t.Fatalf("Unlink status = %v, want EAGAIN", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Unlink timed out")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got == 0 {
		t.Fatal("delete recovery did not stat the remote path after interruption")
	}
	if _, ok := fs.inodes.GetInode(path); !ok {
		t.Fatal("failed unlink should keep the local inode")
	}
}

func TestUnlinkRemoteDeleteAcceptsGoneAfterInterrupt(t *testing.T) {
	path := "/repo/AGENTS.md"

	firstDeleteStarted := make(chan struct{})
	var firstDeleteOnce sync.Once
	var deleted atomic.Bool
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+path:
			if deleteCalls.Add(1) == 1 {
				deleted.Store(true)
				firstDeleteOnce.Do(func() { close(firstDeleteStarted) })
				<-r.Context().Done()
				return
			}
			http.Error(w, "already deleted", http.StatusNotFound)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+path:
			headCalls.Add(1)
			if deleted.Load() {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", "10011")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	parentIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	fs.inodes.Lookup(path, false, 10011, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Unlink(cancel, &gofuse.InHeader{NodeId: parentIno}, "AGENTS.md")
	}()

	select {
	case <-firstDeleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first delete request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Unlink status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Unlink timed out")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got == 0 {
		t.Fatal("delete retry did not confirm the remote path was gone")
	}
	if _, ok := fs.inodes.GetInode(path); ok {
		t.Fatal("unlinked path still has an inode")
	}
}

func TestUnlinkPreservesOpenReadHandleAfterPathRemoval(t *testing.T) {
	const path = "/file.txt"
	data := []byte("Hello, World!")
	var getCalls atomic.Int32
	var deleteCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+path:
			getCalls.Add(1)
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+path:
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.LookupWithIdentity(path, "file-1", 1, false, int64(len(data)), time.Now())
	fh := &FileHandle{Ino: ino, Path: path}
	fhID := fs.allocateFileHandle(fh)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("snapshot GET calls = %d, want 1", got)
	}
	if _, ok := fs.inodes.GetInode(path); ok {
		t.Fatal("unlinked path still has visible inode mapping")
	}
	if fs.openHandles.Has(0, path) {
		t.Fatal("unlinked handle still occupies path index")
	}
	if !fs.openHandles.Has(ino, "") {
		t.Fatal("unlinked handle missing from inode index")
	}

	var attr gofuse.AttrOut
	if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if attr.Nlink != 0 {
		t.Fatalf("nlink = %d, want 0 for open-but-unlinked file", attr.Nlink)
	}

	buf := make([]byte, len(data))
	res, st := fs.Read(nil, &gofuse.ReadIn{Fh: fhID, Size: uint32(len(buf))}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, _ := res.Bytes(buf)
	if string(got) != string(data) {
		t.Fatalf("read data = %q, want %q", string(got), string(data))
	}
}

func TestUnlinkPreservesDirtyOpenHandleRead(t *testing.T) {
	const path = "/file.txt"
	data := []byte("Hello,_World!")
	var deleteCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+path {
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.LookupWithIdentity(path, "file-1", 1, false, 0, time.Now())
	dirty := fs.newWriteBuffer(path, 1024, 0)
	if _, err := dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{Ino: ino, Path: path, Dirty: dirty}
	fh.DirtySeq = fs.markDirtySize(ino, dirty.Size())
	fhID := fs.allocateFileHandle(fh)

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}

	var attr gofuse.AttrOut
	if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if attr.Nlink != 0 {
		t.Fatalf("nlink = %d, want 0 for open-but-unlinked file", attr.Nlink)
	}
	if attr.Size != uint64(len(data)) {
		t.Fatalf("size = %d, want %d", attr.Size, len(data))
	}

	buf := make([]byte, len(data))
	res, st := fs.Read(nil, &gofuse.ReadIn{Fh: fhID, Size: uint32(len(buf))}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read status = %v, want OK", st)
	}
	got, _ := res.Bytes(buf)
	if string(got) != string(data) {
		t.Fatalf("read data = %q, want %q", string(got), string(data))
	}
}

func TestUnlinkAndRmdirUseDeleteKindHints(t *testing.T) {
	var fileQuery atomic.Value
	var dirQuery atomic.Value

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/file.txt":
			fileQuery.Store(r.URL.RawQuery)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir":
			dirQuery.Store(r.URL.RawQuery)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/file.txt", false, 4, time.Now())
	fs.inodes.Lookup("/dir", true, 0, time.Now())

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.txt"); st != gofuse.OK {
		t.Fatalf("Unlink: %v", st)
	}
	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir"); st != gofuse.OK {
		t.Fatalf("Rmdir: %v", st)
	}

	if got, _ := fileQuery.Load().(string); got != "kind=file" {
		t.Fatalf("file delete query = %q, want kind=file", got)
	}
	if got, _ := dirQuery.Load().(string); got != "kind=dir" {
		t.Fatalf("dir delete query = %q, want kind=dir", got)
	}
}

func TestRmdirRejectsKnownLocalChild(t *testing.T) {
	var deleteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCalls.Add(1)
			http.Error(w, "delete should not be called for known non-empty dir", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	childIno := fs.inodes.Lookup("/dir/pipe", false, 0, time.Now())
	fs.inodes.UpdateMode(childIno, uint32(syscall.S_IFIFO)|0o644)
	fs.dirCache.Upsert("/dir", CachedFileInfo{Name: "pipe", HasMode: true, Mode: uint32(syscall.S_IFIFO) | 0o644})

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("remote delete calls = %d, want 0", got)
	}
	if _, ok := fs.inodes.GetEntry(dirIno); !ok {
		t.Fatal("non-empty rmdir should keep directory inode")
	}
}

func TestRmdirRejectsRemoteChildBeforeDelete(t *testing.T) {
	var deleteCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir" && r.URL.Query().Has("list"):
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "file.txt",
					"isDir": false,
					"size":  1,
				}},
			})
		case r.Method == http.MethodDelete:
			deleteCalls.Add(1)
			http.Error(w, "delete should not be called for remote non-empty dir", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/dir", true, 0, time.Now())
	// Register the child file in the inode table so hasKnownLocalDirectoryChildren
	// returns true and Rmdir rejects immediately without retry.
	fs.inodes.Lookup("/dir/file.txt", false, 1, time.Now())

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Errorf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	// With the retry logic, if the local inode state knows about the child,
	// Rmdir returns ENOTEMPTY immediately without a remote list call.
	if got := listCalls.Load(); got != 0 {
		t.Errorf("remote list calls = %d, want 0", got)
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Errorf("remote delete calls = %d, want 0", got)
	}
}

func TestRmdirSucceedsAfterUnlinkWithStaleRemoteList(t *testing.T) {
	// Simulate eventual consistency: a file was unlinked locally (inode
	// marked Unlinked=true) but the remote listing still shows it.
	// Rmdir should proceed with the delete instead of returning ENOTEMPTY,
	// because the local state is authoritative for recently-deleted entries.
	var deleteCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir" && r.URL.Query().Has("list"):
			listCalls.Add(1)
			// Remote still lists the deleted file (eventual consistency lag).
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name":  "stale.txt",
					"isDir": false,
					"size":  1,
				}},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir":
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir/stale.txt":
			// The unlink delete succeeded on the backend.
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// Set up the directory and the file inode.
	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	_ = dirIno
	fileIno := fs.inodes.Lookup("/dir/stale.txt", false, 1, time.Now())
	_ = fileIno

	// Simulate the file being unlinked: RemoveLinkPreserve marks the inode
	// as Unlinked=true while keeping the inode alive for open handles.
	// The real Unlink handler also records a delete tombstone.
	fs.inodes.RemoveLinkPreserve("/dir/stale.txt")
	fs.markPathDeleted("/dir/stale.txt")

	// Rmdir: NodeId=1 (root) is the parent, "dir" is the child to remove.
	// Should proceed to DELETE despite the stale remote listing.
	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.OK {
		t.Errorf("Rmdir status = %v, want OK (stale remote entry should be ignored)", st)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Errorf("remote delete calls = %d, want 1", got)
	}
}

func TestRmdirSucceedsAfterFullUnlinkWithStaleRemoteList(t *testing.T) {
	// Simulate eventual consistency: a file was unlinked and its inode fully
	// removed from the table (RemoveLink, not RemoveLinkPreserve — no open
	// handles). The remote listing shows the file on the first call, but
	// returns empty on the second call (after the retry wait).
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir" && r.URL.Query().Has("list"):
			n := listCalls.Add(1)
			if n == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []map[string]any{{
						"name": "stale.txt", "isDir": false, "size": 1,
					}},
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []map[string]any{},
				})
			}
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir":
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	fs.inodes.Lookup("/dir", true, 0, time.Now())
	fs.inodes.Lookup("/dir/stale.txt", false, 1, time.Now())
	fs.dirCache.Remove("/dir", "stale.txt")
	fs.inodes.RemoveLink("/dir/stale.txt")

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.OK {
		t.Errorf("Rmdir status = %v, want OK (should succeed after retry when remote clears)", st)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Errorf("remote delete calls = %d, want 1", got)
	}
}

func TestRmdirSucceedsWithTombstoneFilteringStaleRemoteListing(t *testing.T) {
	// Verify that delete tombstones filter stale remote listings even when
	// the inode has been fully removed (RemoveLink with no open handles).
	// The mock ALWAYS returns the stale entry (never clears), so the only way
	// Rmdir can succeed is via the tombstone filter — not via polling.
	var deleteCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir" && r.URL.Query().Has("list"):
			listCalls.Add(1)
			// Always returns stale entry — tombstone must filter this.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []map[string]any{{
					"name": "stale.txt", "isDir": false, "size": 1,
				}},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir":
			deleteCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// Simulate: file was unlinked and inode fully removed (RemoveLink).
	fs.inodes.Lookup("/dir", true, 0, time.Now())
	fs.inodes.Lookup("/dir/stale.txt", false, 1, time.Now())
	fs.dirCache.Remove("/dir", "stale.txt")
	fs.inodes.RemoveLink("/dir/stale.txt")
	// Record the tombstone — this is what the Unlink handler does after
	// a successful remote delete.
	fs.markPathDeleted("/dir/stale.txt")

	// Rmdir should succeed immediately because the tombstone filters the stale
	// entry — no polling delay needed.
	start := time.Now()
	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	elapsed := time.Since(start)

	if st != gofuse.OK {
		t.Errorf("Rmdir status = %v, want OK (tombstone should filter stale entry)", st)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Rmdir took %s, expected < 500ms (tombstone should filter without polling)", elapsed)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Errorf("remote delete calls = %d, want 1", got)
	}
}

func TestRmdirRemoteDeleteDoesNotRetryRecreatedPathAfterInterrupt(t *testing.T) {
	path := "/repo/emptydir"

	firstDeleteStarted := make(chan struct{})
	var firstDeleteOnce sync.Once
	var recreated atomic.Bool
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs"+path:
			if deleteCalls.Add(1) == 1 {
				recreated.Store(true)
				firstDeleteOnce.Do(func() { close(firstDeleteStarted) })
				<-r.Context().Done()
				return
			}
			http.Error(w, "must not delete recreated path", http.StatusInternalServerError)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+path:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "true")
			if recreated.Load() {
				w.Header().Set("X-Dat9-Revision", "2")
			} else {
				w.Header().Set("X-Dat9-Revision", "1")
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	parentIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	fs.inodes.Lookup(path, true, 0, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rmdir(cancel, &gofuse.InHeader{NodeId: parentIno}, "emptydir")
	}()

	select {
	case <-firstDeleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first delete request")
	}

	select {
	case st := <-done:
		if st != gofuse.EAGAIN {
			t.Fatalf("Rmdir status = %v, want EAGAIN", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rmdir timed out")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("delete calls = %d, want 1", got)
	}
	if got := headCalls.Load(); got == 0 {
		t.Fatal("rmdir recovery did not stat the remote path after interruption")
	}
	if _, ok := fs.inodes.GetInode(path); !ok {
		t.Fatal("failed rmdir should keep the local inode")
	}
}

func TestRenameRemoteWithTransientRetryRetriesAfterInterrupt(t *testing.T) {
	oldP := "/repo/.git/objects/e6/tmp_obj_XyNuJc"
	newP := "/repo/.git/objects/e6/d0788db28a1c0860d85a7f9181233b37665bce"

	firstRenameStarted := make(chan struct{})
	var firstRenameOnce sync.Once
	var renameCalls atomic.Int32
	var wrongSource atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if r.Header.Get("X-Dat9-Rename-Source") != oldP {
				wrongSource.Store(true)
			}
			if renameCalls.Add(1) == 1 {
				firstRenameOnce.Do(func() { close(firstRenameStarted) })
				<-r.Context().Done()
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	dirIno := fs.inodes.Lookup("/repo/.git/objects/e6", true, 0, time.Now())
	fs.inodes.Lookup(oldP, false, 849, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: dirIno},
			Newdir:   dirIno,
		}, "tmp_obj_XyNuJc", "d0788db28a1c0860d85a7f9181233b37665bce")
	}()

	select {
	case <-firstRenameStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first rename request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rename timed out")
	}
	if got := renameCalls.Load(); got != 2 {
		t.Fatalf("rename calls = %d, want 2", got)
	}
	if wrongSource.Load() {
		t.Fatalf("rename source header did not match %q", oldP)
	}
}

func TestRenameRemoteWithTransientRetryAcceptsTargetVisibleAfterInterrupt(t *testing.T) {
	oldP := "/repo/.git/objects/e6/tmp_obj_XyNuJc"
	newP := "/repo/.git/objects/e6/d0788db28a1c0860d85a7f9181233b37665bce"

	firstRenameStarted := make(chan struct{})
	var firstRenameOnce sync.Once
	var renameCalls atomic.Int32
	var targetVisible atomic.Bool
	var headTargetCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if renameCalls.Add(1) == 1 {
				targetVisible.Store(true)
				firstRenameOnce.Do(func() { close(firstRenameStarted) })
				<-r.Context().Done()
				return
			}
			http.Error(w, "old path no longer exists", http.StatusNotFound)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			headTargetCalls.Add(1)
			if targetVisible.Load() {
				w.Header().Set("Content-Length", "849")
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "2")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	dirIno := fs.inodes.Lookup("/repo/.git/objects/e6", true, 0, time.Now())
	fs.inodes.Lookup(oldP, false, 849, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: dirIno},
			Newdir:   dirIno,
		}, "tmp_obj_XyNuJc", "d0788db28a1c0860d85a7f9181233b37665bce")
	}()

	select {
	case <-firstRenameStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first rename request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rename timed out")
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("rename calls = %d, want 1", got)
	}
	if got := headTargetCalls.Load(); got == 0 {
		t.Fatal("target visibility was not probed after interrupted rename")
	}
}

func TestRenameRemoteWithTransientRetryDoesNotAcceptPreexistingTarget(t *testing.T) {
	oldP := "/repo/.git/config.lock"
	newP := "/repo/.git/config"

	firstRenameStarted := make(chan struct{})
	var firstRenameOnce sync.Once
	var renameCalls atomic.Int32
	var sourceExists atomic.Bool
	var headSourceCalls atomic.Int32
	var headTargetCalls atomic.Int32
	sourceExists.Store(true)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if renameCalls.Add(1) == 1 {
				firstRenameOnce.Do(func() { close(firstRenameStarted) })
				<-r.Context().Done()
				return
			}
			sourceExists.Store(false)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			headTargetCalls.Add(1)
			w.Header().Set("Content-Length", "36")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+oldP:
			headSourceCalls.Add(1)
			if sourceExists.Load() {
				w.Header().Set("Content-Length", "54")
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "2")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	dirIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())
	fs.inodes.Lookup(oldP, false, 54, time.Now())
	fs.inodes.Lookup(newP, false, 36, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: dirIno},
			Newdir:   dirIno,
		}, "config.lock", "config")
	}()

	select {
	case <-firstRenameStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first rename request")
	}

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rename timed out")
	}
	if got := renameCalls.Load(); got != 2 {
		t.Fatalf("rename calls = %d, want 2", got)
	}
	if got := headTargetCalls.Load(); got == 0 {
		t.Fatal("preexisting target was not checked after interrupted rename")
	}
	if got := headSourceCalls.Load(); got == 0 {
		t.Fatal("source path was not checked after interrupted rename")
	}
	if sourceExists.Load() {
		t.Fatal("retry did not commit the overwrite rename")
	}
}

func TestRenamePendingNewCommitFallsBackWhenCanceledUploadReachedRemote(t *testing.T) {
	oldP := "/repo/.git/config.lock"
	newP := "/repo/.git/config"

	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+oldP:
			w.Header().Set("Content-Length", "36")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if r.Header.Get("X-Dat9-Rename-Source") != oldP {
				t.Errorf("rename source = %q, want %q", r.Header.Get("X-Dat9-Rename-Source"), oldP)
			}
			renameCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer fs.commitQueue.DrainAll()

	data := []byte("[core]\n\tfilemode = false\n")
	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	dirIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())
	fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Newdir:   dirIno,
	}, "config.lock", "config")
	if st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("remote rename calls = %d, want 1", got)
	}
	if pending.HasPending(oldP) {
		t.Fatal("old lock path should not remain pending after remote fallback")
	}
	if shadow.Has(oldP) {
		t.Fatal("old lock shadow should be removed after remote fallback")
	}
	if _, ok := fs.inodes.GetInode(oldP); ok {
		t.Fatal("old lock inode should not remain mapped after rename")
	}
	if _, ok := fs.inodes.GetInode(newP); !ok {
		t.Fatal("new config inode should be mapped after rename")
	}
}

func TestRenamePendingNewCommitOldVisibilityProbeIgnoresFuseCancel(t *testing.T) {
	oldP := "/repo/.git/config.lock"
	newP := "/repo/.git/config"

	oldHeadStarted := make(chan struct{})
	oldHeadCanceled := make(chan struct{})
	allowOldHead := make(chan struct{})
	var oldHeadOnce sync.Once
	var oldHeadCanceledOnce sync.Once
	var renameCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+newP:
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+oldP:
			oldHeadOnce.Do(func() { close(oldHeadStarted) })
			select {
			case <-allowOldHead:
			case <-r.Context().Done():
				oldHeadCanceledOnce.Do(func() { close(oldHeadCanceled) })
				return
			}
			w.Header().Set("Content-Length", "36")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if r.Header.Get("X-Dat9-Rename-Source") != oldP {
				t.Errorf("rename source = %q, want %q", r.Header.Get("X-Dat9-Rename-Source"), oldP)
			}
			renameCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)

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
	fs.commitQueue = NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer fs.commitQueue.DrainAll()

	data := []byte("[core]\n\tfilemode = false\n")
	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	dirIno := fs.inodes.Lookup("/repo/.git", true, 0, time.Now())
	fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: dirIno},
			Newdir:   dirIno,
		}, "config.lock", "config")
	}()

	select {
	case <-oldHeadStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for old-path visibility probe")
	}
	select {
	case <-oldHeadCanceled:
		t.Fatal("old-path visibility probe used the canceled FUSE request context")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowOldHead)

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rename timed out")
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("remote rename calls = %d, want 1", got)
	}
	if pending.HasPending(oldP) {
		t.Fatal("old lock path should not remain pending after remote fallback")
	}
	if shadow.Has(oldP) {
		t.Fatal("old lock shadow should be removed after remote fallback")
	}
	if _, ok := fs.inodes.GetInode(oldP); ok {
		t.Fatal("old lock inode should not remain mapped after rename")
	}
	if _, ok := fs.inodes.GetInode(newP); !ok {
		t.Fatal("new config inode should be mapped after rename")
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fileSize := int64(1 << 20) // 1 MiB — above defaultSmallFileThreshold

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

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

func TestRemoteReadLimiterCapsConcurrentRangeReads(t *testing.T) {
	const (
		readLimit = 2
		totalRead = 5
		chunkSize = 4096
	)
	var inflight atomic.Int32
	var maxInflight atomic.Int32
	started := make(chan struct{}, totalRead)
	ready := make(chan struct{}, totalRead)
	startReads := make(chan struct{})
	releaseServerReads := make(chan struct{})
	var releaseServerReadsOnce sync.Once
	releaseAllServerReads := func() {
		releaseServerReadsOnce.Do(func() { close(releaseServerReads) })
	}
	t.Cleanup(releaseAllServerReads)
	response := bytes.Repeat([]byte("x"), chunkSize)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/large.bin" {
			http.NotFound(w, r)
			return
		}
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			max := maxInflight.Load()
			if cur <= max || maxInflight.CompareAndSwap(max, cur) {
				break
			}
		}
		started <- struct{}{}
		<-releaseServerReads
		w.Header().Set("Content-Length", strconv.Itoa(len(response)))
		_, _ = w.Write(response)
	}))
	defer ts.Close()

	opts := &MountOptions{ReadConcurrency: readLimit}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	wb := fs.newWriteBuffer("/large.bin", streamingWriteMaxSize, chunkSize)
	wb.totalSize = int64(totalRead * chunkSize)
	wb.remoteSize = wb.totalSize
	fhID := fs.fileHandles.Allocate(&FileHandle{
		Path:  "/large.bin",
		Dirty: wb, // skip read-target resolution; exercise the remote range read path only.
	})

	var wg sync.WaitGroup
	errs := make(chan string, totalRead)
	for i := range totalRead {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			<-startReads
			_, st := fs.Read(nil, &gofuse.ReadIn{
				Fh:     fhID,
				Offset: uint64(i * chunkSize),
				Size:   chunkSize,
			}, nil)
			if st != gofuse.OK {
				errs <- fmt.Sprintf("read %d status = %v, want OK", i, st)
			}
		}(i)
	}

	for i := 0; i < totalRead; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatalf("only %d read goroutines started, want %d", i, totalRead)
		}
	}
	close(startReads)

	for i := 0; i < readLimit; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("only %d remote reads reached server, want %d", i, readLimit)
		}
	}
	select {
	case <-started:
		t.Fatal("third remote read reached server before a read slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	releaseAllServerReads()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := maxInflight.Load(); got > readLimit {
		t.Fatalf("max remote reads in flight = %d, want <= %d", got, readLimit)
	}
}

func TestRemoteReadLimiterAcquireHonorsCancellationAndReleases(t *testing.T) {
	opts := &MountOptions{ReadConcurrency: 1}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), opts)

	release, err := fs.acquireRemoteReadSlot(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fs.acquireRemoteReadSlot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire err = %v, want context.Canceled", err)
	}

	release()
	releaseAgain, err := fs.acquireRemoteReadSlot(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	releaseAgain()
}

func TestRemoteReadLimiterDoesNotGateWriteHandler(t *testing.T) {
	opts := &MountOptions{ReadConcurrency: 1}
	opts.setDefaults()
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), opts)

	release, err := fs.acquireRemoteReadSlot(context.Background())
	if err != nil {
		t.Fatalf("acquire read slot: %v", err)
	}
	defer release()

	ino := fs.inodes.Lookup("/out.txt", false, 0, time.Now())
	fhID := fs.fileHandles.Allocate(&FileHandle{
		Ino:   ino,
		Path:  "/out.txt",
		Dirty: fs.newWriteBuffer("/out.txt", maxPreloadSize, 0),
	})
	done := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{Fh: fhID, Offset: 0}, []byte("ok"))
		done <- st
	}()

	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Write status = %v, want OK", st)
		}
	case <-time.After(time.Second):
		t.Fatal("Write handler blocked behind the remote read limiter")
	}
}

// TestDoRangeReadBodyTimeoutReturnsForRetry verifies that doRangeRead surfaces
// context deadline errors during body read (not swallowed), allowing the retry
// helper to classify and retry them.
func TestDoRangeReadBodyTimeoutReturnsForRetry(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Send 200 with large Content-Length but write nothing.
		// The client's context will be canceled, causing a body read error.
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
		<-r.Context().Done()
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := fs.doRangeRead(ctx, "/test.bin", nil, 0, 4096)
	if err == nil {
		t.Fatal("expected error from doRangeRead with expired context")
	}
	if !isTransientReadErr(err) {
		t.Fatalf("body-stage error %v should be classified as transient", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded in error chain, got: %v", err)
	}
}

func TestReadStreamRangeWithRetryRefreshesExpiredReadTarget(t *testing.T) {
	var resolveCalls atomic.Int32
	var expiredCalls atomic.Int32
	var freshCalls atomic.Int32
	data := []byte("0123456789")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/large.bin":
			call := resolveCalls.Add(1)
			token := "expired"
			if call > 1 {
				token = "fresh"
			}
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/%s", r.Host, token))
			w.WriteHeader(http.StatusFound)
		case "/s3/expired":
			expiredCalls.Add(1)
			w.WriteHeader(http.StatusForbidden)
		case "/s3/fresh":
			freshCalls.Add(1)
			if got := r.Header.Get("Range"); got != "bytes=2-5" {
				http.Error(w, "wrong range: "+got, http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[2:6])
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fh := &FileHandle{Path: "/large.bin"}

	got, n, err := fs.readStreamRangeWithRetry(context.Background(), "/large.bin", fh, 2, 4)
	if err != nil {
		t.Fatalf("readStreamRangeWithRetry: %v", err)
	}
	if n != 4 || string(got) != "2345" {
		t.Fatalf("range read = %q, %d; want 2345, 4", got, n)
	}
	if resolveCalls.Load() != 2 {
		t.Fatalf("resolve calls = %d, want 2", resolveCalls.Load())
	}
	if expiredCalls.Load() != 1 || freshCalls.Load() != 1 {
		t.Fatalf("expired/fresh calls = %d/%d, want 1/1", expiredCalls.Load(), freshCalls.Load())
	}
	if fh.ReadTarget == nil || !strings.Contains(fh.ReadTarget.ObjectURL, "/s3/fresh") {
		t.Fatalf("read target = %+v, want fresh target", fh.ReadTarget)
	}
}

func TestReadOpenHandleSnapshotRetriesTransientRangeRead(t *testing.T) {
	var objectCalls atomic.Int32
	data := []byte("snapshot bytes after retry")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/fs/snapshot.bin":
			w.Header().Set("Location", fmt.Sprintf("http://%s/s3/snapshot", r.Host))
			w.WriteHeader(http.StatusFound)
		case "/s3/snapshot":
			call := objectCalls.Add(1)
			if call == 1 {
				w.Header().Set("Content-Length", "1048576")
				w.WriteHeader(http.StatusOK)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
				return
			}
			if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", len(data)-1) {
				t.Errorf("Range = %q, want full snapshot range", got)
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got, err := fs.readOpenHandleSnapshot(ctx, "/snapshot.bin", int64(len(data)))
	if err != nil {
		t.Fatalf("readOpenHandleSnapshot: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("snapshot = %q, want %q", got, data)
	}
	if objectCalls.Load() != 2 {
		t.Fatalf("object calls = %d, want 2", objectCalls.Load())
	}
}

func TestReadTargetForHandleDropsResolvedTargetAfterPathChange(t *testing.T) {
	resolveStarted := make(chan struct{})
	allowResolve := make(chan struct{})
	var once sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs/old.bin" {
			http.NotFound(w, r)
			return
		}
		once.Do(func() { close(resolveStarted) })
		<-allowResolve
		w.Header().Set("Location", fmt.Sprintf("http://%s/s3/old", r.Host))
		w.WriteHeader(http.StatusFound)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fh := &FileHandle{Path: "/old.bin"}

	done := make(chan *client.ReadTarget, 1)
	go func() {
		done <- fs.readTargetForHandle(context.Background(), fh)
	}()

	select {
	case <-resolveStarted:
	case <-time.After(time.Second):
		t.Fatal("resolve did not start")
	}
	fh.Lock()
	fh.Path = "/new.bin"
	fh.Unlock()
	close(allowResolve)

	select {
	case target := <-done:
		if target != nil {
			t.Fatalf("target = %+v, want nil after path change", target)
		}
	case <-time.After(time.Second):
		t.Fatal("readTargetForHandle timed out")
	}
	if fh.ReadTarget != nil {
		t.Fatalf("handle cached target = %+v, want nil", fh.ReadTarget)
	}
}

func TestReadTargetForHandleBypassesSQLitePersistentJournal(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	target := &client.ReadTarget{ObjectURL: "http://old.example/object"}
	fh := &FileHandle{Path: "/repo/workload.db-wal", ReadTarget: target}
	fh.Prefetch = NewPrefetcher(fs.client, fs.remotePath(fh.Path), 4096)
	fh.Prefetch.SetReadTarget(target)

	if got := fs.readTargetForHandle(context.Background(), fh); got != nil {
		t.Fatalf("read target = %+v, want nil", got)
	}
	if fh.ReadTarget != nil {
		t.Fatalf("handle cached target = %+v, want cleared", fh.ReadTarget)
	}
	fh.Prefetch.mu.Lock()
	prefetchTarget := fh.Prefetch.target
	fh.Prefetch.mu.Unlock()
	if prefetchTarget != nil {
		t.Fatalf("prefetch target = %+v, want cleared", prefetchTarget)
	}
}

// TestDoRangeReadBodyTruncationReturnsError verifies that when a server sends
// headers successfully but closes the connection mid-body (truncation),
// doRangeRead surfaces an error instead of silently returning partial data.
// This enables the retry helper to detect and retry body-stage truncation.
func TestDoRangeReadBodyTruncationReturnsError(t *testing.T) {
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		getCalls.Add(1)
		// Hijack the connection: send valid HTTP headers with Content-Length
		// promising 4096 bytes, write only 10 bytes, then close.
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Log("server does not support hijacking")
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, buf, _ := hj.Hijack()
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
		_, _ = buf.Write(make([]byte, 10)) // only 10 of 4096 bytes
		_ = buf.Flush()
		_ = conn.Close() // truncate
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ctx := context.Background()
	_, n, err := fs.doRangeRead(ctx, "/truncate.bin", nil, 0, 4096)
	// doRangeRead must return an error for truncated body, not silent success.
	// The old code (io.ReadFull + ErrUnexpectedEOF filter) would return nil here.
	if err == nil {
		t.Fatalf("expected error from truncated body, got nil with n=%d", n)
	}
}

// TestReadPrefetchNotTriggeredOnFailure verifies that Prefetch.OnRead is NOT
// called when the remote read fails after a prefetch cache miss.
func TestReadPrefetchNotTriggeredOnFailure(t *testing.T) {
	fileSize := int64(defaultReadCacheMaxFileSize + 1024) // above default read-cache admission limit
	var getCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls.Add(1)
			// All GETs fail with 503 (transient)
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	cancel := make(chan struct{})
	var entryOut2 gofuse.EntryOut
	st := fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "prefetch-fail.bin", &entryOut2)
	if st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}

	var openOut gofuse.OpenOut
	st = fs.Open(cancel, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entryOut2.NodeId}, Flags: syscall.O_RDONLY}, &openOut)
	if st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}

	// Verify prefetcher is set
	fh, ok := fs.fileHandles.Get(openOut.Fh)
	if !ok {
		t.Fatal("file handle not found")
	}
	if fh.Prefetch == nil {
		t.Fatal("expected Prefetcher for large read-only file")
	}

	// Read — all remote attempts fail
	_, st = fs.Read(cancel, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0, Size: 4096}, nil)
	if st != gofuse.EIO {
		t.Fatalf("Read status = %v, want EIO", st)
	}

	// Verify OnRead was NOT called by checking prefetcher state.
	// After OnRead, readSize would be set to a non-zero value. Since we
	// have no direct accessor, we verify indirectly: the prefetcher's
	// internal sequential tracker should not have advanced. A successful
	// OnRead would schedule background fetches; with a failing server,
	// those fetches would generate additional GET calls beyond the
	// Read retry attempts.
	//
	// With 1 target resolution attempt plus 1 initial range read and
	// 2 retries = 4 GET calls from Read itself.
	// If OnRead fired, the prefetcher would issue additional GETs.
	wantMaxCalls := int32(2 + readTransientRetryCount)
	if got := getCalls.Load(); got > wantMaxCalls {
		t.Fatalf("GET calls = %d, want <= %d (OnRead should not trigger prefetch on failure)", got, wantMaxCalls)
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
		{"HTTP 502", &client.StatusError{StatusCode: http.StatusBadGateway, Message: "bad gateway"}, true},
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

// TestLocalMutations_NotifyBudget verifies that high-volume local mutations
// do not produce kernel notifyEntry/notifyInode calls.
func TestLocalMutations_NotifyBudget(t *testing.T) {
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// Pre-populate inodes for mutation targets.
	fs.inodes.Lookup("/existing.txt", false, 100, time.Now())
	fs.inodes.Lookup("/config.lock", false, 100, time.Now())
	fs.inodes.Lookup("/config", false, 100, time.Now())
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := fs.notifyCount.Load()
			st := tc.fn()
			after := fs.notifyCount.Load()
			if st != gofuse.OK {
				t.Fatalf("%s returned %v, want OK", tc.name, st)
			}
			if delta := after - before; delta != 0 {
				t.Fatalf("%s produced %d kernel notify calls, want 0", tc.name, delta)
			}
		})
	}
}

func TestRenameNotifiesTargetDentryAndInode(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "rename":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "100")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	fs.inodes.Lookup("/config.lock", false, 100, time.Now())
	fs.inodes.Lookup("/config", false, 0, time.Now())

	before := fs.notifyCount.Load()
	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "config.lock", "config")
	if st != gofuse.OK {
		t.Fatalf("Rename returned %v, want OK", st)
	}
	if delta := fs.notifyCount.Load() - before; delta != 2 {
		t.Fatalf("Rename produced %d kernel notify calls, want 2", delta)
	}
	entry, ok := fs.inodes.GetInode("/config")
	if !ok || entry == 0 {
		t.Fatal("renamed target inode missing")
	}
}

// TestCreateWriteFlushRelease_NoKernelNotify verifies that the full
// create→write→flush→release lifecycle produces zero kernel notify calls
// AND that userspace visibility is preserved (inode size, dirCache).
func TestCreateWriteFlushRelease_NoKernelNotify(t *testing.T) {
	ts, uploadedCh := newTestServer(t)
	defer ts.Close()

	opts := &MountOptions{FlushDebounce: 0}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	before := fs.notifyCount.Load()

	// Create
	var createOut gofuse.CreateOut
	st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
	}, "hook.sample", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}

	wantData := []byte("#!/bin/sh\nexit 0\n")

	// Write
	_, st = fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	}, wantData)
	if st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}

	// Flush
	st = fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})
	if st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}

	// Release
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	// Wait for commit queue upload.
	select {
	case <-uploadedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("commit queue upload timed out")
	}

	// Allow async commit queue success callback to run.
	time.Sleep(50 * time.Millisecond)

	after := fs.notifyCount.Load()
	if delta := after - before; delta != 0 {
		t.Fatalf("create→write→flush→release→commit produced %d kernel notify calls, want 0", delta)
	}

	// Verify userspace state is still correct despite no kernel notify.
	// This is the key behavioral assertion: "no notify AND no visibility regression".

	// 1. Inode size should reflect the written data.
	wantSize := int64(len(wantData))
	entry, ok := fs.inodes.GetEntry(createOut.NodeId)
	if !ok {
		t.Fatal("inode entry not found after commit")
	}
	if entry.Size != wantSize {
		t.Fatalf("inode size = %d, want %d", entry.Size, wantSize)
	}

	// 2. dirCache for parent should have been invalidated (not stale).
	if _, cached := fs.dirCache.Get("/"); cached {
		t.Fatal("parent dirCache should have been invalidated after create+commit")
	}
}

// TestOnCommitQueueSuccess_NoKernelNotify_SeedsReadCache verifies that
// onCommitQueueSuccess updates userspace state (readCache, inode revision/size)
// without producing kernel notify calls. This is the mechanism that ensures
// same-mount read-after-close returns correct content.
func TestOnCommitQueueSuccess_NoKernelNotify_SeedsReadCache(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	// Set up shadowStore with test data (simulates what Flush stages).
	shadowDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow

	wantData := []byte("#!/bin/sh\nexit 0\n")

	// Pre-populate inode and write shadow data (mimics Flush staging).
	ino := fs.inodes.Lookup("/hook.sample", false, 0, time.Now())
	if err := shadow.WriteFull("/hook.sample", wantData, 0); err != nil {
		t.Fatal(err)
	}

	before := fs.notifyCount.Load()

	// Simulate commit queue success callback.
	fs.onCommitQueueSuccess(&CommitEntry{
		Path:  "/hook.sample",
		Inode: ino,
		Size:  int64(len(wantData)),
	}, 42) // committedRev=42

	after := fs.notifyCount.Load()
	if delta := after - before; delta != 0 {
		t.Fatalf("onCommitQueueSuccess produced %d kernel notify calls, want 0", delta)
	}

	// Verify readCache was seeded with correct data.
	cachedData, hit := fs.readCache.Get("/hook.sample", 0)
	if !hit {
		t.Fatal("readCache miss after onCommitQueueSuccess — read-after-close would hit remote")
	}
	if string(cachedData) != string(wantData) {
		t.Fatalf("readCache content = %q, want %q", cachedData, wantData)
	}

	// Verify inode revision and size updated.
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Size != int64(len(wantData)) {
		t.Fatalf("inode size = %d, want %d", entry.Size, len(wantData))
	}

	// Verify dirCache was invalidated.
	if _, cached := fs.dirCache.Get("/"); cached {
		t.Fatal("parent dirCache should have been invalidated")
	}
}

func TestOnCommitQueueSuccessMultipartRefreshesInodeSize(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	filePath := "/final/w0/file-000.txt"
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("initial inode entry not found")
	}
	if entry.Size != 0 {
		t.Fatalf("initial inode size = %d, want 0", entry.Size)
	}

	const wantSize = 32820
	before := fs.notifyCount.Load()
	fs.onCommitQueueSuccess(&CommitEntry{
		Path:  filePath,
		Inode: ino,
		Size:  wantSize,
	}, 0)
	if got := fs.notifyCount.Load() - before; got != 1 {
		t.Fatalf("kernel inode notify count = %d, want 1", got)
	}

	entry, ok = fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Size != wantSize {
		t.Fatalf("inode size = %d, want %d", entry.Size, wantSize)
	}

	lookup := fs.dirCache.Lookup("/final/w0", "file-000.txt")
	if lookup.kind != namespaceLookupPositive {
		t.Fatal("dirCache entry not found")
	}
	if lookup.item.Size != wantSize {
		t.Fatalf("dirCache size = %d, want %d", lookup.item.Size, wantSize)
	}
}

// TestSSEForeignChange_StillNotifiesKernel verifies that SSE-driven
// invalidation (from a foreign actor) still produces kernel notify calls
// through the actual SSEWatcher.handleEvent code path. Only local-initiated
// operations skip notify; external changes must invalidate the kernel cache.
func TestSSEForeignChange_StillNotifiesKernel(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	// Pre-populate an inode so SSE invalidation has something to notify.
	fs.inodes.Lookup("/remote-file.txt", false, 100, time.Now())

	// Create an SSEWatcher with our own actor ID.
	w := &SSEWatcher{fs: fs, actor: "mount-abc"}

	// --- Foreign actor change event → should notify kernel ---
	before := fs.notifyCount.Load()
	w.handleEvent(&client.ChangeEvent{
		Path:  "/remote-file.txt",
		Op:    "write",
		Actor: "other-mount-xyz",
	}, nil)
	afterChange := fs.notifyCount.Load()
	if delta := afterChange - before; delta == 0 {
		t.Fatal("SSE foreign ChangeEvent produced 0 kernel notify calls, want >0")
	}

	// --- Own actor change event → should NOT notify kernel ---
	before2 := fs.notifyCount.Load()
	w.handleEvent(&client.ChangeEvent{
		Path:  "/remote-file.txt",
		Op:    "write",
		Actor: "mount-abc", // same as our actor
	}, nil)
	afterSelf := fs.notifyCount.Load()
	if delta := afterSelf - before2; delta != 0 {
		t.Fatalf("SSE own-actor ChangeEvent produced %d kernel notify calls, want 0", delta)
	}

	// --- Foreign structural reset → should notify kernel ---
	before3 := fs.notifyCount.Load()
	w.handleEvent(nil, &client.ResetEvent{
		Reason: "structural_change",
		Actor:  "other-mount-xyz",
	})
	afterReset := fs.notifyCount.Load()
	if delta := afterReset - before3; delta == 0 {
		t.Fatal("SSE foreign ResetEvent produced 0 kernel notify calls, want >0")
	}

	// --- Own actor structural reset → should NOT notify kernel ---
	before4 := fs.notifyCount.Load()
	w.handleEvent(nil, &client.ResetEvent{
		Reason: "structural_change",
		Actor:  "mount-abc",
	})
	afterSelfReset := fs.notifyCount.Load()
	if delta := afterSelfReset - before4; delta != 0 {
		t.Fatalf("SSE own-actor ResetEvent produced %d kernel notify calls, want 0", delta)
	}
}

// TestSetAttr_NoKernelNotify verifies that SetAttr (truncate) does not
// produce redundant kernel notify. The kernel receives updated attrs
// via the SetAttr reply itself.
func TestSetAttr_NoKernelNotify(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	ino := fs.inodes.Lookup("/trunc.txt", false, 100, time.Now())

	// Open with O_RDWR so there's a dirty handle for truncate.
	fh := &FileHandle{
		Ino:   ino,
		Path:  "/trunc.txt",
		Flags: uint32(syscall.O_RDWR),
		Dirty: NewWriteBuffer("/trunc.txt", 100, 8*1024*1024),
	}
	fhID := fs.fileHandles.Allocate(fh)

	before := fs.notifyCount.Load()

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       fhID,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr: %v", st)
	}

	after := fs.notifyCount.Load()
	if delta := after - before; delta != 0 {
		t.Fatalf("SetAttr produced %d kernel notify calls, want 0", delta)
	}
}

// TestLookupParsesModeHeader verifies that Lookup reads the X-Dat9-Mode header
// from the remote stat response and stores it in the inode entry.
func TestLookupParsesModeHeader(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "9")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Mode", "384") // 0o600 in decimal
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "secure.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}

	// Verify the EntryOut attr reflects the mode.
	if out.Mode != syscall.S_IFREG|0o600 {
		t.Errorf("EntryOut mode = %o, want %o", out.Mode, syscall.S_IFREG|0o600)
	}

	// Verify the inode table also stores the mode.
	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Mode != 0o600 {
		t.Errorf("inode mode = %o, want 0o600", entry.Mode)
	}
}

// TestLookupParsesModeHeaderDir verifies that Lookup reads X-Dat9-Mode for directories.
func TestLookupParsesModeHeaderDir(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "0")
		w.Header().Set("X-Dat9-IsDir", "true")
		w.Header().Set("X-Dat9-Mode", "448") // 0o700 in decimal
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "privatedir", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}

	if out.Mode != syscall.S_IFDIR|0o700 {
		t.Errorf("EntryOut mode = %o, want %o", out.Mode, syscall.S_IFDIR|0o700)
	}

	entry, ok := fs.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("inode entry not found")
	}
	if entry.Mode != 0o700 {
		t.Errorf("inode mode = %o, want 0o700", entry.Mode)
	}
}
