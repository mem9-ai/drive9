package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

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

func TestReadDirPlusRecreatesStaleSnapshotInode(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	fileIno := fs.inodes.Lookup("/dir/file.txt", false, 1, time.Now())
	fs.inodes.Forget(fileIno, 1)
	if _, ok := fs.inodes.GetEntry(fileIno); ok {
		t.Fatalf("stale file inode %d is still mapped", fileIno)
	}

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
	size := int64(2 * 1024 * 1024) // above default read-cache admission limit
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
	if opts.NegativeEntryTTL != 10*time.Second {
		t.Fatalf("default NegativeEntryTTL = %v, want 10s", opts.NegativeEntryTTL)
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

func TestXAttr_GetReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	_, st := fs.GetXAttr(nil, &gofuse.InHeader{NodeId: 1}, "user.test", nil)
	if st != gofuse.ENOATTR {
		t.Fatalf("GetXAttr status = %v, want ENOATTR", st)
	}
}

func TestXAttr_ListReturnsEmpty(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

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
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	st := fs.SetXAttr(nil, &gofuse.SetXAttrIn{}, "user.test", []byte("val"))
	if st != gofuse.OK {
		t.Fatalf("SetXAttr status = %v, want OK", st)
	}
}

func TestXAttr_RemoveReturnsENOATTR(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fs := NewDat9FS(newTestClient(ts.URL), opts)
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
	fileSize := int64(2 << 20) // above default read-cache admission limit
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
		{
			name: "Rename",
			fn: func() gofuse.Status {
				return fs.Rename(nil, &gofuse.RenameIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Newdir:   1,
				}, "config.lock", "config")
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
