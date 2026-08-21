package fuse

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestNamespaceAuthorizationErrorsResetMountView(t *testing.T) {
	tests := []struct {
		name string
		call func(*Dat9FS) error
	}{
		{
			name: "stat",
			call: func(fs *Dat9FS) error {
				_, _, err := fs.statWithTransientRetry(nil, "/denied", true)
				return err
			},
		},
		{
			name: "lookup list fallback",
			call: func(fs *Dat9FS) error {
				_, _, err := fs.lookupListWithRetry(nil, "/denied")
				return err
			},
		},
		{
			name: "directory list",
			call: func(fs *Dat9FS) error {
				_, err := fs.listDir(context.Background(), "/denied")
				return err
			},
		},
	}

	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, tt := range tests {
			t.Run(http.StatusText(status)+"/"+tt.name, func(t *testing.T) {
				fs, calls, closeServer := newNamespaceAuthorizationTestFS(t, status)
				defer closeServer()
				seedMountViewCaches(fs)

				err := tt.call(fs)
				if err == nil {
					t.Fatal("namespace call error = nil, want authorization error")
				}
				if got := httpToFuseStatus(err); got != gofuse.EACCES {
					t.Fatalf("FUSE status = %v, want EACCES", got)
				}
				if got := calls.Load(); got != 1 {
					t.Fatalf("remote calls = %d, want 1 without authorization retry", got)
				}
				assertMountViewCachesCleared(t, fs)
			})
		}
	}
}

func TestNamespaceNotFoundDoesNotResetMountView(t *testing.T) {
	fs, calls, closeServer := newNamespaceAuthorizationTestFS(t, http.StatusNotFound)
	defer closeServer()
	seedMountViewCaches(fs)

	_, err := fs.listDir(context.Background(), "/missing")
	if err == nil {
		t.Fatal("listDir error = nil, want not found")
	}
	if got := httpToFuseStatus(err); got != gofuse.ENOENT {
		t.Fatalf("FUSE status = %v, want ENOENT", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("remote calls = %d, want 1", got)
	}
	if _, ok := fs.readCache.Get("/stale.txt", 1); !ok {
		t.Fatal("read cache was cleared by 404")
	}
	if _, ok := fs.dirCache.Get("/stale"); !ok {
		t.Fatal("directory cache was cleared by 404")
	}
}

func TestBatchStatAuthorizationErrorResetsMountView(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"entries":[{"name":"visible.txt","isdir":false}]}`))
			return
		}
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)
	seedMountViewCaches(fs)

	_, err := fs.listDir(context.Background(), "/project")
	if err == nil {
		t.Fatal("listDir error = nil, want batch-stat authorization error")
	}
	if got := httpToFuseStatus(err); got != gofuse.EACCES {
		t.Fatalf("FUSE status = %v, want EACCES", got)
	}
	assertMountViewCachesCleared(t, fs)
}

func TestBatchStatPerPathForbiddenKeepsListOnlyEntry(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"entries":[{"name":"visible.txt","isdir":false}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"path":"/project/visible.txt","status":403,"error":"fs access denied"}]}`))
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)
	seedMountViewCaches(fs)

	entries, err := fs.listDir(context.Background(), "/project")
	if err != nil {
		t.Fatalf("listDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "visible.txt" {
		t.Fatalf("entries = %#v, want list-only visible.txt", entries)
	}
	if _, ok := fs.dirCache.Get("/stale"); !ok {
		t.Fatal("directory cache was cleared by a per-path 403")
	}
	if _, ok := fs.readCache.Get("/stale.txt", 1); !ok {
		t.Fatal("read cache was cleared by a per-path 403")
	}
}

func TestResetMountViewInvalidatesOpenDirectoryHandleEntries(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken("http://localhost", "scoped"), opts)
	dh := &DirHandle{
		Ino:               1,
		Path:              "/",
		Entries:           []DirEntry{{Name: "stale"}},
		entriesGeneration: fs.mountViewGeneration.Load(),
	}
	fs.dirHandles.Allocate(dh)

	fs.resetMountView()

	dh.mu.Lock()
	defer dh.mu.Unlock()
	if dh.Entries != nil {
		t.Fatalf("directory handle entries = %#v, want invalidated", dh.Entries)
	}
	if dh.entriesGeneration != fs.mountViewGeneration.Load() {
		t.Fatalf("handle generation = %d, mount generation = %d", dh.entriesGeneration, fs.mountViewGeneration.Load())
	}
}

func TestDirectoryListDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	listStarted := make(chan struct{})
	releaseList := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(listStarted)
		<-releaseList
		_, _ = w.Write([]byte(`{"entries":[{"name":"stale","isdir":true}]}`))
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)
	dh := &DirHandle{Ino: 1, Path: "/", entriesGeneration: fs.mountViewGeneration.Load()}
	fs.dirHandles.Allocate(dh)

	done := make(chan error, 1)
	go func() {
		_, _, err := fs.loadDirHandleEntries(context.Background(), dh)
		done <- err
	}()
	<-listStarted
	fs.resetMountView()
	close(releaseList)

	err := <-done
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("loadDirHandleEntries error = %v, want EAGAIN after reset", err)
	}
	dh.mu.Lock()
	defer dh.mu.Unlock()
	if dh.Entries != nil {
		t.Fatalf("directory handle retained in-flight entries: %#v", dh.Entries)
	}
	if cached, ok := fs.dirCache.Get("/"); ok {
		t.Fatalf("directory cache retained in-flight entries after reset: %#v", cached)
	}
}

func TestDirectoryPrefetchDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	prefetchStarted := make(chan struct{})
	releasePrefetch := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"entries":[{"name":"prefetch.txt","isdir":false,"size":5,"revision":7}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			close(prefetchStarted)
			<-releasePrefetch
			_, _ = w.Write([]byte(`{"results":[{"path":"/prefetch.txt","status":200,"data":"aGVsbG8=","size":5,"revision":7}]}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()
	opts := &MountOptions{ReadDirPrefetch: true, PrefetchTimeout: time.Second}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)

	done := make(chan error, 1)
	go func() {
		_, err := fs.listDir(context.Background(), "/")
		done <- err
	}()
	<-prefetchStarted
	fs.resetMountView()
	close(releasePrefetch)

	if err := <-done; err != nil {
		t.Fatalf("listDir error = %v, want nil", err)
	}
	if _, ok := fs.dirCache.Get("/"); ok {
		t.Fatal("directory cache was repopulated after reset")
	}
	if _, ok := fs.readCache.Get("/prefetch.txt", 7); ok {
		t.Fatal("read cache was repopulated after reset")
	}
}

func TestLookupListDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	listStarted := make(chan struct{})
	releaseList := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(listStarted)
		<-releaseList
		_, _ = w.Write([]byte(`{"entries":[{"name":"stale","isdir":true}]}`))
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)

	done := make(chan error, 1)
	go func() {
		_, _, err := fs.lookupListWithRetry(nil, "/")
		done <- err
	}()
	<-listStarted
	fs.resetMountView()
	close(releaseList)

	if err := <-done; !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("lookupListWithRetry error = %v, want EAGAIN after reset", err)
	}
	if cached, ok := fs.dirCache.Get("/"); ok {
		t.Fatalf("directory cache retained in-flight lookup entries after reset: %#v", cached)
	}
}

func TestEmptyDirectoryHandleRemainsLoaded(t *testing.T) {
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		listCalls.Add(1)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)
	dh := &DirHandle{Ino: 1, Path: "/", entriesGeneration: fs.mountViewGeneration.Load()}

	if entries, _, err := fs.loadDirHandleEntries(context.Background(), dh); err != nil || len(entries) != 0 {
		t.Fatalf("first load = %#v, %v; want empty loaded directory", entries, err)
	}
	if dh.Entries == nil {
		t.Fatal("empty directory handle was not marked loaded")
	}
	fs.dirCache.Invalidate("/")
	if entries, _, err := fs.loadDirHandleEntries(context.Background(), dh); err != nil || len(entries) != 0 {
		t.Fatalf("second load = %#v, %v; want cached empty directory", entries, err)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("remote list calls = %d, want 1", got)
	}
}

func TestResetMountViewInvalidatesPrefetchWithoutWaitingForHandle(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://fresh.example/object")
		w.WriteHeader(http.StatusFound)
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)
	oldTarget := &client.ReadTarget{ObjectURL: "http://stale.example/object"}
	prefetch := NewPrefetcher(fs.client, "/large.bin", 1024)
	prefetch.SetReadTarget(oldTarget)
	ready := make(chan struct{})
	close(ready)
	prefetch.mu.Lock()
	prefetch.cache[0] = &prefetchBlock{offset: 0, data: []byte("stale"), ready: ready}
	prefetch.mu.Unlock()
	fh := &FileHandle{Ino: 2, Path: "/large.bin", ReadTarget: oldTarget, Prefetch: prefetch}
	fs.fileHandles.Allocate(fh)

	fh.Lock()
	done := make(chan struct{})
	go func() {
		fs.resetMountView()
		close(done)
	}()
	blocked := false
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		blocked = true
	}
	fh.Unlock()
	if blocked {
		<-done
		t.Fatal("resetMountView waited for a locked file handle")
	}
	if data, ok := prefetch.Get(0, len("stale")); ok {
		t.Fatalf("prefetch retained stale data after reset: %q", data)
	}
	target := fs.readTargetForHandle(context.Background(), fh)
	if target == nil || target.ObjectURL != "http://fresh.example/object" {
		t.Fatalf("read target = %+v, want refreshed target", target)
	}
	prefetch.mu.Lock()
	prefetchClosed := prefetch.closed
	prefetchTarget := prefetch.target
	prefetch.mu.Unlock()
	if prefetchClosed || prefetchTarget != target {
		t.Fatalf("prefetch state = closed:%t target:%+v, want active refreshed target", prefetchClosed, prefetchTarget)
	}
}

func TestRmdirRejectsDirectoryListWhenMountViewResetsInFlight(t *testing.T) {
	prefetchStarted := make(chan struct{})
	releasePrefetch := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"entries":[{"name":"child.txt","isdir":false,"size":5,"revision":7}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-read-small":
			close(prefetchStarted)
			<-releasePrefetch
			_, _ = w.Write([]byte(`{"results":[{"path":"/dir/child.txt","status":200,"data":"aGVsbG8=","size":5,"revision":7}]}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()
	opts := &MountOptions{LayerRef: "layer", ReadDirPrefetch: true, PrefetchTimeout: time.Second}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)

	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	}()
	<-prefetchStarted
	fs.resetMountView()
	close(releasePrefetch)

	if got := <-done; got != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("Rmdir status = %v, want EAGAIN after reset", got)
	}
}

func TestResetMountViewPublishesGenerationAfterInvalidation(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken("http://localhost", "scoped"), opts)
	dh := &DirHandle{Ino: 1, Path: "/"}
	fs.dirHandles.Allocate(dh)
	dh.mu.Lock()
	oldGeneration := fs.mountViewGeneration.Load()
	done := make(chan struct{})
	go func() {
		fs.resetMountView()
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	writerLocked := false
	for time.Now().Before(deadline) {
		if fs.mountViewMu.TryRLock() {
			fs.mountViewMu.RUnlock()
			runtime.Gosched()
			continue
		}
		writerLocked = true
		break
	}
	observedGeneration := fs.mountViewGeneration.Load()
	dh.mu.Unlock()
	<-done

	if !writerLocked {
		t.Fatal("resetMountView did not acquire the mount-view write lock")
	}
	if observedGeneration != oldGeneration {
		t.Fatalf("generation became visible during invalidation: got %d, want %d", observedGeneration, oldGeneration)
	}
	if got := fs.mountViewGeneration.Load(); got != oldGeneration+1 {
		t.Fatalf("generation after reset = %d, want %d", got, oldGeneration+1)
	}
}

func TestStatDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			statStarted := make(chan struct{})
			releaseStat := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					_, _ = w.Write([]byte(`{"entries":[]}`))
					return
				}
				close(statStarted)
				<-releaseStat
				if status == http.StatusOK {
					w.Header().Set("Content-Length", "5")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "7")
				}
				w.WriteHeader(status)
			}))
			defer ts.Close()
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts)

			done := make(chan gofuse.Status, 1)
			go func() {
				done <- fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "file.txt", &gofuse.EntryOut{})
			}()
			<-statStarted
			fs.resetMountView()
			close(releaseStat)

			if got := <-done; got != gofuse.Status(syscall.EAGAIN) {
				t.Fatalf("Lookup status = %v, want EAGAIN after reset", got)
			}
		})
	}
}

func TestSmallReadDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	data := []byte("fresh")
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	fs, ino, cleanup := newTestDat9FS(t, int64(len(data)), func(w http.ResponseWriter, _ *http.Request) {
		close(readStarted)
		<-releaseRead
		_, _ = w.Write(data)
	})
	defer cleanup()
	fs.inodes.UpdateRevision(ino, 1)
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fh := openDat9FSTestHandle(t, fs, ino, "/file.bin")
	defer fs.fileHandles.Delete(fh)
	diskKey, ok := fs.diskReadCacheKey("/file.bin", mustGetInodeEntry(t, fs, ino), 0, int64(len(data)))
	if !ok {
		t.Fatal("disk cache key unavailable")
	}

	done := make(chan gofuse.Status, 1)
	go func() {
		_, status, _ := readDat9FSTestRange(fs, ino, fh, 0, len(data))
		done <- status
	}()
	<-readStarted
	fs.resetMountView()
	close(releaseRead)

	if got := <-done; got != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("Read status = %v, want EAGAIN after reset", got)
	}
	if cached, ok := fs.readCache.Get("/file.bin", 1); ok {
		t.Fatalf("read cache retained in-flight data after reset: %q", cached)
	}
	if cached, ok := fs.diskReadCache.Get(diskKey); ok {
		t.Fatalf("disk read cache retained in-flight data after reset: %q", cached)
	}
}

func TestRangeReadDiscardedWhenMountViewResetsInFlight(t *testing.T) {
	data := bytes.Repeat([]byte("r"), defaultReadCacheMaxFileSize+1024)
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	fs, ino, cleanup := newTestDat9FSWithRangeObject(t, int64(len(data)), func(w http.ResponseWriter, _ *http.Request) {
		close(readStarted)
		<-releaseRead
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[128:192])
	})
	defer cleanup()
	fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
	fs.inodes.UpdateRevision(ino, 1)
	fh := openDat9FSTestHandle(t, fs, ino, "/file.bin")
	defer fs.fileHandles.Delete(fh)
	diskKey, ok := fs.diskReadCacheKey("/file.bin", mustGetInodeEntry(t, fs, ino), 128, 64)
	if !ok {
		t.Fatal("disk cache key unavailable")
	}

	done := make(chan gofuse.Status, 1)
	go func() {
		_, status, _ := readDat9FSTestRange(fs, ino, fh, 128, 64)
		done <- status
	}()
	<-readStarted
	fs.resetMountView()
	close(releaseRead)

	if got := <-done; got != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("Read status = %v, want EAGAIN after reset", got)
	}
	if cached, ok := fs.diskReadCache.Get(diskKey); ok {
		t.Fatalf("disk read cache retained in-flight range after reset: %q", cached)
	}
}

func TestMountViewReadGuardRejectsStaleGeneration(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken("http://localhost", "scoped"), opts)
	staleGeneration := fs.mountViewGeneration.Load()
	fs.resetMountView()

	if fs.lockMountViewRead(staleGeneration) {
		fs.mountViewMu.RUnlock()
		t.Fatal("stale generation acquired mount-view read guard")
	}
}

func TestMountViewReadGuardExcludesResetWriter(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken("http://localhost", "scoped"), opts)
	generation := fs.mountViewGeneration.Load()
	if !fs.lockMountViewRead(generation) {
		t.Fatal("current generation failed to acquire mount-view read guard")
	}
	if fs.mountViewMu.TryLock() {
		fs.mountViewMu.Unlock()
		fs.mountViewMu.RUnlock()
		t.Fatal("reset writer acquired mount-view lock during response guard")
	}
	fs.mountViewMu.RUnlock()
	if !fs.mountViewMu.TryLock() {
		t.Fatal("reset writer could not acquire mount-view lock after response guard")
	}
	fs.mountViewMu.Unlock()
}

func newNamespaceAuthorizationTestFS(t *testing.T, status int) (*Dat9FS, *atomic.Int32, func()) {
	t.Helper()
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, http.StatusText(status), status)
	}))
	opts := &MountOptions{}
	opts.setDefaults()
	return NewDat9FS(client.NewWithToken(ts.URL, "scoped"), opts), &calls, ts.Close
}

func seedMountViewCaches(fs *Dat9FS) {
	fs.readCache.Put("/stale.txt", []byte("stale"), 1)
	fs.dirCache.Put("/stale", []CachedFileInfo{{Name: "stale.txt", Revision: 1}})
}

func assertMountViewCachesCleared(t *testing.T, fs *Dat9FS) {
	t.Helper()
	if _, ok := fs.readCache.Get("/stale.txt", 1); ok {
		t.Fatal("read cache retained stale entry after authorization error")
	}
	if _, ok := fs.dirCache.Get("/stale"); ok {
		t.Fatal("directory cache retained stale entry after authorization error")
	}
}
