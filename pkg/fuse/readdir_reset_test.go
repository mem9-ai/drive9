package fuse

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestReadDirectoryFirstPageSurvivesSSEReset(t *testing.T) {
	for _, plus := range []bool{false, true} {
		name := "readdir"
		if plus {
			name = "readdirplus"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					close(started)
					<-release
					_, _ = w.Write([]byte(`{"entries":[{"name":"stale-before-reset","isdir":true}]}`))
					return
				}
				_, _ = w.Write([]byte(`{"entries":[{"name":"fresh-after-reset","isdir":true}]}`))
			}))
			defer server.Close()
			defer unblock()
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
			open := &gofuse.OpenOut{}
			if status := fs.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: 1}}, open); status != gofuse.OK {
				t.Fatal(status)
			}
			out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
			done := make(chan gofuse.Status, 1)
			go func() {
				in := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: open.Fh, Size: 4096}
				if plus {
					done <- fs.ReadDirPlus(nil, in, out)
				} else {
					done <- fs.ReadDir(nil, in, out)
				}
			}()
			<-started
			(&SSEWatcher{fs: fs}).handleReset()
			unblock()
			if status := <-done; status != gofuse.OK {
				t.Fatalf("first directory page after reset = %v, want OK", status)
			}
			buf := dirEntryListBuf(t, out)
			if !bytes.Contains(buf, []byte("fresh-after-reset")) || bytes.Contains(buf, []byte("stale-before-reset")) {
				t.Fatalf("directory page did not contain only the fresh snapshot: %q", buf)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("List calls = %d, want exactly 2", got)
			}
		})
	}
}

// The handle is not registered so reset can complete while its publication
// mutex is held. This isolates the second generation check from listDir's check.
func TestDirectoryHandleCommitRejectsSSEReset(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte(`{"entries":[{"name":"stale-before-reset","isdir":true}]}`))
	}))
	defer server.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
	dh := &DirHandle{Ino: 1, Path: "/", entriesGeneration: fs.mountViewGeneration.Load()}
	done := make(chan error, 1)
	go func() {
		_, _, err := fs.loadDirHandleEntries(context.Background(), dh, false)
		done <- err
	}()
	<-started
	dh.mu.Lock()
	close(release)
	// Wait for the actual cache publication, not an estimated duration. The
	// test deadline bounds a broken implementation without a scheduling sleep.
	for {
		if _, ok := fs.dirCache.Get("/"); ok {
			break
		}
		if t.Context().Err() != nil {
			dh.mu.Unlock()
			t.Fatal("List did not publish the directory cache")
		}
		runtime.Gosched()
	}
	(&SSEWatcher{fs: fs}).handleReset()
	dh.mu.Unlock()
	if err := <-done; !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("handle publication error = %v, want EAGAIN", err)
	}
	if dh.Entries != nil {
		t.Fatal("handle published entries from the invalidated generation")
	}
}

func TestDirectoryFirstPageRetriesHandleCommitReset(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			_, _ = w.Write([]byte(`{"entries":[{"name":"discarded-before-commit","isdir":true}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"entries":[{"name":"current","isdir":true}]}`))
	}))
	defer server.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
	dh := &DirHandle{Ino: 1, Path: "/", entriesGeneration: fs.mountViewGeneration.Load()}
	done := make(chan error, 1)
	go func() {
		entries, generation, err := fs.lockDirectoryPage(context.Background(), dh, 0)
		if err == nil {
			if len(entries) != 1 || entries[0].Name != "current" || generation != fs.mountViewGeneration.Load() {
				err = errors.New("wrong directory snapshot after handle publication retry")
			}
			fs.mountViewMu.RUnlock()
		}
		done <- err
	}()
	<-started
	dh.mu.Lock()
	close(release)
	for {
		if _, ok := fs.dirCache.Get("/"); ok {
			break
		}
		if t.Context().Err() != nil {
			dh.mu.Unlock()
			t.Fatal("List did not publish the directory cache")
		}
		runtime.Gosched()
	}
	(&SSEWatcher{fs: fs}).handleReset()
	dh.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("List calls = %d, want exactly 2", got)
	}
}

func TestDirectoryPageRetryIsBounded(t *testing.T) {
	var fs *Dat9FS
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		(&SSEWatcher{fs: fs}).handleReset()
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs = NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
	dh := &DirHandle{Ino: 1, Path: "/"}
	_, _, err := fs.lockDirectoryPage(context.Background(), dh, 0)
	if !errors.Is(err, errDirectoryViewChanged) || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("error = %v, want bounded directory-view EAGAIN", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("List calls = %d, want retry budget of 2", got)
	}
}

func TestDirectoryPageDoesNotRetryBackendErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			fs, calls, closeServer := newNamespaceAuthorizationTestFS(t, status)
			defer closeServer()
			_, _, err := fs.lockDirectoryPage(context.Background(), &DirHandle{Ino: 1, Path: "/"}, 0)
			var backendError *client.StatusError
			if !errors.As(err, &backendError) || backendError.StatusCode != status || errors.Is(err, errDirectoryViewChanged) {
				t.Fatalf("error = %v, want original backend status %d", err, status)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("backend calls = %d, want no retry", got)
			}
		})
	}
}

func TestDirectoryPageCancellationDoesNotRetry(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := fs.lockDirectoryPage(ctx, &DirHandle{Ino: 1, Path: "/"}, 0)
		done <- err
	}()
	<-started
	(&SSEWatcher{fs: fs}).handleReset()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("List calls = %d, want no retry after cancellation", got)
	}
}

func TestDirectoryContinuationRejectsReset(t *testing.T) {
	for _, plus := range []bool{false, true} {
		name := "readdir"
		if plus {
			name = "readdirplus"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(`{"entries":[{"name":"alpha","isdir":true},{"name":"bravo","isdir":true},{"name":"charlie","isdir":true}]}`))
			}))
			defer server.Close()
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(client.NewWithToken(server.URL, "test"), opts)
			open := &gofuse.OpenOut{}
			if status := fs.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: 1}}, open); status != gofuse.OK {
				t.Fatal(status)
			}
			read := func(offset uint64, size int) (*gofuse.DirEntryList, gofuse.Status) {
				out := gofuse.NewDirEntryList(make([]byte, size), offset)
				in := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}, Fh: open.Fh, Offset: offset, Size: uint32(size)}
				if plus {
					return out, fs.ReadDirPlus(nil, in, out)
				}
				return out, fs.ReadDir(nil, in, out)
			}
			size := 96
			if plus {
				size += 3 * int(reflect.TypeOf(gofuse.EntryOut{}).Size())
			}
			first, status := read(0, size)
			if status != gofuse.OK {
				t.Fatal(status)
			}
			buf := dirEntryListBuf(t, first)
			if !bytes.Contains(buf, []byte("alpha")) || bytes.Contains(buf, []byte("bravo")) {
				t.Fatalf("expected a partial first page ending with alpha: %q", buf)
			}
			(&SSEWatcher{fs: fs}).handleReset()
			next, status := read(3, 4096)
			if status != gofuse.Status(syscall.EAGAIN) || len(dirEntryListBuf(t, next)) != 0 {
				t.Fatalf("continuation status = %v, bytes = %d; want EAGAIN without mixed entries", status, len(dirEntryListBuf(t, next)))
			}
			if calls.Load() != 1 {
				t.Fatal("continuation fetched a different directory generation")
			}
			if _, status := read(0, 4096); status != gofuse.OK {
				t.Fatalf("explicit rewind failed: %v", status)
			}
		})
	}
}
