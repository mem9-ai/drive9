package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

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
				_, err := fs.statWithTransientRetry(nil, "/denied", true)
				return err
			},
		},
		{
			name: "lookup list fallback",
			call: func(fs *Dat9FS) error {
				_, err := fs.lookupListWithRetry(nil, "/denied")
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

	entries, err := fs.listDir(context.Background(), "/project")
	if err != nil {
		t.Fatalf("listDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "visible.txt" {
		t.Fatalf("entries = %#v, want list-only visible.txt", entries)
	}
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
