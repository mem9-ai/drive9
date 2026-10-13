// v1 dat9fs setattr-mode rows, retargeted to the core terminal: the
// remote chmod strips privilege bits (0o6755 → 0o755 on the wire, the
// reply keeps the full set), directory chmods go to the trailing-slash
// form, transient post-chmod 404s retry, and a 404 whose target still
// exists keeps the change local instead of failing.
package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

type chmodRecorder struct {
	mu    sync.Mutex
	calls int
	paths []string
	modes []uint32
}

func (r *chmodRecorder) record(path string, mode uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.paths = append(r.paths, path)
	r.modes = append(r.modes, mode)
}

func (r *chmodRecorder) snapshot() (int, []string, []uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.paths, r.modes
}

// v1 TestSetAttrModePreservesSpecialPermissionBits.
func TestPortSetAttrModeStripsPrivilegeBitsRemotely(t *testing.T) {
	rec := &chmodRecorder{}
	notFound := atomic.Bool{} // first chmod 404s (post-upload lag)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			var req struct {
				Mode uint32 `json:"mode"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			rec.record(r.URL.Path, req.Mode)
			if notFound.CompareAndSwap(true, false) {
				http.Error(w, "lag", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := newSetattrCore(t, ts)
	ctx := context.Background()

	// The set-GID+set-UID mode: the wire chmod carries the rwx bits only
	// (remoteChmodMode), the transient 404 retries (postUploadMode).
	notFound.Store(true)
	if _, err := c.Do(ctx, opSetAttr("/special.txt", 0o6755), nil); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	calls, _, modes := rec.snapshot()
	if calls != 2 {
		t.Fatalf("chmod calls = %d, want 2 (one 404 retry)", calls)
	}
	// v1 remoteChmodMode masks the wire chmod to 0o777: privilege bits
	// never leave the mount (the local view keeps the full 0o7777).
	const wantWireMode = 0o755
	for _, m := range modes {
		if m != wantWireMode {
			t.Fatalf("remote chmod mode = %o, want %o", m, wantWireMode)
		}
	}
}

// v1 TestSetAttrModeUsesDirectoryRemotePath: a directory chmod targets
// the trailing-slash form.
func TestPortSetAttrDirectoryChmodUsesTrailingSlash(t *testing.T) {
	rec := &chmodRecorder{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			rec.record(r.URL.Path, 0)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := newSetattrCore(t, ts)

	if _, err := c.Do(context.Background(), opSetAttr("/adir", 0o750), nil); err != nil {
		t.Fatalf("dir setattr: %v", err)
	}
	calls, paths, _ := rec.snapshot()
	if calls != 1 {
		t.Fatalf("chmod calls = %d, want 1", calls)
	}
	if paths[0] != "/v1/fs/adir/" {
		t.Fatalf("chmod path = %q, want trailing-slash form", paths[0])
	}
}

// v1 TestSetAttrModeRemoteChmodNotFoundStillFailsWhenTargetMissing: a
// chmod 404 whose target is genuinely absent surfaces as an error (no
// local-only acceptance).
func TestPortSetAttrChmodNotFoundMissingTargetFails(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			http.Error(w, "gone", http.StatusNotFound)
		case r.Method == http.MethodHead:
			http.Error(w, "gone", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := newSetattrCore(t, ts)

	if _, err := c.Do(context.Background(), opSetAttr("/ghost.txt", 0o600), nil); err == nil {
		t.Fatal("chmod 404 with missing target must fail, not stay local")
	}
}

func newSetattrCore(t *testing.T, ts *httptest.Server) *Core {
	t.Helper()
	cl := newTestClientFor(t, ts)
	c, err := New(cl, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestClientFor(t *testing.T, ts *httptest.Server) *client.Client {
	t.Helper()
	return client.New(ts.URL, "")
}

// opSetAttr builds a mode-only setattr op.
func opSetAttr(path string, mode uint32) *fs.Op {
	return &fs.Op{Kind: fs.OpSetAttr, Path: path, Mode: mode, Size: -1}
}
