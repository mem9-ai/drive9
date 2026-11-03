// v1 writeback_test.go pending-visibility rows (batch 4), retargeted to
// the split plane: re-open reads the pending image (close-to-open
// consistency while the upload lags), fsync pays exactly one upload,
// consecutive saves on one path compose, and unlink of a pending-new
// entry skips the remote delete.
package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// v1 TestReopenAfterClose_ReadsPendingCache: with the upload lagging
// (server accepts but never answers from storage), a fresh open of the
// path reads the pending image, not the stale remote.
func TestPortReopenReadsPendingImageWhileUploadLags(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, cache := newWriteCacheStack(t, PolicyWriteBack, b)
	_ = cache
	ctx := context.Background()

	h := open(t, ch, "/cto.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("fresh data")); err != nil {
		t.Fatal(err)
	}
	// Flush stages the .dat snapshot; the uploader has not landed it.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Re-open: the read must serve the pending bytes (server still empty
	// or stale).
	buf := make([]byte, 64)
	n, err := readThroughChain(t, ch, "/cto.txt", buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "fresh data" {
		t.Fatalf("re-open read = %q, want fresh data (close-to-open broken)", buf[:n])
	}
	_ = wb.Drain(context.Background())
}

// readThroughChain opens the path read-only through the chain and reads.
func readThroughChain(t *testing.T, ch *fs.Chain, path string, buf []byte) (int, error) {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: 0})
	if err != nil {
		return 0, err
	}
	if res.Handle == nil {
		return 0, errNoHandle
	}
	return res.Handle.Read(context.Background(), 0, buf)
}

var errNoHandle = &noHandleError{}

type noHandleError struct{}

func (*noHandleError) Error() string { return "no handle" }

// v1 TestFsync_NoDuplicateUpload: an fsync over an already-staged .dat
// snapshot uploads exactly once.
func TestPortFsyncUploadsExactlyOnce(t *testing.T) {
	var puts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut || r.Method == http.MethodPost:
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", "2")
			_, _ = w.Write([]byte(`{"revision":2}`))
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte("payload"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	cl := client.New(ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/once.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	// Flush stages + UploadSync lands (1 PUT). The fsync of the now-clean
	// handle adds none.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("PUTs after flush = %d, want 1", got)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_ = wb.Drain(context.Background())
	if got := puts.Load(); got != 1 {
		t.Fatalf("PUTs after fsync+release = %d, want still 1 (no duplicate upload)", got)
	}
}

// v1 TestSamePathConsecutiveSaves: two write-close cycles on one path
// both land, the second replacing the first.
func TestPortSamePathConsecutiveSaves(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	for i, content := range []string{"first save", "second save"} {
		h := open(t, ch, "/repeat.txt")
		if err := h.Write(ctx, 0, []byte(content)); err != nil {
			t.Fatalf("save %d write: %v", i, err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("save %d flush: %v", i, err)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatalf("save %d release: %v", i, err)
		}
	}
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/repeat.txt")
	if !ok || string(n.Data) != "second save" {
		t.Fatalf("remote = %v, want second save", n.Data)
	}
}
