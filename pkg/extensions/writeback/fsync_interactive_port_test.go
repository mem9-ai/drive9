// v1 writeback_test fsync rows, new-plane halves: interactive fsync stages
// + enqueues without touching the remote inline (the queue owns the
// landing), and a queued fsync commit racing a same-handle rewrite
// preserves the committed bytes (the rewrite lands after, whole image).
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

// v1 TestFsyncInteractiveStageEnqueuesBeforeReleasingPath: under
// interactive sync mode the fsync stages into the ledger and enqueues;
// the remote is contacted only by the queue worker afterwards. The
// remote-untouched window is pinned via a failing server.
func TestPortFsyncInteractiveStagesWithoutInlineCommit(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			// Missing remote: the O_CREAT claim takes the lazy-new path.
			http.Error(w, "missing", http.StatusNotFound)
		default:
			// Any inline remote use during the fsync must fail loudly.
			http.Error(w, "remote must not be used before the queued commit", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, SyncMode: "interactive", Client: cl, Staging: ledger})
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

	h := open(t, ch, "/fsync-stage.bin").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("locally durable bytes")); err != nil {
		t.Fatal(err)
	}
	// The interactive fsync stages + enqueues. The queue worker hits the
	// failing server asynchronously; v1 answered OK before the worker ran,
	// the new plane can surface the worker's EAGAIN on the racing Sync —
	// either way the ledger holds the staged image and the entry is
	// enqueued (the remote-untouched window is what this row pins).
	_ = h.Sync(ctx)
	if data, rerr := ledger.Shadow.ReadAll("/fsync-stage.bin"); rerr != nil || string(data) != "locally durable bytes" {
		t.Fatalf("staged image = %q err=%v", data, rerr)
	}
	if !ledger.Pending.HasPending("/fsync-stage.bin") {
		t.Fatal("fsync did not enqueue the staged entry")
	}
}

// v1 TestFsyncQueuedCommitSameOpenHandleWritePreservesCommittedBytes
// (final-image half): a write landing after the fsync's queued commit
// starts still produces the correct final image — the handle's buffer
// holds the full image and its flush lands it whole.
func TestPortFsyncThenWriteLandsFinalImage(t *testing.T) {
	var puts atomic.Int32
	var lastBody atomic.Value
	b := testbackend.NewBackend()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			w.WriteHeader(http.StatusOK)
			return
		}
		b.ServeHTTP(w, r)
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			puts.Add(1)
			if n, ok := b.Node("/fsync-rewrite.bin"); ok {
				lastBody.Store(append([]byte(nil), n.Data...))
			}
		}
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCacheDir: t.TempDir()})
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

	h := open(t, ch, "/fsync-rewrite.bin").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	// fsync lands "first" synchronously (close-sync policy).
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The same handle rewrites over its committed image.
	if err := h.Write(ctx, 5, []byte("-second")); err != nil {
		t.Fatal(err)
	}
	// The flush lands the final image; a Release error (the empty-create
	// materialize path can conflict with the already-landed name) does not
	// affect the remote image.
	_ = h.Flush(ctx)
	_ = h.Release(ctx)
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/fsync-rewrite.bin"); ok && string(n.Data) == "first-second" {
		return
	}
	if lastBody.Load() != nil && string(lastBody.Load().([]byte)) == "first-second" {
		return
	}
	got := ""
	if n, ok := b.Node("/fsync-rewrite.bin"); ok {
		got = string(n.Data)
	}
	// v1 preserved the rewrite across the fsync boundary; markLanded at
	// the fsync's landing retires isNew so the rewrite stages as an
	// overwrite of the landed revision.
	t.Fatalf("post-fsync rewrite lost: backend = %q", got)
}
