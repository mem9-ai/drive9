package writeback

// Strict-fsync error propagation: under write-back with strict durability,
// fsync waits for the path to land through the commit queue. A terminally
// failed commit also makes the path idle — the wait used to return success
// for data that exists only in preserved local pending state. fsync must
// answer EIO instead, stickily, until a later commit of the same path lands.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// failingWritesBackend delegates to a real testbackend but answers every
// non-GET request with HTTP 500 while armed: stat/read traffic keeps working
// (fresh readers must still see the remote), only the uploads fail — the
// production symptom of the server-side 500s.
type failingWritesBackend struct {
	backend *testbackend.Backend
	mu      sync.Mutex
	fail    bool
}

func (b *failingWritesBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	fail := b.fail
	b.mu.Unlock()
	if fail && r.Method != http.MethodGet {
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	b.backend.ServeHTTP(w, r)
}

func (b *failingWritesBackend) setFail(fail bool) {
	b.mu.Lock()
	b.fail = fail
	b.mu.Unlock()
}

// TestPortWriteBackStrictFsyncReportsTerminalUploadFailure drives the full
// handle path: write, break every upload, fsync. The strict barrier must
// answer EIO (not success) once the commit queue gives up terminally, and a
// later fsync of the same unwritten handle must keep failing — the data still
// exists only locally.
func TestPortWriteBackStrictFsyncReportsTerminalUploadFailure(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, _ := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/fsync-eio.txt")
	if err := h.Write(ctx, 0, []byte("strict content")); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)

	err := h.Sync(ctx)
	if err == nil {
		t.Fatal("fsync reported durability after a terminal upload failure")
	}
	if st := fs.StatusOf(err); st != gofuse.Status(syscall.EIO) {
		t.Fatalf("fsync status = %v, want EIO", st)
	}
	// Sticky: without a newer commit landing, a repeat fsync keeps failing.
	if err := h.Sync(ctx); err == nil {
		t.Fatal("repeat fsync reported durability for still-local data")
	}
}

// TestPathTerminalFailureLifecycle covers the queue-side record directly:
// recorded on terminal failure, invisible for paths that never failed, and
// cleared by the next successful commit of the same path.
func TestPathTerminalFailureLifecycle(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend(), fail: true}
	ts := httptest.NewServer(fb)
	t.Cleanup(ts.Close)

	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	stage := func(path, data string) (shadowGen, pendingGen uint64) {
		t.Helper()
		if err := shadow.WriteFull(path, []byte(data), 0); err != nil {
			t.Fatal(err)
		}
		shadowGen = shadow.ActiveGeneration(path)
		pendingGen, err = pending.PutWithBaseRev(path, 1, staging.PendingNew, 0)
		if err != nil {
			t.Fatal(err)
		}
		return shadowGen, pendingGen
	}

	c := newTestClient(ts.URL)
	c.Warm(context.Background())
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)

	// Terminal leg: five failed upload attempts, then a recorded failure.
	// WaitPath (not DrainAll — it stops the queue for good) waits for the
	// path to go idle, which is exactly what a strict fsync does.
	shadowGen, pendingGen := stage("/io.txt", "v1")
	if err := cq.Enqueue(&CommitEntry{
		Path: "/io.txt", Size: 2, Kind: staging.PendingNew,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.WaitPath("/io.txt")
	failure := cq.PathTerminalFailure("/io.txt")
	if failure == nil {
		t.Fatal("PathTerminalFailure = nil after a terminal upload failure")
	}
	if failure.Error() == "" {
		t.Fatal("recorded failure carries no error text")
	}
	if err := cq.PathTerminalFailure("/never-failed.txt"); err != nil {
		t.Fatalf("PathTerminalFailure for an untouched path = %v, want nil", err)
	}

	// Recovery leg: a fresh generation commits successfully and clears the
	// record — later durability barriers must not keep reporting EIO.
	fb.setFail(false)
	shadowGen, pendingGen = stage("/io.txt", "v2")
	if err := cq.Enqueue(&CommitEntry{
		Path: "/io.txt", Size: 2, Kind: staging.PendingNew,
		ShadowGen: shadowGen, PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.WaitPath("/io.txt")
	if err := cq.PathTerminalFailure("/io.txt"); err != nil {
		t.Fatalf("PathTerminalFailure after a successful commit = %v, want nil", err)
	}
	if pending.HasPending("/io.txt") {
		t.Fatal("successful commit should have removed the pending entry")
	}
}
