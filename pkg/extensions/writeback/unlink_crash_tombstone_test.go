// Issue #1065: an unlink that lands inside an interactive fsync's
// stage→enqueue→upload window must leave the WAL in a state that crash
// recovery cannot resurrect.
//
// Both unlink paths cancel the queued commit with CancelPathPreserveLocal
// (the anonymous fd keeps reading the staged image), and that cancel used
// to journal nothing: the path's JournalPendingMeta/JournalFsync frames
// stayed replayable until a LATER non-preserving CancelPath landed a done
// marker — the facade's unlink-of-open-file branch (FUSE RELEASE is
// asynchronous, so close(2)+unlink(2) races it) defers that to the last
// close plus a remote HEAD. A kill -9 inside that window replayed the
// frames and re-committed the unlinked file.
package writeback

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// journalFramesFor renders the WAL frames of one path for diagnostics.
func journalFramesFor(t *testing.T, j *staging.Journal, path string) string {
	t.Helper()
	var lines []string
	_ = j.Replay(func(e staging.JournalEntry) {
		if e.Path != path {
			return
		}
		lines = append(lines, fmt.Sprintf("seq=%d op=%d len=%d base=%d meta=%d", e.Seq, e.Op, e.Length, e.BaseRev, len(e.Meta)))
	})
	return strings.Join(lines, "\n")
}

// unlinkWindowStack is an interactive writeback over a staging ledger
// whose PUTs of holdPath block until the client cancels them (the
// "PUT → 499" of the issue) or the test releases them.
type unlinkWindowStack struct {
	ch      *fs.Chain
	wb      *Writeback
	ledger  *staging.Staging
	dir     string
	backend *testbackend.Backend

	putStarted chan struct{}
	release    chan struct{}
	hold       atomic.Bool
	deletes    atomic.Int32
}

func newUnlinkWindowStack(t *testing.T, holdPath string) *unlinkWindowStack {
	t.Helper()
	s := &unlinkWindowStack{
		backend:    testbackend.NewBackend(),
		putStarted: make(chan struct{}, 4),
		release:    make(chan struct{}),
		dir:        t.TempDir(),
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		target := strings.HasSuffix(r.URL.Path, holdPath)
		switch {
		case target && s.hold.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost):
			// Drain the body first: net/http only arms the client-gone
			// detection (request context cancel) once the body is consumed.
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case s.putStarted <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
			case <-s.release:
			}
			http.Error(w, "canceled", http.StatusRequestTimeout)
			return
		case target && r.Method == http.MethodDelete:
			s.deletes.Add(1)
		}
		s.backend.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(s.release) })

	cl := client.New(ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	ledger, err := staging.NewStaging(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	s.ledger = ledger
	wb, err := New(Options{
		Policy:   PolicyWriteBack,
		SyncMode: "interactive",
		Client:   cl,
		Staging:  ledger,
		// The direct unlink path's bounded wait on the held upload.
		RemoteCommitWaitTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.wb = wb
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	s.ch = ch
	return s
}

// waitPutStarted blocks until the held upload reached the server.
func (s *unlinkWindowStack) waitPutStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.putStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("queued commit never reached the server")
	}
}

// crashAndReplay simulates kill -9 (no Close, no compaction): a fresh
// ledger reopens the same cache dir and replays the WAL the way the
// recovery mount does, then reports whether path came back pending.
func (s *unlinkWindowStack) crashAndReplay(t *testing.T, path string) bool {
	t.Helper()
	if err := s.wb.WaitCommitQueueIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("pre-crash WAL frames for %s:\n%s", path, journalFramesFor(t, s.ledger.Journal, path))
	recovered, err := staging.NewStaging(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recovered.Close)
	if err := recovered.Pending.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	if recovered.Pending.HasPending(path) {
		meta, _ := recovered.Pending.GetMeta(path)
		t.Logf("replayed pending meta for %s: %+v", path, meta)
		return true
	}
	return false
}

// The facade's unlink-of-open-file branch: MarkPathUnlinked runs while
// the fsync'd commit is still uploading, and the crash lands before the
// deferred OpUnlink (last close + remote HEAD) can journal anything.
func TestUnlinkOfOpenFileInsideFsyncWindowDoesNotResurrectAfterCrash(t *testing.T) {
	const path = "/doomed.txt"
	s := newUnlinkWindowStack(t, path)
	s.hold.Store(true)
	ctx := context.Background()

	h := open(t, s.ch, path).(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("doomed-by-unlink\n")); err != nil {
		t.Fatal(err)
	}
	// Interactive fsync: shadow + JournalPendingMeta + JournalFsync, async enqueue.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	s.waitPutStarted(t)

	// close(2) returned; RELEASE is still in flight, so unlink(2) takes the
	// unlink-of-open-file branch: the queued upload is canceled mid-flight.
	s.wb.MarkPathUnlinked(path)

	// kill -9 here.
	if s.crashAndReplay(t, path) {
		t.Fatal("unlinked path resurrected from the WAL: the unlink must journal a done marker for the staged frames")
	}
	if got := s.deletes.Load(); got != 0 {
		t.Fatalf("no DELETE must reach the server before the deferred unlink, got %d", got)
	}
	// The in-process state is unchanged by the marker: the anonymous fd
	// still serves the staged image until its last close.
	buf := make([]byte, 32)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "doomed-by-unlink\n" {
		t.Fatalf("post-unlink read = %q n=%d err=%v", buf[:n], n, err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// The direct OpUnlink path: the previous image landed (stat 200), the
// DELETE succeeds, and purgeStaging cancels a queued overwrite with
// preserve-local. The crash lands right after the unlink returned.
func TestUnlinkWithQueuedOverwriteDoesNotResurrectAfterCrash(t *testing.T) {
	const path = "/overwrite.txt"
	s := newUnlinkWindowStack(t, path)
	ctx := context.Background()

	// First image lands normally (rev 1).
	h := open(t, s.ch, path).(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.wb.WaitCommitQueueIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := s.backend.Node(path); !ok || string(n.Data) != "first" {
		t.Fatalf("first image did not land: %+v ok=%v", n, ok)
	}

	// The overwrite's fsync'd commit is held in flight.
	s.hold.Store(true)
	if err := h.Write(ctx, 0, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	s.waitPutStarted(t)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// unlink(2) with no open fd: bounded wait on the held upload, stat 200,
	// DELETE, then purgeStaging's preserve-local cancel of the overwrite.
	if _, err := s.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path}); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if got := s.deletes.Load(); got != 1 {
		t.Fatalf("DELETE count = %d, want 1", got)
	}
	if _, ok := s.backend.Node(path); ok {
		t.Fatal("remote entry must be gone after unlink")
	}

	// kill -9 here.
	if s.crashAndReplay(t, path) {
		t.Fatal("deleted path's queued overwrite resurrected from the WAL: purgeStaging must journal a done marker")
	}
}
