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
//
// The marker is serialized with the staging publication through the
// same-path lock (a racing fsync stages nothing under the dead name), and
// a WAL that cannot take the marker falls back to dropping the preserved
// state instead of leaving it replayable.
package writeback

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
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
	ts      *httptest.Server

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
	s.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		target := strings.HasSuffix(r.URL.Path, holdPath)
		switch {
		case target && s.hold.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost):
			// Buffer the body (net/http only arms the client-gone detection
			// once it is consumed) so a parked upload that is later released
			// can still fall through to the backend intact.
			var body bytes.Buffer
			_, _ = io.Copy(&body, r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body.Bytes()))
			select {
			case s.putStarted <- struct{}{}:
			default:
			}
			// Park while held; re-check so a test that lifts the hold later
			// lets a parked upload continue to the backend instead of
			// waiting for the whole server teardown.
			timer := time.NewTimer(20 * time.Millisecond)
			defer timer.Stop()
			for s.hold.Load() {
				select {
				case <-r.Context().Done():
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-s.release:
					http.Error(w, "canceled", http.StatusRequestTimeout)
					return
				case <-timer.C:
					timer.Reset(20 * time.Millisecond)
				}
			}
		case target && r.Method == http.MethodDelete:
			s.deletes.Add(1)
		}
		s.backend.ServeHTTP(w, r)
	}))
	t.Cleanup(s.ts.Close)
	t.Cleanup(func() { close(s.release) })

	cl := client.New(s.ts.URL, "")
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
	if err := s.wb.MarkPathUnlinked(path); err != nil {
		t.Fatalf("unlink-of-open-file marking: %v", err)
	}

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

// An interactive fsync racing the unlink-of-open-file marking (the handle
// passed its dirty check, the unlink marked it anonymous, neither side has
// published) must not stage, enqueue, or upload under the dead name, and
// the WAL must stay non-replayable for it. Serialized by the same-path lock
// cancelPath and stageAndEnqueueOpt share.
func TestFsyncRacingUnlinkOfOpenFileStagesNothing(t *testing.T) {
	const path = "/raced.txt"
	s := newUnlinkWindowStack(t, path)
	ctx := context.Background()

	h := open(t, s.ch, path).(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("race")); err != nil {
		t.Fatal(err)
	}

	// The unlink's marking takes no path lock (a commit worker holds it
	// across its upload); the race is closed by the staging side: the
	// pre-enqueue guard, or — when the marking lands inside the
	// check-to-enqueue window — the post-enqueue recheck that cancels the
	// just-enqueued entry. Mark BEFORE the fsync runs: its guard must bar
	// the staging outright.
	if err := s.wb.MarkPathUnlinked(path); err != nil {
		t.Fatalf("racing unlink marking: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("racing fsync: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.wb.WaitCommitQueueIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.backend.Node(path); ok {
		t.Fatal("racing fsync uploaded the unlinked name")
	}
	if s.crashAndReplay(t, path) {
		t.Fatal("racing fsync left replayable frames for the unlinked name")
	}
}

// When the WAL cannot take the unlink marker (a full or failing cache disk),
// the preserve-local cancel must not leave the retained shadow/index as
// replayable state: it falls back to the non-preserving cleanup, and the
// recovery mount prunes the WAL-resurrected entry instead of re-uploading.
func TestUnlinkMarkerFailureDropsPreservedState(t *testing.T) {
	const path = "/waldown.txt"
	s := newUnlinkWindowStack(t, path)
	s.hold.Store(true)
	ctx := context.Background()

	h := open(t, s.ch, path).(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("wal-down")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	s.waitPutStarted(t)

	// Break the WAL the way a full disk would.
	if err := s.ledger.Journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.wb.MarkPathUnlinked(path); err != nil {
		t.Fatalf("marker failure with a working cleanup must not fail the unlink: %v", err)
	}

	if s.ledger.Shadow.Has(path) {
		t.Fatal("marker failure must drop the retained shadow")
	}
	if s.ledger.Pending.HasPending(path) {
		t.Fatal("marker failure must drop the retained pending entry")
	}

	// Full recovery-mount shape: replay + queue RecoverPending. The replayed
	// fsync frame re-adds an index entry, but the missing shadow makes
	// recoveredEntry prune it — nothing re-uploads.
	s.hold.Store(false)
	recovered, err := staging.NewStaging(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recovered.Close)
	if err := recovered.Recover(); err != nil {
		t.Fatal(err)
	}
	cl := client.New(s.ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	wb2, err := New(Options{Policy: PolicyWriteBack, SyncMode: "interactive", Client: cl, Staging: recovered})
	if err != nil {
		t.Fatal(err)
	}
	if err := wb2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb2.WaitCommitQueueIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.backend.Node(path); ok {
		t.Fatal("recovery re-uploaded the unlinked name")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// When the WAL cannot take the marker AND the fallback cleanup cannot make
// the staged image provably gone (a read-only or failing cache filesystem),
// the unlink must be reported as failed instead of acknowledged on the
// assumption that the void removal APIs did their job. The failure must not
// strand the fsynced publication either: while the shadow is still stat-able
// the upload is re-driven once the filesystem recovers, and when even the
// stat fails the pending entry stays so the overlay keeps serving the file.
func TestUnlinkMarkerAndCleanupFailureFailsUnlink(t *testing.T) {
	// The failure injection is directory permission bits: root ignores them
	// and Windows has no POSIX permission semantics, and under either the
	// cleanup succeeds and these rows would false-fail.
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("directory permission bits cannot block the shadow removal on this platform/user")
	}
	for _, tc := range []struct {
		name  string
		mode  os.FileMode
		statd bool // the shadow stays stat-able through the fault
	}{
		{"removal fails, stat works", 0o500, true},
		{"removal and stat both fail", 0o400, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const path = "/bothdown.txt"
			s := newUnlinkWindowStack(t, path)
			s.hold.Store(true)
			ctx := context.Background()

			h := open(t, s.ch, path).(*bufferingHandle)
			if err := h.Write(ctx, 0, []byte("both-down")); err != nil {
				t.Fatal(err)
			}
			if err := h.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			s.waitPutStarted(t)

			// Break the WAL, then make the shadow store unremovable: a
			// directory without write permission rejects the retirement
			// rename and the fallback remove; without search permission it
			// rejects the verification stat as well.
			if err := s.ledger.Journal.Close(); err != nil {
				t.Fatal(err)
			}
			shadowDir := filepath.Join(s.dir, "shadow")
			if err := os.Chmod(shadowDir, tc.mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(shadowDir, 0o700) })

			err := s.wb.MarkPathUnlinked(path)
			if err == nil {
				t.Fatal("marker + cleanup both failing must fail the unlink instead of acknowledging it")
			}
			// The recovery source did not provably disappear — that is what
			// the error reports (Has alone cannot tell a survivor from an
			// unstat-able one, so ask the verifier).
			if verr := s.ledger.Shadow.VerifyRemoved(path); verr == nil {
				t.Fatal("precondition: the shadow must still be present or unverifiable")
			}
			// Rollback: the descriptor goes on serving its name, not an
			// anonymous snapshot of a file the unlink never removed.
			h.mu.Lock()
			unlinked := h.unlinked
			h.mu.Unlock()
			if unlinked {
				t.Fatal("a failed unlink must restore the handle to its name")
			}
			if !s.ledger.Pending.HasPending(path) {
				t.Fatal("a failed unlink must keep the pending entry discoverable")
			}

			// Repair the fault and lift the upload hold.
			if err := os.Chmod(shadowDir, 0o700); err != nil {
				t.Fatal(err)
			}
			s.hold.Store(false)

			if tc.statd {
				// The re-driven upload lands the fsynced bytes: the failed
				// unlink did not strand the publication.
				deadline := time.Now().Add(10 * time.Second)
				for {
					if n, ok := s.backend.Node(path); ok {
						if string(n.Data) != "both-down" {
							t.Fatalf("re-driven upload landed %q, want the fsynced image", n.Data)
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("the re-driven upload never landed after the fault was repaired")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			// The file must still be reachable through the chain after the
			// last handle closes: pending overlay while staged, the landed
			// remote image once the re-drive completes.
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.wb.WaitCommitQueueIdle(ctx); err != nil {
				t.Fatal(err)
			}
			if !tc.statd {
				if _, ok := s.backend.Node(path); ok {
					t.Fatal("nothing may upload while the shadow cannot even be stat-ed")
				}
				if !s.ledger.Pending.HasPending(path) {
					t.Fatal("the pending entry must survive an unverifiable cleanup")
				}
				return
			}
			res, err := s.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path})
			if err != nil {
				t.Fatalf("lookup after the recovered fault: %v", err)
			}
			if res.Attr == nil || res.Attr.Kind != fs.KindFile {
				t.Fatalf("lookup after the recovered fault = %+v, want a file", res.Attr)
			}
		})
	}
}
