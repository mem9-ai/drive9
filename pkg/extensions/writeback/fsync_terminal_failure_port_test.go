package writeback

// Strict-fsync error propagation: under write-back with strict durability,
// fsync waits for the path to land through the commit queue. A terminally
// failed commit also makes the path idle — the wait used to return success
// for data that exists only in preserved local pending state. fsync must
// answer EIO instead, stickily, until a later commit of the same path lands.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

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

// TestPortWriteBackStrictFsyncRecoversAfterRestagedLanding pins the
// recovery-side contract: after a terminal failure made fsync answer EIO, a
// rewritten image staged into the write-back cache must be able to land and
// retire the failure through fsync itself — the sticky EIO may only persist
// while no newer image actually lands.
func TestPortWriteBackStrictFsyncRecoversAfterRestagedLanding(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, _ := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/recover.txt")
	if err := h.Write(ctx, 0, []byte("first image")); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)
	if err := h.Sync(ctx); err == nil {
		t.Fatal("fsync reported durability after a terminal upload failure")
	}
	// The writer publishes a replacement while the backend recovers: the
	// Flush stages it into the write-back cache and retires the dirty flag,
	// so the next fsync's commitLocked is a no-op — the staged landing below
	// is the only recovery path this fsync has.
	fb.setFail(false)
	if err := h.Write(ctx, 0, []byte("replacement image")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync after a restaged replacement landed = %v, want nil", err)
	}
	// The recovery is durable for later barriers too, and the remote carries
	// the replacement, not the failed image.
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("repeat fsync after recovery = %v, want nil", err)
	}
	if n, ok := fb.backend.Node("/recover.txt"); !ok || string(n.Data) != "replacement image" {
		t.Fatalf("backend after recovery = %q ok=%v, want the replacement image", n.Data, ok)
	}
}

// TestPortWriteBackUploaderLandingRetiresFailure covers the async leg: under
// write-back, Release hands the Flush snapshot to the background uploader.
// When that newer image lands, the path's recorded terminal failure must be
// retired — otherwise later fsyncs keep answering EIO for data that is
// already remote. The clearing is guarded: a failure recorded after the
// upload started (a newer queue generation) must survive.
func TestPortWriteBackUploaderLandingRetiresFailure(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/async-recover.txt")
	if err := h.Write(ctx, 0, []byte("queue image")); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)
	if err := h.Sync(ctx); err == nil {
		t.Fatal("fsync reported durability after a terminal upload failure")
	}
	if wb.queue.PathTerminalFailure("/async-recover.txt") == nil {
		t.Fatal("terminal failure not recorded for the path")
	}

	// Replacement: Flush stages it; Release submits the uploader and returns
	// before the remote landing — the async worker owns the recovery.
	fb.setFail(false)
	if err := h.Write(ctx, 0, []byte("async replacement")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for wb.queue.PathTerminalFailure("/async-recover.txt") != nil {
		if time.Now().After(deadline) {
			t.Fatal("uploader landing did not retire the recorded terminal failure")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The durable conflict marker is retired with it: a fresh strict fsync
	// through a re-opened handle must report durability, not the old EIO.
	h2 := open(t, ch, "/async-recover.txt")
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("fsync after the async landing = %v, want nil", err)
	}
	if n, ok := fb.backend.Node("/async-recover.txt"); !ok || string(n.Data) != "async replacement" {
		t.Fatalf("backend after the async landing = %q ok=%v, want the replacement", n.Data, ok)
	}
}

// TestPathTerminalFailureRestartFallback pins the restart leg: the in-memory
// record dies with the process, but onCommitTerminalFailure durably marks the
// pending entry as a conflict. A queue rebuilt over the recovered index must
// still refuse to certify that path — the data never landed remotely.
func TestPathTerminalFailureRestartFallback(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend(), fail: true}
	ts := httptest.NewServer(fb)
	t.Cleanup(ts.Close)

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := staging.NewShadowStoreWithQuota(shadowDir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := staging.NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.Warm(context.Background())
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := shadow.WriteFull("/restart.txt", []byte("only local"), 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev("/restart.txt", 10, staging.PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path: "/restart.txt", Size: 10, Kind: staging.PendingNew,
		ShadowGen: shadow.ActiveGeneration("/restart.txt"), PendingIndexGen: pendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.WaitPath("/restart.txt")
	cq.DrainAll()

	// "Restart": fresh index recovered from disk, fresh queue, no in-memory
	// state. Only the durable conflict marker remains.
	recovered, err := staging.NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	cq2 := NewCommitQueue(newTestClient(ts.URL), shadow, recovered, nil, 1, 8)
	t.Cleanup(cq2.DrainAll)
	if err := cq2.PathTerminalFailure("/restart.txt"); err == nil {
		t.Fatal("PathTerminalFailure after restart = nil, want the durable conflict marker to answer")
	}
	if err := cq2.PathTerminalFailure("/untouched.txt"); err != nil {
		t.Fatalf("PathTerminalFailure for a never-failed path after restart = %v, want nil", err)
	}
}

// TestPathTerminalFailureEvictionKeepsUnresolvedRecords pins the eviction
// rule: pruning may only drop records the barrier can still re-derive from
// durable state (a PendingConflict marker) or that are already stale (no
// pending entry at all). Records with a live non-conflict pending entry are
// the only witness of an unresolved failure and must never be evicted — the
// map may stay above its bound while any remain.
func TestPathTerminalFailureEvictionKeepsUnresolvedRecords(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)

	// Evictable records first (older seqs): a conflict-backed one and stale
	// ones with no pending state left.
	if _, err := pending.PutWithBaseRev("/conflict.txt", 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := pending.MarkConflict("/conflict.txt"); err != nil {
		t.Fatal(err)
	}
	cq.recordCommitTerminalFailure(&CommitEntry{Path: "/conflict.txt"}, boom)
	for i := 0; i < 7; i++ {
		cq.recordCommitTerminalFailure(&CommitEntry{Path: fmt.Sprintf("/stale-%d.txt", i)}, boom)
	}

	// Unresolvable records: live pending entries the conflict marking could
	// not reach (e.g. the marker write failed) — evicting them would let
	// fsync claim durability for data that never landed.
	for i := 0; i < terminalFailuresLimit+4; i++ {
		path := fmt.Sprintf("/witness-%d.txt", i)
		if _, err := pending.PutWithBaseRev(path, 3, staging.PendingNew, 0); err != nil {
			t.Fatal(err)
		}
		cq.recordCommitTerminalFailure(&CommitEntry{Path: path}, boom)
	}

	cq.mu.Lock()
	_, witnessKept := cq.terminalFailures["/witness-0.txt"]
	size := len(cq.terminalFailures)
	cq.mu.Unlock()
	if !witnessKept {
		t.Fatal("eviction dropped the only copy of an unresolved terminal failure")
	}
	if size != terminalFailuresLimit+4 {
		t.Fatalf("pruned map holds %d records, want the %d unresolvable ones only", size, terminalFailuresLimit+4)
	}
	// The conflict-backed record is gone from the map, but the barrier still
	// answers through the durable marker; the stale ones answer nil.
	if err := cq.PathTerminalFailure("/conflict.txt"); err == nil {
		t.Fatal("conflict-backed failure no longer answers after eviction")
	}
	if err := cq.PathTerminalFailure("/stale-0.txt"); err != nil {
		t.Fatalf("stale evicted record still answers: %v", err)
	}
	if err := cq.PathTerminalFailure("/witness-0.txt"); err == nil {
		t.Fatal("unresolvable failure no longer answers")
	}
}

// TestTerminalFailureClearGuards pins the ordering guards: a landing may only
// retire the failure it observed before it started (the seq fence), and the
// durable conflict marker only when the landed image provably superseded it
// (the generation fence).
func TestTerminalFailureClearGuards(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)

	// Seq fence.
	if _, err := pending.PutWithBaseRev("/seq.txt", 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	cq.recordCommitTerminalFailure(&CommitEntry{Path: "/seq.txt"}, boom)
	seq := cq.PathTerminalFailureSeq("/seq.txt")
	if seq == 0 {
		t.Fatal("PathTerminalFailureSeq = 0 for a recorded failure")
	}
	cq.ClearTerminalFailureIfSeq("/seq.txt", 0) // nothing observed at capture
	cq.ClearTerminalFailureIfSeq("/seq.txt", seq+1)
	if cq.PathTerminalFailure("/seq.txt") == nil {
		t.Fatal("landing cleared a failure recorded after it started")
	}
	cq.ClearTerminalFailureIfSeq("/seq.txt", seq)
	if cq.PathTerminalFailure("/seq.txt") != nil {
		t.Fatal("landing did not clear the failure it observed")
	}

	// Generation fence on the durable marker.
	if _, err := pending.PutWithBaseRev("/gen.txt", 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := pending.MarkConflict("/gen.txt"); err != nil {
		t.Fatal(err)
	}
	conflictGen := pending.Generation("/gen.txt")
	cq.ResolveSupersededConflict("/gen.txt", conflictGen-1)
	if !pending.HasPending("/gen.txt") {
		t.Fatal("resolve dropped a conflict marker newer than the landed image")
	}
	cq.ResolveSupersededConflict("/gen.txt", conflictGen)
	if pending.HasPending("/gen.txt") {
		t.Fatal("resolve kept a conflict marker the landed image superseded")
	}
}

// TestSyncWaitDeadlineSizeForReopenedPendingHandles pins the wait-deadline
// input for handles that serve a staged pending image without a buffer: the
// deadline must scale with the image being waited for (the worker's own
// releaseTimeout budget), not fall back to the 60s floor — a healthy 2GiB
// upload outlasts it.
func TestSyncWaitDeadlineSizeForReopenedPendingHandles(t *testing.T) {
	h := &bufferingHandle{baseSize: 2 << 30}
	if got := h.syncWaitDeadlineSizeLocked(); got != 2<<30 {
		t.Fatalf("deadline size for a spill-serving handle = %d, want the pending image size %d", got, 2<<30)
	}
	h.pendingImage = make([]byte, 4096)
	if got := h.syncWaitDeadlineSizeLocked(); got != 4096 {
		t.Fatalf("deadline size for a materialized pending image = %d, want 4096", got)
	}
	if releaseTimeout(2<<30) <= releaseTimeout(0) {
		t.Fatal("a 2GiB pending image must scale the wait beyond the 60s floor")
	}
}
