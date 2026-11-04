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

// TestPortRenameMovesConflictMarkers pins the durable side of rename: a
// terminally failed source keeps its conflict marker (and preserved bytes)
// through the rename — they follow the name — while the replaced target's
// own marker dies with its incarnation. Clearing only the in-memory records
// would leave both markers answering EIO through the durable fallback.
func TestPortRenameMovesConflictMarkers(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// Both files land once, then a second edit terminally fails: each name
	// ends with a durable conflict marker over a landed remote image.
	stageConflict := func(path, landed, unlanded string) {
		t.Helper()
		h := open(t, ch, path)
		if err := h.Write(ctx, 0, []byte(landed)); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		fb.setFail(true)
		if err := h.Write(ctx, 0, []byte(unlanded)); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err == nil {
			t.Fatalf("fsync for %s reported durability during the upload outage", path)
		}
		if err := wb.queue.PathTerminalFailure(path); err == nil {
			t.Fatalf("terminal failure not recorded for %s", path)
		}
		fb.setFail(false)
	}
	stageConflict("/mv-src.txt", "src landed", "src unlanded v2")
	stageConflict("/mv-dst.txt", "dst landed", "dst unlanded v2")

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/mv-src.txt", Path2: "/mv-dst.txt"}); err != nil {
		t.Fatal(err)
	}

	// The old name is clean: record cleared, no pending entry left behind.
	if err := wb.queue.PathTerminalFailure("/mv-src.txt"); err != nil {
		t.Fatalf("old name still answers EIO after rename: %v", err)
	}
	if wb.opts.Staging.Pending.HasPending("/mv-src.txt") {
		t.Fatal("source pending entry stayed keyed under the dead old name")
	}
	// The new name carries the SOURCE's failure: its unlanded image now
	// lives there, so the marker (and the preserved bytes) must have moved.
	if err := wb.queue.PathTerminalFailure("/mv-dst.txt"); err == nil {
		t.Fatal("the moved source's terminal failure was lost by the rename")
	}
	meta, ok := wb.opts.Staging.Pending.GetMeta("/mv-dst.txt")
	if !ok || meta == nil || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending entry at the new name = %+v ok=%v, want the transferred conflict marker", meta, ok)
	}
	data, err := wb.opts.Staging.Shadow.ReadAll("/mv-dst.txt")
	if err != nil || string(data) != "src unlanded v2" {
		t.Fatalf("shadow at the new name = %q err=%v, want the source's preserved bytes", data, err)
	}
}

// TestPortRenameRetiresTargetConflictMarker covers the no-source-failure
// leg: a clean source replaces a terminally failed target, and the target's
// durable marker must die with its incarnation — the new incumbent of the
// name has nothing undurable.
func TestPortRenameRetiresTargetConflictMarker(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	src := open(t, ch, "/mv2-src.txt")
	if err := src.Write(ctx, 0, []byte("clean source")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	dst := open(t, ch, "/mv2-dst.txt")
	if err := dst.Write(ctx, 0, []byte("target landed")); err != nil {
		t.Fatal(err)
	}
	if err := dst.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)
	if err := dst.Write(ctx, 0, []byte("target unlanded")); err != nil {
		t.Fatal(err)
	}
	if err := dst.Sync(ctx); err == nil {
		t.Fatal("fsync reported durability during the upload outage")
	}
	fb.setFail(false)

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/mv2-src.txt", Path2: "/mv2-dst.txt"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/mv2-src.txt", "/mv2-dst.txt"} {
		if err := wb.queue.PathTerminalFailure(path); err != nil {
			t.Fatalf("PathTerminalFailure(%s) after rename = %v, want nil", path, err)
		}
	}
	if wb.opts.Staging.Pending.HasPending("/mv2-dst.txt") {
		t.Fatal("replaced target's conflict marker survived its incarnation")
	}
}

// TestSupersededGenComesFromHandleContent pins the supersession fence's
// source of truth: a write-cache staging may only claim the pending
// generation the handle's own content actually contains, never whatever
// generation happens to be registered at the path when the image is stamped.
// Landing an image that never carried a foreign writer's bytes must not
// retire that writer's conflict marker.
func TestSupersededGenComesFromHandleContent(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// Writer B: stages and terminally fails, leaving a conflict marker.
	b := open(t, ch, "/gen-race.txt")
	if err := b.Write(ctx, 0, []byte("B landed base")); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)
	if err := b.Write(ctx, 0, []byte("B never landed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(ctx); err == nil {
		t.Fatal("fsync reported durability during the upload outage")
	}
	fb.setFail(false)
	meta, ok := wb.opts.Staging.Pending.GetMeta("/gen-race.txt")
	if !ok || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending entry = %+v ok=%v, want a conflict marker", meta, ok)
	}
	markerGen := meta.Generation

	// Writer A: a handle that captured no pending image claims nothing —
	// the stamp is the handle's content, not the path's live generation.
	a := &bufferingHandle{w: wb, path: "/gen-race.txt", flags: uint32(syscall.O_RDWR)}
	if a.supersededPendingGen != 0 {
		t.Fatalf("fresh handle claims to supersede generation %d", a.supersededPendingGen)
	}
	if a.supersededPendingGen == markerGen {
		t.Fatal("a handle claims to contain a generation its image never carried")
	}

	// The fenced cleanup leaves B's marker alone: A's image does not
	// supersede it, so the path keeps answering EIO.
	wb.resolveLandedStaging("/gen-race.txt", 42, a.supersededPendingGen, wb.queue.PathTerminalFailureSeq("/gen-race.txt"))
	if _, still := wb.opts.Staging.Pending.GetMeta("/gen-race.txt"); !still {
		t.Fatal("a landing that never contained the marker's bytes retired it")
	}
	if err := wb.queue.PathTerminalFailure("/gen-race.txt"); err == nil {
		t.Fatal("the foreign writer's terminal failure stopped being reported")
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

// TestPortRenameMovesConflictMarkerStagedDuringRename pins the transfer fence
// for an image that was staged (and terminally failed) under the old name
// WHILE the remote rename was in flight: the pre-rename generation snapshot
// no longer matches, and the transfer must re-derive the live generations
// under the path locks and move THAT marker (and its preserved bytes) to the
// new name instead of reporting the fenced-out transfer as a success the
// caller would act on.
func TestPortRenameMovesConflictMarkerStagedDuringRename(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// The rename observed this conflict under /a ...
	mkConflict := func(path, unlanded string) {
		t.Helper()
		h := open(t, ch, path)
		if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		fb.setFail(true)
		if err := h.Write(ctx, 0, []byte(unlanded)); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err == nil {
			t.Fatalf("fsync for %s reported durability during the upload outage", path)
		}
		fb.setFail(false)
	}
	mkConflict("/race-src.txt", "v2 unlanded")
	// ... while the RPC was in flight, a second writer staged a newer image
	// and terminally failed it: the pending entry advanced past the snapshot.
	mkConflict("/race-src.txt", "v3 unlanded")

	// The post-Rename transition, with the stale pre-rename generations the
	// old code trusted: the marker that must move is the LIVE one (v3).
	if err := wb.retargetConflictMarkers("/race-src.txt", "/race-dst.txt"); err != nil {
		t.Fatalf("retarget reported the fenced-out transfer as a failure: %v", err)
	}
	// What the rename caller does on a successful transition.
	wb.queue.clearTerminalFailure("/race-src.txt")
	wb.queue.clearTerminalFailure("/race-dst.txt")

	if wb.opts.Staging.Pending.HasPending("/race-src.txt") {
		t.Fatal("source pending entry stayed keyed under the dead old name")
	}
	meta, ok := wb.opts.Staging.Pending.GetMeta("/race-dst.txt")
	if !ok || meta == nil || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending entry at the new name = %+v ok=%v, want the racing writer's transferred marker", meta, ok)
	}
	data, err := wb.opts.Staging.Shadow.ReadAll("/race-dst.txt")
	if err != nil || string(data) != "v3 unlanded" {
		t.Fatalf("shadow at the new name = %q err=%v, want the NEWEST unlanded image", data, err)
	}
	// The in-memory records were cleared like a successful rename does; the
	// durable marker must keep answering EIO for the unlanded image.
	if err := wb.queue.PathTerminalFailure("/race-dst.txt"); err == nil {
		t.Fatal("the racing writer's terminal failure was lost by the rename")
	}
}

// TestStandaloneTransferFailuresAreNeverEvicted pins the eviction rule for
// standalone records (RecordTerminalFailureForPath): their unlanded bytes sit
// under a DIFFERENT old pathname, so "no pending entry at this path" is not
// staleness but the record's normal shape — the absence of destination
// metadata must not authorize eviction. A standalone record whose path does
// carry a durable conflict marker stays evictable through the fallback.
func TestStandaloneTransferFailuresAreNeverEvicted(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)

	// Marker-backed standalone record: the durable fallback re-derives it.
	if _, err := pending.PutWithBaseRev("/marker.txt", 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := pending.MarkConflict("/marker.txt"); err != nil {
		t.Fatal(err)
	}
	cq.RecordTerminalFailureForPath("/marker.txt", boom)
	// Bare standalone records: nothing at their path can re-derive them.
	for i := 0; i < terminalFailuresLimit+4; i++ {
		cq.RecordTerminalFailureForPath(fmt.Sprintf("/moved-%d.txt", i), boom)
	}

	cq.mu.Lock()
	size := len(cq.terminalFailures)
	_, markerKept := cq.terminalFailures["/marker.txt"]
	cq.mu.Unlock()
	if markerKept {
		t.Fatal("a marker-backed standalone record was not pruned past the bound")
	}
	if size != terminalFailuresLimit+4 {
		t.Fatalf("pruned map holds %d records, want every bare standalone one of %d kept", size, terminalFailuresLimit+4)
	}
	if err := cq.PathTerminalFailure("/marker.txt"); err == nil {
		t.Fatal("the marker-backed record no longer answers through the durable fallback")
	}
	if err := cq.PathTerminalFailure("/moved-0.txt"); err == nil {
		t.Fatal("a bare standalone failure was evicted although nothing can re-derive it")
	}
}
