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
//
// renameGate, when set, parks the FIRST rename request until the channel
// closes and reports the parking through gated once. The rename is applied
// SERVER-SIDE first and only the response is held: a test can stage under
// the old name while the namespace has already moved — the publication
// window the pre-rename drain cannot see, including everything that happens
// between the server-side change and the delayed response. The parked
// request keeps the fail snapshot from its arrival, so the rename itself
// still succeeds while everything staged meanwhile fails.
type failingWritesBackend struct {
	backend *testbackend.Backend
	mu      sync.Mutex
	fail    bool
	// failStat answers stat/read probes (HEAD/GET) with 500 while armed, for
	// callers that must fail closed when they cannot observe the remote.
	failStat bool

	renameGate chan struct{}
	gated      chan struct{}
}

func (b *failingWritesBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	fail := b.fail
	failStat := b.failStat
	gate := b.renameGate
	if gate != nil && r.URL.Query().Has("rename") {
		b.renameGate = nil
		gated := b.gated
		b.gated = nil
		b.mu.Unlock()
		if gated != nil {
			close(gated)
		}
		// Apply the rename server-side, then hold only the response: the
		// namespace has moved while the client still believes the RPC is in
		// flight.
		rec := httptest.NewRecorder()
		b.backend.ServeHTTP(rec, r)
		<-gate
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}
	b.mu.Unlock()
	if failStat && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
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

func (b *failingWritesBackend) setFailStat(fail bool) {
	b.mu.Lock()
	b.failStat = fail
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
	if err := wb.retargetConflictMarkers(context.Background(), "/race-src.txt", "/race-dst.txt", renameFence{}, false); err != nil {
		t.Fatalf("retarget reported the fenced-out transfer as a failure: %v", err)
	}
	// What the rename caller does on a successful transition.
	wb.queue.RenameTerminalFailure("/race-src.txt", "/race-dst.txt")

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

// TestPortRenameRetargetsSuccessorStagedDuringRename pins the publication
// window the pre-rename drain cannot close: while the remote rename RPC is
// in flight, the open writer stages (and enqueues) a successor image under
// the OLD name. Its upload runs against entry.Path — the dead old name — so
// the rename must move the whole obligation (entry, bytes, queue work) to
// the new name. With every upload failing, the re-driven landing terminally
// fails THERE: the marker and preserved bytes answer EIO at the new name
// instead of the successor being acknowledged at (or resurrecting) the old
// one, and the retargeted handle's strict Sync must refuse to certify it.
func TestPortRenameRetargetsSuccessorStagedDuringRename(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	gate := make(chan struct{})
	gated := make(chan struct{})
	fb.renameGate, fb.gated = gate, gated
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// The source lands cleanly, so the rename starts from a durable image.
	h := open(t, ch, "/settle-src.txt")
	if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Hold the rename RPC in flight and stage the successor inside the
	// window: Flush publishes and enqueues without waiting, and every upload
	// from here on fails, so the successor cannot land anywhere.
	renamed := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/settle-src.txt", Path2: "/settle-dst.txt"})
		renamed <- err
	}()
	select {
	case <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("the rename request never reached the backend")
	}
	fb.setFail(true)
	if err := h.Write(ctx, 0, []byte("successor never landed")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(gate)

	select {
	case err := <-renamed:
		if err != nil {
			t.Fatalf("rename reported %v, want success after moving the successor", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the rename never moved the successor staged during its RPC")
	}

	// The successor's obligation followed the name: its pending state and
	// preserved bytes live at the destination, and with uploads failing the
	// re-driven landing terminally fails THERE.
	wb.queue.WaitPath("/settle-dst.txt")
	fb.setFail(false)
	if wb.opts.Staging.Pending.HasPending("/settle-src.txt") {
		t.Fatal("the successor's pending entry stayed keyed under the dead old name")
	}
	meta, ok := wb.opts.Staging.Pending.GetMeta("/settle-dst.txt")
	if !ok || meta == nil || meta.Kind != staging.PendingConflict {
		t.Fatalf("pending entry at the new name = %+v ok=%v, want the moved successor's marker", meta, ok)
	}
	data, err := wb.opts.Staging.Shadow.ReadAll("/settle-dst.txt")
	if err != nil || string(data) != "successor never landed" {
		t.Fatalf("shadow at the new name = %q err=%v, want the successor's preserved bytes", data, err)
	}
	if err := wb.queue.PathTerminalFailure("/settle-dst.txt"); err == nil {
		t.Fatal("the moved successor's terminal failure is not reachable at the new name")
	}
	if err := h.Sync(ctx); err == nil {
		t.Fatal("strict Sync on the retargeted handle certified the never-landed successor")
	}
	if _, recreated := nodeString(fb.backend, "/settle-src.txt"); recreated {
		t.Fatal("the dead old name was resurrected by the successor's upload")
	}
}

// TestPortRenameRetargetedSuccessorLandsAtNewName is the healthy-backend
// counterpart: the staged successor is moved to the new name and its landing
// is re-driven THERE — the metadata-only rename preserved the base revision,
// so the re-driven upload CASes cleanly and the retargeted handle's strict
// Sync reports durability for the successor's bytes, which land under the
// new name and only there.
func TestPortRenameRetargetedSuccessorLandsAtNewName(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	gate := make(chan struct{})
	gated := make(chan struct{})
	fb.renameGate, fb.gated = gate, gated
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/land-src.txt")
	if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	renamed := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/land-src.txt", Path2: "/land-dst.txt"})
		renamed <- err
	}()
	select {
	case <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("the rename request never reached the backend")
	}
	// Failing uploads only until the rename returns keep the successor from
	// landing at the still-live old name during the window; afterwards the
	// backend is healthy so the re-driven landing must succeed at the NEW name.
	fb.setFail(true)
	if err := h.Write(ctx, 0, []byte("successor landed at the new name")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(gate)

	select {
	case err := <-renamed:
		if err != nil {
			t.Fatalf("rename reported %v, want success after moving the successor", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the rename never moved the successor staged during its RPC")
	}
	fb.setFail(false)

	if wb.opts.Staging.Pending.HasPending("/land-src.txt") {
		t.Fatal("the successor's pending entry stayed keyed under the dead old name")
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on the retargeted handle = %v, want the re-driven landing at the new name to succeed", err)
	}
	if err := wb.queue.PathTerminalFailure("/land-dst.txt"); err != nil {
		t.Fatalf("the landed successor still answers EIO: %v", err)
	}
	if got, ok := nodeString(fb.backend, "/land-dst.txt"); !ok || got != "successor landed at the new name" {
		t.Fatalf("remote image at the new name = %q ok=%v, want the successor's bytes", got, ok)
	}
	if _, recreated := nodeString(fb.backend, "/land-src.txt"); recreated {
		t.Fatal("the dead old name was resurrected by the successor's upload")
	}
}

// TestPortDirRenameRetargetsQueuedDescendant pins the directory-rename row of
// the same window: a child staged (and enqueued) under the old prefix while
// the rename RPC was in flight is LIVE, not terminal, and its durability
// obligation must follow the move like the renamed path's own staged image —
// its upload may not run against (or recreate) the dead prefix.
func TestPortDirRenameRetargetsQueuedDescendant(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	gate := make(chan struct{})
	gated := make(chan struct{})
	fb.renameGate, fb.gated = gate, gated
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/a")
	if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	renamed := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"})
		renamed <- err
	}()
	select {
	case <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("the rename request never reached the backend")
	}
	fb.setFail(true)
	if err := h.Write(ctx, 0, []byte("descendant successor")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(gate)

	select {
	case err := <-renamed:
		if err != nil {
			t.Fatalf("directory rename reported %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the directory rename never completed")
	}
	fb.setFail(false)

	if wb.opts.Staging.Pending.HasPending("/d/a") {
		t.Fatal("the queued descendant stayed keyed under the dead old prefix")
	}
	meta, ok := wb.opts.Staging.Pending.GetMeta("/e/a")
	if !ok || meta == nil {
		t.Fatal("the queued descendant did not follow the directory rename")
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on the retargeted descendant = %v, want the re-driven landing at the new prefix to succeed", err)
	}
	if got, ok := nodeString(fb.backend, "/e/a"); !ok || got != "descendant successor" {
		t.Fatalf("remote image at the new prefix = %q ok=%v, want the descendant's successor bytes", got, ok)
	}
	if _, recreated := nodeString(fb.backend, "/d/a"); recreated {
		t.Fatal("the dead old prefix was resurrected by the descendant's upload")
	}
}

// TestPortRenameRefusesDataLandedAtObsoleteName pins the resurrection guard:
// a worker that reached the old name before the transition's locks can land
// the in-window successor THERE — its not-found retry recreates the dead
// name. An upload acknowledged at the obsolete pathname is not durability
// for the renamed object, so the transition must refuse to certify it.
func TestPortRenameRefusesDataLandedAtObsoleteName(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// A completed clean rename: the source landed and moved.
	h := open(t, ch, "/obs-src.txt")
	if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/obs-src.txt", Path2: "/obs-dst.txt"}); err != nil {
		t.Fatal(err)
	}

	// The resurrection residue: the old name exists remotely again, exactly
	// as the landed-at-the-old-name successor's create-retry leaves it.
	squatter := open(t, ch, "/obs-src.txt")
	if err := squatter.Write(ctx, 0, []byte("landed at the obsolete name")); err != nil {
		t.Fatal(err)
	}
	if err := squatter.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	unlock := wb.lockRenamePaths("/obs-src.txt", "/obs-dst.txt")
	err := wb.conflictTransitionLocked(ctx, "/obs-src.txt", "/obs-dst.txt", renameFence{}, true)
	unlock()
	if err == nil {
		t.Fatal("the transition certified data landed under the obsolete old name")
	}
	if err := wb.queue.PathTerminalFailure("/obs-dst.txt"); err == nil {
		t.Fatal("the resurrection refusal did not leave a failure record at the new name")
	}
}

// TestPortDirRenameCarriesStandaloneFailureRecords pins the second rename of
// the reviewer's chain: a descendant transfer failure leaves a standalone
// failure record at the new name with NO pending entry behind it (the marker
// and bytes stay under the pre-move name), so neither prefix loop of a later
// directory rename would carry it. The record must follow the move —
// rekeyed with the prefix — or the retargeted handle's strict Sync would
// check a dead name and certify data that never landed.
func TestPortDirRenameCarriesStandaloneFailureRecords(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// The pre-move residue: a terminally failed image under /d/a whose
	// marker and preserved bytes stay there.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	src := open(t, ch, "/d/a")
	if err := src.Write(ctx, 0, []byte("landed base")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)
	if err := src.Write(ctx, 0, []byte("unlanded")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err == nil {
		t.Fatal("fsync reported durability during the upload outage")
	}
	fb.setFail(false)

	// The failed /d -> /e transfer's destination-only residue: the record the
	// descendant transfer leaves at the new name when its marker could not be
	// installed (prepare ENOSPC), while the durable marker stays at /d/a.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/e", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	incumbent := open(t, ch, "/e/a")
	if err := incumbent.Write(ctx, 0, []byte("incumbent")); err != nil {
		t.Fatal(err)
	}
	if err := incumbent.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	wb.queue.RecordTerminalFailureForPath("/e/a", errors.New("simulated failed transfer: prepare /e/a.meta: no space left on device"))
	if err := wb.queue.PathTerminalFailure("/e/a"); err == nil {
		t.Fatal("the standalone record did not land at /e/a")
	}

	// The second directory rename: nothing under /e has a pending entry, so
	// only the record rekey can carry the witness.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/e", Path2: "/f"}); err != nil {
		t.Fatalf("directory rename: %v", err)
	}
	if err := wb.queue.PathTerminalFailure("/e/a"); err != nil {
		t.Fatalf("the standalone record stayed at the dead name /e/a: %v", err)
	}
	if err := wb.queue.PathTerminalFailure("/f/a"); err == nil {
		t.Fatal("the standalone record did not follow the directory rename to /f/a")
	}
	// The barrier on the renamed name answers EIO, and the durable marker
	// that stayed behind documents where the unlanded bytes still are.
	moved := open(t, ch, "/f/a")
	if err := moved.Sync(ctx); err == nil {
		t.Fatal("strict Sync at /f/a certified data whose unlanded image never landed")
	}
	if meta, ok := wb.opts.Staging.Pending.GetMeta("/d/a"); !ok || meta == nil || meta.Kind != staging.PendingConflict {
		t.Fatalf("pre-move marker at /d/a = %+v ok=%v, want the preserved conflict entry", meta, ok)
	}
}

// TestPortRenameFailsClosedWhenOldNameStatErrors pins the fail-closed row of
// the obsolete-name guard: a Stat that cannot decide (timeout, 5xx) is not
// evidence that the successor landed at the new name, so the transition must
// preserve the durability obligation instead of silently accepting — with or
// without a resurrection actually sitting at the old name.
func TestPortRenameFailsClosedWhenOldNameStatErrors(t *testing.T) {
	for _, resurrected := range []bool{false, true} {
		fb := &failingWritesBackend{backend: testbackend.NewBackend()}
		ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
		ctx := context.Background()

		h := open(t, ch, "/stat-src.txt")
		if err := h.Write(ctx, 0, []byte("landed base")); err != nil {
			t.Fatal(err)
		}
		if err := h.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/stat-src.txt", Path2: "/stat-dst.txt"}); err != nil {
			t.Fatal(err)
		}
		if resurrected {
			squatter := open(t, ch, "/stat-src.txt")
			if err := squatter.Write(ctx, 0, []byte("landed at the obsolete name")); err != nil {
				t.Fatal(err)
			}
			if err := squatter.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}

		fb.setFailStat(true)
		unlock := wb.lockRenamePaths("/stat-src.txt", "/stat-dst.txt")
		err := wb.conflictTransitionLocked(ctx, "/stat-src.txt", "/stat-dst.txt", renameFence{}, true)
		unlock()
		if err == nil {
			t.Fatalf("resurrected=%v: the transition certified the rename although the old name could not be observed", resurrected)
		}
		if err := wb.queue.PathTerminalFailure("/stat-dst.txt"); err == nil {
			t.Fatalf("resurrected=%v: the undecided Stat left no failure record", resurrected)
		}
	}
}

// TestPortDirRenameKeepsSuccessorPublishedAfterRetarget pins the reviewer's
// interleaving: after the directory rename moves the handles, an open writer
// can Flush a NEWER image at the new name before the descendant loop reaches
// the child. That publication belongs to the renamed object's new
// incarnation — it must not be retired as the replaced target's, and the
// older queued image must not land over it. The older image stays preserved
// under the old name, EIO answers at the new name until the newer image
// lands, and the landing clears the record.
func TestPortDirRenameKeepsSuccessorPublishedAfterRetarget(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/a")
	if err := h.Write(ctx, 0, []byte("v1 landed")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// G2 staged "during the rename RPC" and left queued (uploads failing).
	fb.setFail(true)
	if err := h.Write(ctx, 0, []byte("v2 queued")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The rename completes, the handles move, and the retargeted writer
	// publishes the NEWER G3 at the new name before the descendant loop
	// acquires the child locks.
	movedAt := time.Now()
	wb.retarget("/d", "/e")
	if err := h.Write(ctx, 0, []byte("v3 newest")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := wb.retargetQueuedImage("/d/a", "/e/a", renameFence{movedAt: movedAt}); err == nil {
		t.Fatal("the move retired a newer post-rename publication at the destination")
	}
	// G3 is untouched at the new name — still queued, still live.
	dstMeta, ok := wb.opts.Staging.Pending.GetMeta("/e/a")
	if !ok || dstMeta == nil || dstMeta.Kind == staging.PendingConflict || dstMeta.Size != int64(len("v3 newest")) {
		t.Fatalf("newer image at the new name = %+v ok=%v, want the untouched live G3", dstMeta, ok)
	}
	// The older G2 stayed preserved under the old name as a conflict marker
	// with its bytes, and nothing re-drives it at the dead prefix.
	srcMeta, ok := wb.opts.Staging.Pending.GetMeta("/d/a")
	if !ok || srcMeta == nil || srcMeta.Kind != staging.PendingConflict {
		t.Fatalf("older image under the old name = %+v ok=%v, want the preserved conflict marker", srcMeta, ok)
	}
	if !wb.opts.Staging.Shadow.Has("/d/a") {
		t.Fatal("the preserved older image lost its payload")
	}
	if err := wb.queue.PathTerminalFailure("/e/a"); err == nil {
		t.Fatal("the refused move left no failure record at the new name")
	}

	// The writer's barrier: with the backend healthy again, G3 lands at the
	// new name, its landing clears the record, and strict Sync answers nil —
	// for the NEWEST bytes, not the older queued image.
	fb.setFail(false)
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on the retargeted writer = %v, want G3 to land and clear the record", err)
	}
	if got, ok := nodeString(fb.backend, "/e/a"); !ok || got != "v3 newest" {
		t.Fatalf("remote image at the new name = %q ok=%v, want G3's bytes", got, ok)
	}
	if err := wb.queue.PathTerminalFailure("/e/a"); err != nil {
		t.Fatalf("the landed G3 still answers EIO: %v", err)
	}
}

// TestPortRenameCatchesLandingBeforeResponse pins the lost-history row of
// the in-window detector: the server applies the rename but the response is
// delayed, and the in-window successor's worker finishes INSIDE that delay —
// its 404/create retry recreates the old name, its pending entry is removed,
// and the queue is idle again before the handler resumes. The landed
// watermark (not HasPath) must still flag the window, the strict old-name
// check must catch the resurrection, and the rename must answer EIO instead
// of certifying a destination that never contained the successor.
func TestPortRenameCatchesLandingBeforeResponse(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	gate := make(chan struct{})
	gated := make(chan struct{})
	fb.renameGate, fb.gated = gate, gated
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/hist-src.txt")
	if err := h.Write(ctx, 0, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// The rename applies server-side; only its response is held. Inside the
	// delay the writer stages the successor and — uploads healthy — its
	// worker resurrects the OLD name and finishes before the gate opens.
	renamed := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/hist-src.txt", Path2: "/hist-dst.txt"})
		renamed <- err
	}()
	select {
	case <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("the rename request never reached the backend")
	}
	if err := h.Write(ctx, 0, []byte("v2 resurrected the old name")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !wb.opts.Staging.Pending.HasPending("/hist-src.txt") {
			if _, ok := nodeString(fb.backend, "/hist-src.txt"); ok {
				break // the worker landed (and recreated) the old name
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if wb.opts.Staging.Pending.HasPending("/hist-src.txt") {
		t.Fatal("the successor never landed inside the response delay")
	}
	if _, ok := nodeString(fb.backend, "/hist-src.txt"); !ok {
		t.Fatal("the old name was not resurrected by the create retry")
	}
	close(gate)

	select {
	case err := <-renamed:
		if err == nil {
			t.Fatal("the rename certified a destination that never contained the successor")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the rename never completed")
	}
	if err := wb.queue.PathTerminalFailure("/hist-dst.txt"); err == nil {
		t.Fatal("the resurrection left no failure witness at the new name")
	}
	if got, ok := nodeString(fb.backend, "/hist-dst.txt"); !ok || got != "v1" {
		t.Fatalf("remote image at the new name = %q ok=%v, want the stale pre-successor bytes", got, ok)
	}
}

// TestPortRenameKeepsFreshOpenerStagedDuringResponse pins the incarnation
// fence's revision row: between the server-side rename and the delayed
// response, a fresh writable open of the destination sees the RENAMED object
// and stages an overwrite carrying its revision. That publication belongs to
// the new incarnation even though it predates the response, and the live
// image move must refuse to retire it — the older queued image stays
// preserved under the old name and EIO answers at the destination until the
// newer image lands. The server-side move is driven through the raw client
// (the port chain serializes dispatches, so the rename handler itself cannot
// be held mid-RPC while another dispatch runs).
func TestPortRenameKeepsFreshOpenerStagedDuringResponse(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	// The source lands (its revision is the renamed object's), and the
	// destination exists with its own landed incumbent.
	src := open(t, ch, "/inc-src.txt")
	if err := src.Write(ctx, 0, []byte("source v1")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	srcRev, ok := wb.landedRevision("/inc-src.txt")
	if !ok || srcRev <= 0 {
		t.Fatalf("source revision = %d ok=%v, want the landed watermark", srcRev, ok)
	}
	dst := open(t, ch, "/inc-dst.txt")
	if err := dst.Write(ctx, 0, []byte("old incumbent")); err != nil {
		t.Fatal(err)
	}
	if err := dst.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// The server applies the rename; the writeback handler has not run (its
	// response never arrived). Inside that delay a fresh writable open of the
	// destination observes the RENAMED object (opened before the outage so
	// the open itself is not the failing plane) ...
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/inc-src.txt"), wb.remotePath("/inc-dst.txt")); err != nil {
		t.Fatal(err)
	}
	fresh := open(t, ch, "/inc-dst.txt")
	// ... and with uploads now failing, both sides stage inside the delay:
	// the source's writer queues an older successor, and the fresh opener
	// stages its overwrite of the renamed object (so both stay live).
	fb.setFail(true)
	if err := src.Write(ctx, 0, []byte("source v2 queued")); err != nil {
		t.Fatal(err)
	}
	if err := src.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Write(ctx, 0, []byte("fresh opener over the renamed object")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	dstMeta, ok := wb.opts.Staging.Pending.GetMeta("/inc-dst.txt")
	if !ok || dstMeta == nil {
		t.Fatal("the fresh opener did not stage at the destination")
	}
	if dstMeta.BaseRev != srcRev {
		t.Fatalf("fresh opener staged against rev %d, want the renamed object's rev %d", dstMeta.BaseRev, srcRev)
	}

	// The delayed response arrives NOW: the fresh opener's publication
	// predates it, and only the revision fence can tell the incarnations
	// apart.
	if err := wb.retargetQueuedImage("/inc-src.txt", "/inc-dst.txt", renameFence{movedAt: time.Now(), srcRev: srcRev}); err == nil {
		t.Fatal("the move retired a fresh opener's publication that predates the response")
	}

	// The fresh opener's image is intact at the destination and still live.
	dstMeta, ok = wb.opts.Staging.Pending.GetMeta("/inc-dst.txt")
	if !ok || dstMeta == nil || dstMeta.Kind == staging.PendingConflict {
		t.Fatalf("destination entry = %+v ok=%v, want the fresh opener's untouched live image", dstMeta, ok)
	}
	// The older queued successor stayed preserved under the old name.
	srcMeta, ok := wb.opts.Staging.Pending.GetMeta("/inc-src.txt")
	if !ok || srcMeta == nil || srcMeta.Kind != staging.PendingConflict {
		t.Fatalf("old-name entry = %+v ok=%v, want the preserved conflict marker", srcMeta, ok)
	}
	if err := wb.queue.PathTerminalFailure("/inc-dst.txt"); err == nil {
		t.Fatal("the refused move left no failure record at the destination")
	}

	// With the backend healthy the fresh opener's image lands and clears
	// the record; the barrier then answers for THOSE bytes.
	fb.setFail(false)
	if err := fresh.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on the fresh opener = %v, want its image to land and clear the record", err)
	}
	if got, ok := nodeString(fb.backend, "/inc-dst.txt"); !ok || got != "fresh opener over the renamed object" {
		t.Fatalf("remote image at the destination = %q ok=%v, want the fresh opener's bytes", got, ok)
	}
}

// TestPortDirRenameCatchesRepeatChildLanding pins the repeat-landing row of
// the in-window child detector: a child that had ALREADY landed before the
// RPC lands AGAIN in place during the delayed response (the watermark is
// replaced, so a set snapshot cannot tell it from the pre-existing landing).
// The worker's create retry recreates the child under the dead prefix and
// finishes before the response arrives; the rename must still refuse to
// certify the child's new name and leave a failure witness there.
func TestPortDirRenameCatchesRepeatChildLanding(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	gate := make(chan struct{})
	gated := make(chan struct{})
	fb.renameGate, fb.gated = gate, gated
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	// G1 lands BEFORE the rename, so the child already has a landed
	// watermark when the delayed response window opens.
	h := open(t, ch, "/d/f")
	if err := h.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	renamed := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"})
		renamed <- err
	}()
	select {
	case <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("the rename request never reached the backend")
	}
	// Inside the delayed response the same writer stages G2 and — uploads
	// healthy — the worker resurrects the child under the dead prefix and
	// finishes, replacing the child's existing watermark in place.
	if err := h.Write(ctx, 0, []byte("child v2 resurrected")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !wb.opts.Staging.Pending.HasPending("/d/f") {
			if _, ok := nodeString(fb.backend, "/d/f"); ok {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if wb.opts.Staging.Pending.HasPending("/d/f") {
		t.Fatal("the successor never landed inside the response delay")
	}
	if _, ok := nodeString(fb.backend, "/d/f"); !ok {
		t.Fatal("the dead prefix child was not resurrected by the create retry")
	}
	close(gate)

	// The rename itself answers success (the remote move happened and the
	// child's failure is per-descendant), but the barrier must not certify
	// the child: the resurrection leaves a failure witness at its new name,
	// where the retargeted handle's strict Sync will answer EIO.
	select {
	case err := <-renamed:
		if err != nil {
			t.Fatalf("directory rename reported %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the directory rename never completed")
	}
	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the child resurrection left no failure witness at the new prefix")
	}
	if got, ok := nodeString(fb.backend, "/e/f"); !ok || got != "child v1" {
		t.Fatalf("remote image at the new prefix = %q ok=%v, want the stale pre-successor bytes", got, ok)
	}
}

// TestPortDirRenameKeepsFreshChildOpenerStagedDuringResponse pins the
// child-revision row of the incarnation fence for directory renames: during
// the delayed response a fresh writable open of the renamed CHILD stages
// over the CHILD's revision, which differs from the directory's — the fence
// must classify by the child's own revision or it would retire that newer
// publication as the replaced incumbent's and move the older queued image
// over it. The server-side move is driven through the raw client (the port
// chain serializes dispatches).
func TestPortDirRenameKeepsFreshChildOpenerStagedDuringResponse(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	src := open(t, ch, "/d/f")
	if err := src.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	childRev, ok := wb.landedRevision("/d/f")
	if !ok || childRev <= 0 {
		t.Fatalf("child revision = %d ok=%v, want the landed watermark", childRev, ok)
	}

	// The server applies the directory rename; the writeback handler has
	// not run. Inside the delay: the source's writer queues an older G2
	// under the old prefix ...
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}
	fresh := open(t, ch, "/e/f")
	fb.setFail(true)
	if err := src.Write(ctx, 0, []byte("child v2 queued")); err != nil {
		t.Fatal(err)
	}
	if err := src.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// ... and the fresh opener stages G3 over the renamed CHILD's revision.
	if err := fresh.Write(ctx, 0, []byte("fresh child opener")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	dstMeta, ok := wb.opts.Staging.Pending.GetMeta("/e/f")
	if !ok || dstMeta == nil {
		t.Fatal("the fresh opener did not stage at the child's new name")
	}
	if dstMeta.BaseRev != childRev {
		t.Fatalf("fresh opener staged against rev %d, want the renamed child's rev %d", dstMeta.BaseRev, childRev)
	}

	// The child's fence must classify G3 by the CHILD's revision and refuse
	// the move; the directory's own revision (or zero) classifies nothing.
	if err := wb.retargetQueuedImage("/d/f", "/e/f", wb.childFence(ctx, "/e/f", time.Now())); err == nil {
		t.Fatal("the move retired a fresh child opener's publication that predates the response")
	}
	dstMeta, ok = wb.opts.Staging.Pending.GetMeta("/e/f")
	if !ok || dstMeta == nil || dstMeta.Kind == staging.PendingConflict {
		t.Fatalf("child destination entry = %+v ok=%v, want the fresh opener's untouched live image", dstMeta, ok)
	}
	srcMeta, ok := wb.opts.Staging.Pending.GetMeta("/d/f")
	if !ok || srcMeta == nil || srcMeta.Kind != staging.PendingConflict {
		t.Fatalf("old-prefix child entry = %+v ok=%v, want the preserved conflict marker", srcMeta, ok)
	}
	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the refused move left no failure record at the child's new name")
	}

	// With the backend healthy the fresh opener's image lands and clears
	// the record.
	fb.setFail(false)
	if err := fresh.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on the fresh child opener = %v, want its image to land and clear the record", err)
	}
	if got, ok := nodeString(fb.backend, "/e/f"); !ok || got != "fresh child opener" {
		t.Fatalf("remote image at the child's new name = %q ok=%v, want the fresh opener's bytes", got, ok)
	}
}
