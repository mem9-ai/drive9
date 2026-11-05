package writeback

// Strict-fsync error propagation: under write-back with strict durability,
// fsync waits for the path to land through the commit queue. A terminally
// failed commit also makes the path idle — the wait used to return success
// for data that exists only in preserved local pending state. fsync must
// answer EIO instead, stickily, until a later commit of the same path lands.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	if _, err := wb.retargetConflictMarkers(context.Background(), "/race-src.txt", "/race-dst.txt", renameFence{}, false); err != nil {
		t.Fatalf("retarget reported the fenced-out transfer as a failure: %v", err)
	}
	// What the rename caller does on a successful transition.
	wb.queue.RenameTerminalFailure("/race-src.txt", "/race-dst.txt", nil)

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
	_, err := wb.conflictTransitionLocked(ctx, "/obs-src.txt", "/obs-dst.txt", renameFence{}, true)
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
		_, err := wb.conflictTransitionLocked(ctx, "/stat-src.txt", "/stat-dst.txt", renameFence{}, true)
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

	if _, err := wb.retargetQueuedImage("/d/a", "/e/a", renameFence{movedAt: movedAt}); err == nil {
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
	if _, err := wb.retargetQueuedImage("/inc-src.txt", "/inc-dst.txt", renameFence{movedAt: time.Now()}); err == nil {
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
	// The event stream drops mid-window (EventUnverified clears the whole
	// landed table — disconnects emit it regardless of actors), wiping the
	// writeback watermark for /d/f before the rename response is processed:
	// the queue-side landmark must still carry the evidence.
	wb.clearLanded()
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
	if _, err := wb.retargetQueuedImage("/d/f", "/e/f", renameFence{movedAt: time.Now()}); err == nil {
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

// TestPortRenameCarriesPublicationFromRetargetGap pins the final-publication
// window: between the rename transition releasing its path locks and the
// handles moving to the new name, an open writer can still stage (and
// enqueue, clearing its dirty flag) under the OLD name — the one window no
// earlier snapshot covers. The second pass after the handle retarget must
// carry that stranded publication to the new name, where the retargeted
// handle's strict Sync waits its landing instead of certifying without it.
func TestPortRenameCarriesPublicationFromRetargetGap(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	h := open(t, ch, "/gap-src.txt")
	if err := h.Write(ctx, 0, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if rev, ok := wb.landedRevision("/gap-src.txt"); !ok || rev <= 0 {
		t.Fatalf("source revision = %d ok=%v", rev, ok)
	}

	// The server applies the rename (this sequence exercises the handler's
	// post-RPC phases directly); srcRev is the pre-RPC source stat.
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/gap-src.txt"), wb.remotePath("/gap-dst.txt")); err != nil {
		t.Fatal(err)
	}
	// First pass: the clean rename's transition (dstPreGen mirrors the
	// handler's pre-RPC destination snapshot; the destination is absent).
	fence := renameFence{movedAt: time.Now()}
	installedGen, terr := wb.retargetConflictMarkers(ctx, "/gap-src.txt", "/gap-dst.txt", fence, false)
	if terr != nil {
		t.Fatalf("first transition pass: %v", terr)
	}
	// THE GAP: locks released, handles not yet moved — the writer stages a
	// fresh image under the old name and its Flush clears the dirty flag.
	fb.setFail(true)
	if err := h.Write(ctx, 0, []byte("gap v2")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The handle retarget: publications now go to the new name only.
	wb.retarget("/gap-src.txt", "/gap-dst.txt")
	// The handler's second pass: the stranded old-name publication moves.
	secondFence := renameFence{movedAt: fence.movedAt, ownGen: installedGen}
	secondWork := (wb.queue != nil && wb.queue.HasPath("/gap-src.txt")) || wb.everLanded("/gap-src.txt")
	if !secondWork {
		t.Fatal("the gap publication was not detected as in-window work")
	}
	if _, terr := wb.retargetConflictMarkers(ctx, "/gap-src.txt", "/gap-dst.txt", secondFence, secondWork); terr != nil {
		t.Fatalf("second transition pass: %v", terr)
	}

	if wb.opts.Staging.Pending.HasPending("/gap-src.txt") {
		t.Fatal("the gap publication stayed stranded under the dead old name")
	}
	// The retargeted handle's strict Sync waits the re-driven landing.
	fb.setFail(false)
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("strict Sync after the gap publication = %v, want the re-driven landing", err)
	}
	if got, ok := nodeString(fb.backend, "/gap-dst.txt"); !ok || got != "gap v2" {
		t.Fatalf("remote image at the new name = %q ok=%v, want the gap publication's bytes", got, ok)
	}
	if _, recreated := nodeString(fb.backend, "/gap-src.txt"); recreated {
		t.Fatal("the dead old name was resurrected")
	}
}

// TestPortChildFenceProtectsAdvanceToNewerRevision pins the lineage row of
// the child fence: a fresh opener stages G3 over the renamed child's R1
// during the delayed response, and ANOTHER client (a second mount) advances
// the child to R2 before the fence Stat. Exact equality with the current
// revision would classify G3 as the replaced incumbent's; the fence's
// source-entry base revision (the renamed lineage's at-rename point) must
// protect it.
func TestPortChildFenceProtectsAdvanceToNewerRevision(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := landChild(t, ch, "/d/f", "child v1"); err != nil {
		t.Fatal(err)
	}
	childRev, ok := wb.landedRevision("/d/f")
	if !ok || childRev <= 0 {
		t.Fatalf("child revision = %d ok=%v", childRev, ok)
	}
	// The server applies the directory rename; the handler never runs.
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}

	// The old-name straggler: a FRESH opener of the old child (so its
	// staging carries the child's real revision, not a stale create shape).
	straggler := open(t, ch, "/d/f")
	// The fresh destination opener stages G3 over the renamed child (R1).
	fresh := open(t, ch, "/e/f")
	fb.setFail(true)
	if err := straggler.Write(ctx, 0, []byte("straggler over R1")); err != nil {
		t.Fatal(err)
	}
	if err := straggler.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Write(ctx, 0, []byte("G3 over renamed child")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	g3, ok2 := wb.opts.Staging.Pending.GetMeta("/e/f")
	if !ok2 || g3 == nil {
		t.Fatal("the fresh opener must be pending at the destination")
	}
	if g3.BaseRev != childRev {
		t.Fatalf("G3 base=%d, want the renamed child's rev %d", g3.BaseRev, childRev)
	}
	// ANOTHER client advances the child to R2 before the fence Stat — on
	// its own backend wrapper, so this mount's queued straggler/G3 stay
	// pending (a shared failure switch would let them land first).
	fb2 := &failingWritesBackend{backend: fb.backend}
	ch2, _ := newUploadStack(t, fb2, Options{Policy: PolicyWriteBack})
	other := open(t, ch2, "/e/f")
	if err := other.Write(ctx, 0, []byte("another client advances to R2")); err != nil {
		t.Fatal(err)
	}
	if err := other.Sync(ctx); err != nil {
		t.Fatalf("the other client's landing: %v", err)
	}

	// A child fence has no per-child old revision, and G3 carries the
	// renamed child's revision rather than a create-shaped base: it can
	// never be POSITIVELY proven the replaced incumbent's, however far the
	// current revision has advanced, so the move must refuse.
	if _, err := wb.retargetQueuedImage("/d/f", "/e/f", renameFence{movedAt: time.Now()}); err == nil {
		t.Fatal("the move retired G3 although its base belongs to the renamed child's lineage")
	}
	g3After, ok := wb.opts.Staging.Pending.GetMeta("/e/f")
	if !ok || g3After == nil || g3After.Kind == staging.PendingConflict || g3After.BaseRev != childRev {
		t.Fatalf("destination entry = %+v ok=%v, want G3 untouched", g3After, ok)
	}
	srcMeta, ok := wb.opts.Staging.Pending.GetMeta("/d/f")
	if !ok || srcMeta == nil || srcMeta.Kind != staging.PendingConflict {
		t.Fatalf("old-prefix entry = %+v ok=%v, want the preserved conflict marker", srcMeta, ok)
	}
	// G3 lands once uploads recover and clears the record.
	fb.setFail(false)
	if err := fresh.Sync(ctx); err != nil {
		t.Fatalf("strict Sync on G3 = %v, want it to land and clear the record", err)
	}
}

// TestRecoveryKeepsShadowlessConflictWitness pins the recovery rule the
// pre-move crash witness depends on: a conflict marker without a shadow is
// PRESERVED (it is the durable EIO witness a crashed transfer left at the
// renamed name, with the bytes preserved under the old one), while a
// shadowless LIVE entry stays pruned as an orphan.
func TestRecoveryKeepsShadowlessConflictWitness(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/witness.txt", 7, staging.PendingNew, 3); err != nil {
		t.Fatal(err)
	}
	if err := pending.MarkConflict("/witness.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/orphan.txt", 7, staging.PendingNew, 3); err != nil {
		t.Fatal(err)
	}
	shadows, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	cq := NewCommitQueue(nil, shadows, pending, nil, 1, 8)
	cq.RecoverPending()

	if !pending.HasPending("/witness.txt") {
		t.Fatal("recovery pruned the shadowless conflict witness")
	}
	if err := cq.PathTerminalFailure("/witness.txt"); err == nil {
		t.Fatal("the preserved witness does not answer EIO")
	}
	if pending.HasPending("/orphan.txt") {
		t.Fatal("recovery kept a shadowless live entry")
	}
}

// landChild lands one image through a fresh handle and returns.
func landChild(t *testing.T, ch *fs.Chain, path, content string) error {
	t.Helper()
	h := open(t, ch, path)
	if err := h.Write(context.Background(), 0, []byte(content)); err != nil {
		return err
	}
	return h.Sync(context.Background())
}

// TestDstProtectedLineageRules pins the incarnation classifier's identity
// rule directly: retiring a destination entry requires its pending
// GENERATION to equal the pre-RPC snapshot of that same path — generations
// come from one global counter, so the match proves the entry is the very
// one that existed before the rename (the replaced incumbent's). Revisions
// cannot do this (per-inode counters collide across objects; a post-rename
// opener can even stage create-shaped), so every non-matching generation —
// including fresh openers under any revision, and children without a
// snapshot — stays protected, and the rename's own installation is
// replaceable only by its own second pass.
func TestDstProtectedLineageRules(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	_, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})

	stage := func(path string) uint64 {
		t.Helper()
		gen, err := wb.opts.Staging.Pending.Put(path, 3, staging.PendingOverwrite)
		if err != nil {
			t.Fatal(err)
		}
		return gen
	}
	preGen := stage("/pre.txt") // existed before the rename
	stage("/fresh.txt")         // published during the window
	// movedAt AFTER the stagings: no entry may win on CreatedAt.
	now := time.Now()

	// The snapshotted generation is provably the pre-rename entry:
	// replaceable even under an advanced destination revision.
	if _, _, _, protected := wb.dstProtected("/pre.txt", renameFence{movedAt: now, preGen: map[string]uint64{"/pre.txt": preGen}}); protected {
		t.Fatal("the snapshotted pre-rename entry was classified protected")
	}

	// Any other generation — a fresh publication during the window, whatever
	// revision it carries — stays protected, and so does everything when the
	// path has no snapshot at all (children without one).
	if _, _, _, protected := wb.dstProtected("/fresh.txt", renameFence{movedAt: now, preGen: map[string]uint64{"/pre.txt": preGen}}); !protected {
		t.Fatal("an entry published during the window was retired without identity proof")
	}
	if _, _, _, protected := wb.dstProtected("/fresh.txt", renameFence{movedAt: now}); !protected {
		t.Fatal("an entry without a snapshot was retired without identity proof")
	}

	// The rename's own installation is replaceable — by exactly its own
	// second pass.
	if _, _, _, protected := wb.dstProtected("/fresh.txt", renameFence{movedAt: now, ownGen: wb.genOf(t, "/fresh.txt")}); protected {
		t.Fatal("the rename's own installation was not replaceable by its second pass")
	}
}

// genOf reads a path's current pending generation for fence assertions.
func (w *Writeback) genOf(t *testing.T, path string) uint64 {
	t.Helper()
	m, ok := w.opts.Staging.Pending.GetMeta(path)
	if !ok || m == nil {
		t.Fatalf("no pending entry at %s", path)
	}
	return m.Generation
}

// TestPortDirRenameCatchesChildLandedInRetargetGap pins the directory
// rename's final window: a child that stages AND COMPLETELY LANDS its
// successor under the obsolete prefix between the in-window snapshot and
// the handle retarget — its pending entry is gone and only the landed
// watermark remains, which the pre-retarget snapshot never contained. The
// post-retarget settlement must read the prefix's watermarks fresh (they
// were wiped before the RPC) and strict-check every entry-less child, so
// the resurrection leaves a failure witness at the child's new name.
func TestPortDirRenameCatchesChildLandedInRetargetGap(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/f")
	if err := h.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// The handler's pre-RPC watermark wipe: after this, the in-window
	// snapshot (nothing pending, no watermarks) does NOT contain the child —
	// exactly the state when its successor lands later than the snapshot.
	wb.forgetLanded("/d")
	wb.forgetLandedPrefix("/d/")
	// The server applies the directory rename; the handler never ran.
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}

	// THE GAP (between the snapshot and the handle retarget): the child
	// writer stages its successor and — uploads healthy — the worker's
	// create retry resurrects /d/f and COMPLETES, leaving only a watermark.
	if err := h.Write(ctx, 0, []byte("child v2 landed in the gap")); err != nil {
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
		t.Fatal("the gap successor never completed under the old prefix")
	}
	if _, ok := nodeString(fb.backend, "/d/f"); !ok {
		t.Fatal("the obsolete child name was not resurrected in the gap")
	}

	// The handle retarget and the handler's post-retarget settlement.
	wb.retarget("/d", "/e")
	wb.settleOldPrefixAfterMove(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}, time.Now(), nil, nil, nil, map[string]bool{})

	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the gap-completed child left no failure witness at the new prefix")
	}
	if got, ok := nodeString(fb.backend, "/e/f"); !ok || got != "child v1" {
		t.Fatalf("remote image at the new prefix = %q ok=%v, want the stale pre-successor bytes", got, ok)
	}
}

// TestSettleOldPrefixWithoutLedger pins the write-sync configuration:
// mount.go wires a staging ledger only for writeback/close-sync, so
// PolicyWriteSync runs with Staging == nil. An inline child write that
// completes under the old prefix after the watermark wipe leaves a landed
// watermark, and the post-move settlement must handle it without
// dereferencing the absent ledger — surfacing an old-name landing instead
// of panicking after the remote rename already succeeded.
func TestSettleOldPrefixWithoutLedger(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ts := httptest.NewServer(fb)
	t.Cleanup(ts.Close)
	c := newTestClient(ts.URL)
	c.Warm(context.Background())
	wb, err := New(Options{Policy: PolicyWriteSync, Client: c})
	if err != nil {
		t.Fatal(err)
	}
	if wb.opts.Staging != nil {
		t.Fatal("write-sync must run without a staging ledger for this row")
	}

	// The in-window child landing: only the watermark remains (no pending
	// entry exists to move), exactly what the fresh scan finds.
	wb.landedAt("/d/f", 7, 7, 0, nil)
	op := &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}
	// Must not panic on the nil ledger.
	wb.settleOldPrefixAfterMove(context.Background(), op, time.Now(), nil, nil, nil, map[string]bool{})
}

// TestStandaloneTransferFailureSurvivesRestart pins the restart row of the
// standalone rename-failure record: the successor already landed under the
// obsolete old name (its queue cleanup removed the pending metadata), the
// rename answers success, and the in-memory record alone would die with the
// process — a fresh queue would find neither a record nor a durable marker
// and certify the stale destination. The shadowless conflict witness the
// failure recording now installs must survive recovery and keep answering
// EIO at the new name.
func TestStandaloneTransferFailureSurvivesRestart(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/f")
	if err := h.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	wb.forgetLanded("/d")
	wb.forgetLandedPrefix("/d/")
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}
	// The gap publication lands under the resurrected old name.
	if err := h.Write(ctx, 0, []byte("child v2 landed in the gap")); err != nil {
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
		t.Fatal("the gap successor never completed under the old prefix")
	}

	// The settlement records the standalone failure at /e/f and — the fix —
	// installs the durable shadowless witness there too.
	wb.retarget("/d", "/e")
	wb.settleOldPrefixAfterMove(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}, time.Now(), nil, nil, nil, map[string]bool{})
	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("no failure recorded at the new prefix")
	}

	// Restart: a fresh queue over the recovered pending index. Only the
	// durable witness remains — recovery must preserve it (shadowless
	// conflict markers are witnesses, not orphans) and the barrier must
	// still answer EIO for the successor that never reached this name.
	recovered, err := staging.NewPendingIndex(wb.opts.Staging.Pending.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	cq2 := NewCommitQueue(nil, nil, recovered, nil, 1, 8)
	t.Cleanup(cq2.DrainAll)
	if err := cq2.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the standalone failure did not survive the restart")
	}
}

// TestRekeyDeclinesAfterDestinationLanding pins the record-rekey ordering:
// the transfer moves the source marker to the destination and releases its
// locks; a recovery write at the destination then lands, replaces the moved
// marker and clears the destination failure state — all BEFORE the endpoint
// rekey runs (the reviewer's interleaving had the re-driven worker do this;
// any landing at the destination fulfills the record's obligation the same
// way). Moving the old source record onto that landing would reinstall
// stale EIO forever; the landed-stamp fence must decline the move.
func TestRekeyDeclinesAfterDestinationLanding(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	src := open(t, ch, "/rk-src.txt")
	if err := src.Write(ctx, 0, []byte("failed image")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// A terminal failure at the source: its staging becomes the conflict
	// marker and the in-memory record is stamped there.
	fb.setFail(true)
	if err := src.Write(ctx, 0, []byte("never lands")); err != nil {
		t.Fatal(err)
	}
	_ = src.Sync(ctx) // EIO or EAGAIN; the marker below pins the state
	if m, ok := wb.opts.Staging.Pending.GetMeta("/rk-src.txt"); !ok || m == nil || m.Kind != staging.PendingConflict {
		t.Fatalf("source marker = %+v ok=%v", m, ok)
	}
	if err := wb.queue.PathTerminalFailure("/rk-src.txt"); err == nil {
		t.Fatal("no failure record at the source")
	}
	fb.setFail(false)
	// The server applies the rename; the handler never ran.
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/rk-src.txt"), wb.remotePath("/rk-dst.txt")); err != nil {
		t.Fatal(err)
	}

	// The handler's pre-transfer landed snapshot (empty: nothing landed at
	// the destination yet), then the transfer pass moving the marker.
	snaps := wb.queue.LandedSnapshotsForRename("/rk-dst.txt", "/rk-dst.txt/")
	if _, terr := wb.retargetConflictMarkers(ctx, "/rk-src.txt", "/rk-dst.txt", renameFence{movedAt: time.Now()}, false); terr != nil {
		t.Fatalf("transfer: %v", terr)
	}
	// The destination recovery lands BEFORE the rekey runs: a fresh write
	// through the renamed name replaces the moved marker and commits, and
	// its success clears the destination failure state exactly like the
	// re-driven worker's landing would.
	recovery := open(t, ch, "/rk-dst.txt")
	if err := recovery.Write(ctx, 0, []byte("recovery over the moved marker")); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Sync(ctx); err != nil {
		t.Fatalf("recovery landing: %v", err)
	}
	if err := wb.queue.PathTerminalFailure("/rk-dst.txt"); err != nil {
		t.Fatalf("the recovery landing left a failure at the destination: %v", err)
	}

	// The late rekey: the destination landed after the snapshot, so the old
	// source record must NOT be re-installed over it.
	wb.queue.RenameTerminalFailure("/rk-src.txt", "/rk-dst.txt", snaps)
	if err := wb.queue.PathTerminalFailure("/rk-dst.txt"); err != nil {
		t.Fatalf("the rekey reinstalled stale EIO over the landed recovery: %v", err)
	}
	// Without a landing the fence is inert: a live record still moves.
	wb.queue.RecordTerminalFailureForPath("/rk-other.txt", errors.New("boom"))
	wb.queue.RenameTerminalFailure("/rk-other.txt", "/rk-dst2.txt", snaps)
	if err := wb.queue.PathTerminalFailure("/rk-dst2.txt"); err == nil {
		t.Fatal("the unfenced rekey lost a live failure record")
	}
}

// TestMovedMarkerKeepsSupersessionChain pins the moved marker's
// supersession chain end to end: the rename move mints a fresh generation
// for the same failed image, and every retained proof of that image's
// recovery — a handle's fence, a pre-staged cache snapshot — references the
// SOURCE generation. The moved marker must remember that generation, and the
// queue's resolver must accept a landing that superseded any generation in
// the chain; without it a recovery whose fence predates the move answers
// EIO forever even though its bytes (which contain the failed image) are
// remote.
func TestMovedMarkerKeepsSupersessionChain(t *testing.T) {
	dir := t.TempDir()
	idx, err := staging.NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	shadows, err := staging.NewShadowStoreWithQuota(filepath.Join(dir, "shadows"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	if err := shadows.WriteFull("/a", []byte("failed image"), 0); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put("/a", int64(len("failed image")), staging.PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/a"); err != nil {
		t.Fatal(err)
	}
	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	if committed, cerr := transfer.Commit(); cerr != nil || !committed {
		t.Fatalf("commit = %v err=%v", committed, cerr)
	}
	moved, ok := idx.GetMeta("/b")
	if !ok || moved == nil || moved.Kind != staging.PendingConflict {
		t.Fatalf("moved marker = %+v ok=%v", moved, ok)
	}
	if moved.Generation <= gen || moved.SupersedesPendingGen != gen {
		t.Fatalf("moved marker gen=%d chain=%d, want gen > %d and chain == the source generation", moved.Generation, moved.SupersedesPendingGen, gen)
	}

	// The resolver, over the very marker the move produced (recovered the
	// way a restart would): a landing whose supersession fence is the SOURCE
	// generation resolves the moved marker through the chain; a fence below
	// the whole chain does not.
	recovered, err := staging.NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, nil, recovered, nil, 1, 8)
	t.Cleanup(cq.DrainAll)
	if m, ok := recovered.GetMeta("/b"); !ok || m == nil || m.SupersedesPendingGen != gen {
		t.Fatalf("recovered moved marker = %+v ok=%v, want the chain preserved", m, ok)
	}
	cq.ResolveSupersededConflict("/b", gen-1)
	if !recovered.HasPending("/b") {
		t.Fatal("a fence below the whole chain resolved the marker")
	}
	cq.ResolveSupersededConflict("/b", gen) // the chain's source generation
	if recovered.HasPending("/b") {
		t.Fatal("a landing that superseded the chain's source generation did not resolve the moved marker")
	}
}

// TestLargeChildLandingSurvivesClearLanded pins the >1MiB row of the
// event-clear window: the queue's landed LANDMARK is a selective lineage
// cache (no checksum above maxLandedPayloadBytes ⇒ no landmark), so a large
// successor that fully lands under the obsolete prefix and whose writeback
// watermark is then wiped by EventUnverified must still be detected — the
// rename's commit-seq stamps carry every successful commit regardless of
// size or entry provenance.
func TestLargeChildLandingSurvivesClearLanded(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/f")
	if err := h.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The rename's pre-RPC evidence wipe and commit-seq snapshot.
	wb.forgetLanded("/d")
	wb.forgetLandedPrefix("/d/")
	snap := wb.queue.CommitSeqsUnderPrefix("/d/")
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}
	// A >1MiB successor lands under the resurrected old name.
	big := bytes.Repeat([]byte("L"), (1<<20)+64)
	if err := h.Write(ctx, 0, big); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if !wb.opts.Staging.Pending.HasPending("/d/f") {
			if _, ok := nodeString(fb.backend, "/d/f"); ok {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if wb.opts.Staging.Pending.HasPending("/d/f") {
		t.Fatal("the large successor never completed under the old prefix")
	}
	// The selective landmark must indeed be absent for this payload — that
	// is the regression's premise — and the event stream wipes the
	// writeback watermarks before the response is processed.
	if seq := wb.queue.LandedSeq("/d/f"); seq != 0 {
		t.Fatalf("premise broken: large payload has a landmark (%d); the row no longer pins the cache gap", seq)
	}
	wb.clearLanded()

	// The handler's post-response union + settlement: commit-seq delta must
	// catch the child, the strict check must find the resurrection, and the
	// witness must land at /e/f.
	moved := wb.queue.CommitSeqsUnderPrefix("/d/")
	if moved["/d/f"] <= snap["/d/f"] {
		t.Fatal("the large landing did not advance the commit-seq stamp")
	}
	wb.retarget("/d", "/e")
	wb.settleOldPrefixAfterMove(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}, time.Now(), nil, nil, snap, map[string]bool{"/d/f": true})
	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the large child landing left no failure witness at the new prefix")
	}
}

// TestRekeyDeclinesAfterReDrivenLanding pins the original worker
// interleaving: the transfer's live-image branch re-drives the replacement
// through a RECOVERED entry (no SnapshotID, no landed landmark on success),
// the worker lands it and clears the destination state, and the late rekey
// must still decline — the commit-seq fence observes that landing even
// though the lineage cache never recorded it.
func TestRekeyDeclinesAfterReDrivenLanding(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	src := open(t, ch, "/rr-src.txt")
	if err := src.Write(ctx, 0, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The source's terminal failure record (standalone: no marker entry, so
	// the successor staged below stays LIVE — re-staging over a conflict
	// marker inherits the conflict, which is not the re-drive shape).
	wb.queue.RecordTerminalFailureForPath("/rr-src.txt", errors.New("terminal before the rename"))
	if err := wb.queue.PathTerminalFailure("/rr-src.txt"); err == nil {
		t.Fatal("no failure record at the source")
	}
	// The writer's replacement staged during the rename window with uploads
	// held by the outage — still LIVE at transfer time, the reviewer's shape.
	fb.setFail(true)
	if err := wb.opts.Staging.Shadow.WriteFull("/rr-src.txt", []byte("v3 re-driven"), 0); err != nil {
		t.Fatal(err)
	}
	baseRev, _ := wb.landedRevision("/rr-src.txt")
	if baseRev <= 0 {
		t.Fatal("no landed revision for the base")
	}
	if _, err := wb.opts.Staging.Pending.PutWithBaseRev("/rr-src.txt", int64(len("v3 re-driven")), staging.PendingOverwrite, baseRev); err != nil {
		t.Fatal(err)
	}
	if _, _, _, live := wb.pendingImageState("/rr-src.txt"); !live {
		t.Fatal("no live successor staged at the source")
	}
	fb.setFail(false)
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/rr-src.txt"), wb.remotePath("/rr-dst.txt")); err != nil {
		t.Fatal(err)
	}
	fb.setFail(true)

	// Pre-transfer commit-seq snapshot, then the transfer: the live image
	// moves and its landing is re-driven at the destination through a
	// recovered entry.
	snaps := wb.queue.LandedSnapshotsForRename("/rr-dst.txt", "/rr-dst.txt/")
	if _, terr := wb.retargetConflictMarkers(ctx, "/rr-src.txt", "/rr-dst.txt", renameFence{movedAt: time.Now()}, true); terr != nil {
		t.Fatalf("transfer: %v", terr)
	}
	// The re-driven worker lands BEFORE the rekey runs; the recovered entry
	// leaves no lineage landmark, but the commit-seq stamp advances.
	fb.setFail(false)
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if !wb.opts.Staging.Pending.HasPending("/rr-dst.txt") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if wb.opts.Staging.Pending.HasPending("/rr-dst.txt") {
		t.Fatal("the re-driven replacement never landed at the destination")
	}
	if seq := wb.queue.LandedSeq("/rr-dst.txt"); seq != 0 {
		t.Fatalf("premise broken: the recovered entry recorded a landmark (%d)", seq)
	}
	// The late rekey must decline: the landing fulfilled the source record.
	wb.queue.RenameTerminalFailure("/rr-src.txt", "/rr-dst.txt", snaps)
	if err := wb.queue.PathTerminalFailure("/rr-dst.txt"); err != nil {
		t.Fatalf("the rekey reinstalled stale EIO over the re-driven landing: %v", err)
	}
}

// TestWitnessMarkerDoesNotForgeTruncation pins the metadata row of the
// standalone witness: after the settlement installs the shadowless marker at
// /e/f (whose remote image is the nonempty v1), stat and list must keep
// reporting the real size — the witness carries an obligation, not a
// zero-byte staged image.
func TestWitnessMarkerDoesNotForgeTruncation(t *testing.T) {
	fb := &failingWritesBackend{backend: testbackend.NewBackend()}
	ch, wb := newUploadStack(t, fb, Options{Policy: PolicyWriteBack})
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/d/f")
	if err := h.Write(ctx, 0, []byte("child v1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	wb.forgetLanded("/d")
	wb.forgetLandedPrefix("/d/")
	if err := wb.opts.Client.RenameCtx(ctx, wb.remotePath("/d"), wb.remotePath("/e")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("child v2 landed in the gap")); err != nil {
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
	wb.retarget("/d", "/e")
	wb.settleOldPrefixAfterMove(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}, time.Now(), nil, nil, nil, map[string]bool{})
	marker, ok := wb.opts.Staging.Pending.GetMeta("/e/f")
	if !ok || marker == nil || !marker.WitnessOnly {
		t.Fatalf("witness marker = %+v ok=%v", marker, ok)
	}

	// The metadata overlays must skip the witness: stat keeps the remote's
	// real size instead of the marker's zero.
	if attr, ok := wb.pendingAttr("/e/f"); ok && attr.Size != int64(len("child v1")) {
		t.Fatalf("pendingAttr served the witness instead of the remote size: %+v", attr)
	}
	if size, _, _, ok := wb.pendingListAttr("/e/f"); ok && size == 0 {
		t.Fatal("pendingListAttr served the witness as a zero-byte image")
	}
	// And the barrier still answers EIO from it.
	if err := wb.queue.PathTerminalFailure("/e/f"); err == nil {
		t.Fatal("the witness stopped answering EIO")
	}
}
