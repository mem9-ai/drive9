// Crash-safety and concurrency rows for the generation-fenced conflict-marker
// transfer used by writeback's rename: the transfer must keep metadata and
// payload paired throughout, must not clobber a destination that already has
// its own entry, and must exclude same-path publications for its whole
// lifetime.

package staging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newConflictTransferFixture stages a conflicted entry at path in dir and
// returns the index, the generation, and a shadow store seeded with the
// entry's payload.
func newConflictTransferFixture(t *testing.T, dir, path, data string) (*PendingIndex, *ShadowStore, uint64) {
	t.Helper()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	shadows, err := NewShadowStoreWithQuota(filepath.Join(dir, "shadows"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	if err := shadows.WriteFull(path, []byte(data), 0); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put(path, int64(len(data)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict(path); err != nil {
		t.Fatal(err)
	}
	return idx, shadows, gen
}

// TestConflictTransferKeepsBothSidesRecoverable pins the crash window: after
// the prepare and before the payload move BOTH .meta files are on disk, so a
// restart keeps whichever side the payload actually backs, while memory still
// answers from the old name.
func TestConflictTransferKeepsBothSidesRecoverable(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "unlanded")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || transfer == nil {
		t.Fatal("begin did not open a transfer")
	}

	for _, path := range []string{"/a", "/b"} {
		raw, rerr := os.ReadFile(filepath.Join(dir, HashPath(path)+".meta"))
		if rerr != nil || len(raw) == 0 {
			t.Fatalf("no durable meta for %s after prepare: %v", path, rerr)
		}
	}
	// The move has not happened, so memory must not publish the new name.
	if idx.HasPending("/b") {
		t.Fatal("prepare published the new name before the payload moved")
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Generation != gen {
		t.Fatalf("old-name entry = %+v found=%v, want the untouched generation %d", meta, found, gen)
	}
	transfer.Abort()
}

// TestConflictTransferNeverOverwritesLiveDestination pins the CodeRabbit row:
// a destination that already carries its own entry keeps its persisted .meta
// byte-for-byte until commit, so an aborted transfer needs no restore and a
// crash cannot pair the source's metadata with the destination's payload.
func TestConflictTransferNeverOverwritesLiveDestination(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, srcGen := newConflictTransferFixture(t, dir, "/a", "src unlanded")

	// Give /b its OWN conflicted entry with distinct, recognizable metadata.
	dstGen, err := idx.PutWithBaseRev("/b", 11, PendingNew, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/b"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, HashPath("/b")+".meta"))
	if err != nil {
		t.Fatal(err)
	}

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", srcGen, dstGen, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v, want an open transfer", ok, err)
	}
	during, err := os.ReadFile(filepath.Join(dir, HashPath("/b")+".meta"))
	if err != nil {
		t.Fatal(err)
	}
	if string(during) != string(before) {
		t.Fatal("prepare rewrote a live destination's meta")
	}

	transfer.Abort()
	after, err := os.ReadFile(filepath.Join(dir, HashPath("/b")+".meta"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("abort changed the destination's persisted meta")
	}
	if meta, found := idx.GetMeta("/b"); !found || meta.Generation != dstGen || meta.BaseRev != 7 || meta.Size != 11 {
		t.Fatalf("destination entry after abort = %+v found=%v, want its own metadata intact", meta, found)
	}
}

// TestConflictTransferAbortClearsPreparedMeta pins the empty-destination
// rollback: the prepared .meta is removed and the old name stays the single
// authoritative record.
func TestConflictTransferAbortClearsPreparedMeta(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "unlanded")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	transfer.Abort()

	if _, err := os.Stat(filepath.Join(dir, HashPath("/b")+".meta")); !os.IsNotExist(err) {
		t.Fatalf("prepared meta for the new name survived the abort: %v", err)
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Kind != PendingConflict || meta.Generation != gen {
		t.Fatalf("old-name entry after abort = %+v found=%v, want the conflict marker intact", meta, found)
	}
	if idx.HasPending("/b") {
		t.Fatal("aborted transfer left a live entry at the new name")
	}
}

// TestConflictTransferCommitMovesEntry pins the happy path plus the source
// fence: a commit built on a stale source generation is refused.
func TestConflictTransferCommitMovesEntry(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "unlanded")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	if committed, cerr := transfer.Commit(); cerr != nil || !committed {
		t.Fatalf("commit = %v committed=%v err=%v", committed, committed, cerr)
	}
	if idx.HasPending("/a") {
		t.Fatal("committed transfer left the old name behind")
	}
	meta, found := idx.GetMeta("/b")
	if !found || meta.Kind != PendingConflict {
		t.Fatalf("new-name entry after commit = %+v found=%v, want the transferred marker", meta, found)
	}
	data, rerr := shadows.ReadAll("/b")
	if rerr != nil || string(data) != "unlanded" {
		t.Fatalf("payload at the new name = %q err=%v, want the preserved bytes", data, rerr)
	}

	// A stale generation is refused outright.
	_, shadows2, _ := newConflictTransferFixture(t, t.TempDir(), "/c", "other")
	if _, ok, err := idx.BeginConflictTransfer("/b", "/d", gen, 0, shadows2); err != nil || ok {
		t.Fatalf("begin with a stale source generation = ok=%v err=%v, want a refusal", ok, err)
	}
}

// TestConflictTransferExcludesConcurrentStaging pins qiffang's race: the
// transaction holds both path locks for its lifetime, so a same-path Put
// cannot advance either endpoint between the fence and the payload move. The
// concurrent Put must therefore block until the transfer finishes.
func TestConflictTransferExcludesConcurrentStaging(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "G1")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}

	staged := make(chan uint64, 1)
	go func() {
		g, _ := idx.PutWithBaseRev("/a", 2, PendingNew, 0)
		staged <- g
	}()
	select {
	case <-staged:
		t.Fatal("a same-path publication slipped in between the fence and the commit")
	case <-time.After(150 * time.Millisecond):
		// Still blocked on the transfer's path lock, as required.
	}

	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	if committed, cerr := transfer.Commit(); cerr != nil || !committed {
		t.Fatalf("commit failed: committed=%v err=%v", committed, cerr)
	}

	// The blocked Put lands only now, on top of the committed state.
	select {
	case g := <-staged:
		if g == 0 {
			t.Fatal("concurrent Put did not produce a generation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Put never completed after the transfer")
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Generation == 0 {
		t.Fatalf("post-transfer entry at the old name = %+v found=%v, want the concurrent Put's generation", meta, found)
	}
}

// TestConflictTransferPublishesMarkerWhenPayloadMoveFails pins qiffang's other
// race: the remote name has already moved, so a failed payload move must
// still leave the destination answering for the unlanded image while the old
// name keeps the bytes.
func TestConflictTransferPublishesMarkerWhenPayloadMoveFails(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "stays behind")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	// Simulate the failed move: the shadow never leaves the old name.
	if perr := transfer.PublishDestination(); perr != nil {
		t.Fatal(perr)
	}

	if meta, found := idx.GetMeta("/b"); !found || meta.Kind != PendingConflict {
		t.Fatalf("destination entry after the failed move = %+v found=%v, want a conflict marker", meta, found)
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Kind != PendingConflict {
		t.Fatalf("old-name entry after the failed move = %+v found=%v, want the marker and bytes kept", meta, found)
	}
	if !shadows.Has("/a") {
		t.Fatal("the preserved payload did not stay at the old name")
	}
	if shadows.Has("/b") {
		t.Fatal("the payload was reported at the destination although the move failed")
	}
}

// TestConflictTransferLiveDestinationNotRepublished pins the destination
// guard on the failed-move path: when the destination carried its own entry
// and the move never replaced it, publishing the source's marker over it
// would mislabel the destination's own bytes.
func TestConflictTransferLiveDestinationNotRepublished(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, srcGen := newConflictTransferFixture(t, dir, "/a", "src")

	dstGen, err := idx.PutWithBaseRev("/b", 9, PendingNew, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/b"); err != nil {
		t.Fatal(err)
	}

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", srcGen, dstGen, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if perr := transfer.PublishDestination(); perr != nil {
		t.Fatal(perr)
	}
	meta, found := idx.GetMeta("/b")
	if !found || meta.Generation != dstGen || meta.Size != 9 || meta.BaseRev != 3 {
		t.Fatalf("destination entry = %+v found=%v, want its own metadata untouched", meta, found)
	}
}

// TestConflictTransferLiveDestinationKeepsPairAcrossCrash pins the crash
// window between a successful payload move over a LIVE destination and the
// commit: the destination's .meta must already describe the bytes that were
// moved onto it, so a restart pairs the surviving metadata with the payload
// it actually describes instead of the dead destination's (whose larger size
// would make recovery reject the surviving source bytes as too short).
func TestConflictTransferLiveDestinationKeepsPairAcrossCrash(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, srcGen := newConflictTransferFixture(t, dir, "/a", "short src")

	// A larger conflicted destination: its old metadata must not survive the
	// move to mislabel the source bytes that replace it.
	dstGen, err := idx.PutWithBaseRev("/b", 4096, PendingNew, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/b"); err != nil {
		t.Fatal(err)
	}
	if err := shadows.WriteFull("/b", make([]byte, 4096), 7); err != nil {
		t.Fatal(err)
	}
	srcMeta, found := idx.GetMeta("/a")
	if !found || srcMeta == nil {
		t.Fatal("source meta missing before the transfer")
	}

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", srcGen, dstGen, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	// Crash: no Commit, no memory publish — restart from disk only.
	transfer = nil

	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	dstMeta, found := recovered.GetMeta("/b")
	if !found || dstMeta == nil {
		t.Fatal("destination meta missing after the crash window")
	}
	if dstMeta.Kind != PendingConflict || dstMeta.Size != int64(len("short src")) || dstMeta.BaseRev != srcMeta.BaseRev {
		t.Fatalf("recovered destination meta = %+v, want the SOURCE's kind/size/baseRev paired with its moved bytes", dstMeta)
	}
	if shadows.Has("/a") {
		t.Fatal("source payload still at the old name after the move")
	}
	data, rerr := os.ReadFile(shadows.shadowPath("/b"))
	if rerr != nil || string(data) != "short src" {
		t.Fatalf("payload beside the recovered meta = %q err=%v, want the source bytes", data, rerr)
	}
}

// TestConflictTransferLiveDestinationFailedMovePublishesMarker pins the
// failed-move path over a live destination: once the move retired the dead
// destination payload and persisted the prepared .meta, a failed payload
// rename must still publish the marker at the destination — the remote name
// already moved, so silence there would let a later fsync certify bytes that
// never landed.
func TestConflictTransferLiveDestinationFailedMovePublishesMarker(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, srcGen := newConflictTransferFixture(t, dir, "/a", "stays behind")

	dstGen, err := idx.PutWithBaseRev("/b", 9, PendingNew, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/b"); err != nil {
		t.Fatal(err)
	}
	if err := shadows.WriteFull("/b", []byte("dst bytes"), 3); err != nil {
		t.Fatal(err)
	}

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", srcGen, dstGen, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	// Make the payload rename fail behind the store's back: the in-memory
	// entry (and Has) still sees the source, but os.Rename cannot find it.
	if err := os.Remove(shadows.shadowPath("/a")); err != nil {
		t.Fatal(err)
	}
	if transfer.MovePayload() {
		t.Fatal("payload move reported success with the source file gone")
	}
	if perr := transfer.PublishDestination(); perr != nil {
		t.Fatal(perr)
	}

	meta, found := idx.GetMeta("/b")
	if !found || meta.Kind != PendingConflict || meta.Size != int64(len("stays behind")) {
		t.Fatalf("destination entry after the failed move = %+v found=%v, want the published source marker", meta, found)
	}
	if _, err := os.Stat(filepath.Join(dir, HashPath("/b")+".meta")); err != nil {
		t.Fatalf("published marker is not durable: %v", err)
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Kind != PendingConflict {
		t.Fatalf("old-name entry after the failed move = %+v found=%v, want the marker kept", meta, found)
	}
}

// TestConflictTransferAbortAfterMoveCommits pins the degenerate abort: once
// the payload has moved, the durable state already describes the committed
// pairing, so aborting must finish the publication rather than strand the
// moved bytes without metadata.
func TestConflictTransferAbortAfterMoveCommits(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newConflictTransferFixture(t, dir, "/a", "unlanded")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	transfer.Abort()

	if idx.HasPending("/a") {
		t.Fatal("abort after the move left the old name published")
	}
	meta, found := idx.GetMeta("/b")
	if !found || meta.Kind != PendingConflict {
		t.Fatalf("new-name entry after the aborted transfer = %+v found=%v, want the published marker", meta, found)
	}
	if data, rerr := shadows.ReadAll("/b"); rerr != nil || string(data) != "unlanded" {
		t.Fatalf("payload at the new name = %q err=%v, want the preserved bytes", data, rerr)
	}
}

// TestConflictTransferLiveDestinationFailedRemovalKeepsPair pins the
// removal-confirmation fence: when the destination's own payload cannot be
// removed (an unwritable shadow directory), the prepared .meta must NOT be
// written beside the surviving bytes — relabeling them with the source's
// size/base revision would recreate the durable mispairing on the failure
// path. The move reports failure, the destination keeps its own metadata and
// payload, and PublishDestination leaves the live entry untouched.
func TestConflictTransferLiveDestinationFailedRemovalKeepsPair(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, srcGen := newConflictTransferFixture(t, dir, "/a", "src")

	dstGen, err := idx.PutWithBaseRev("/b", 4096, PendingNew, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkConflict("/b"); err != nil {
		t.Fatal(err)
	}
	if err := shadows.WriteFull("/b", make([]byte, 4096), 7); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, HashPath("/b")+".meta"))
	if err != nil {
		t.Fatal(err)
	}

	shadowDir := filepath.Join(dir, "shadows")
	if err := os.Chmod(shadowDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shadowDir, 0o755) })

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", srcGen, dstGen, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if transfer.MovePayload() {
		t.Fatal("move reported success although the destination payload was unremovable")
	}
	after, err := os.ReadFile(filepath.Join(dir, HashPath("/b")+".meta"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("the failed removal relabeled the destination's metadata beside the surviving bytes")
	}
	if !shadows.Has("/b") {
		t.Fatal("the destination payload vanished although its removal was not confirmed")
	}
	if perr := transfer.PublishDestination(); perr != nil {
		t.Fatal(perr)
	}
	meta, found := idx.GetMeta("/b")
	if !found || meta.Generation != dstGen || meta.Size != 4096 || meta.BaseRev != 7 {
		t.Fatalf("destination entry = %+v found=%v, want its own metadata kept", meta, found)
	}
	if meta, found := idx.GetMeta("/a"); !found || meta.Kind != PendingConflict {
		t.Fatalf("source entry = %+v found=%v, want the marker kept for the caller's failure record", meta, found)
	}
}

// newLiveTransferFixture stages a LIVE (non-conflict) queued entry at path
// with its payload — the shape writeback's rename re-target moves.
func newLiveTransferFixture(t *testing.T, dir, path, data string) (*PendingIndex, *ShadowStore, uint64) {
	t.Helper()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	shadows, err := NewShadowStoreWithQuota(filepath.Join(dir, "shadows"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	if err := shadows.WriteFull(path, []byte(data), 0); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put(path, int64(len(data)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	return idx, shadows, gen
}

// TestConflictTransferMovesLiveEntryPairAcrossCrash pins the live-image move
// (writeback re-targets a queued successor through this transaction) to the
// same crash discipline as the marker move: after the payload rename and
// before the commit, a restart must find the destination metadata paired
// with the moved bytes and the old name prunable — never metadata at one name
// and payload at the other.
func TestConflictTransferMovesLiveEntryPairAcrossCrash(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newLiveTransferFixture(t, dir, "/a", "queued successor")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	if !transfer.MovePayload() {
		t.Fatal("payload move failed")
	}
	// Crash: no Commit, no memory publish — restart from disk only.

	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	dstMeta, dstFound := recovered.GetMeta("/b")
	if dstMeta == nil || !dstFound {
		t.Fatal("destination meta missing after the crash window")
	}
	// The old name's stale .meta may linger until the commit — its payload is
	// gone, so recovery prunes it; the destination pair is what must hold.
	if dstMeta.Kind == PendingConflict {
		t.Fatalf("the live image was moved as a terminal-failure marker: %+v", dstMeta)
	}
	if dstMeta.Size != int64(len("queued successor")) {
		t.Fatalf("recovered destination meta size = %d, want the live image's", dstMeta.Size)
	}
	data, rerr := os.ReadFile(shadows.shadowPath("/b"))
	if rerr != nil || string(data) != "queued successor" {
		t.Fatalf("payload beside the recovered meta = %q err=%v, want the live image's bytes", data, rerr)
	}
	if shadows.Has("/a") {
		t.Fatal("source payload still at the old name after the move")
	}
}

// TestConflictTransferAbortKeepsLiveEntryIntact pins the failed-move rollback
// for a live entry: the prepared destination .meta is removed and the old
// name keeps its entry and payload — flipped to a preserved conflict marker
// at Begin, so recovery skips it instead of re-uploading the dead name — and
// the re-target's failure path leaves no unbacked metadata behind.
func TestConflictTransferAbortKeepsLiveEntryIntact(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newLiveTransferFixture(t, dir, "/a", "queued successor")

	transfer, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	// Make the payload rename fail behind the store's back.
	if err := os.Remove(shadows.shadowPath("/a")); err != nil {
		t.Fatal(err)
	}
	if transfer.MovePayload() {
		t.Fatal("payload move reported success with the source file gone")
	}
	transfer.Abort()

	if _, serr := os.Stat(filepath.Join(dir, HashPath("/b")+".meta")); !os.IsNotExist(serr) {
		t.Fatalf("prepared meta for the new name survived the abort: %v", serr)
	}
	meta, found := idx.GetMeta("/a")
	if !found || meta == nil || meta.Kind != PendingConflict || meta.Generation != gen {
		t.Fatalf("old-name entry after the abort = %+v found=%v, want the preserved marker at the same generation", meta, found)
	}
}

// TestConflictTransferPrepareFlipsLiveSource pins the recovery boundary
// BEFORE the payload move: a live queued image whose transfer is prepared
// must already be a durable conflict marker at the old name, so a crash
// between the prepare and the move leaves RecoverPending SKIPPING the old
// name (preserved, never re-uploaded to the dead path) while the prepared
// destination keeps the entry's live kind — the obligation it carries is a
// landing, not a terminal failure.
func TestConflictTransferPrepareFlipsLiveSource(t *testing.T) {
	dir := t.TempDir()
	idx, shadows, gen := newLiveTransferFixture(t, dir, "/a", "queued successor")

	_, ok, err := idx.BeginConflictTransfer("/a", "/b", gen, 0, shadows)
	if err != nil || !ok {
		t.Fatalf("begin = ok=%v err=%v", ok, err)
	}
	// Crash: nothing after the prepare — the transaction is never committed
	// or aborted, exactly the process-death state.

	// The old name's durable .meta is a conflict marker at the same
	// generation: recovery preserves it instead of re-driving the dead name.
	raw, rerr := os.ReadFile(filepath.Join(dir, HashPath("/a")+".meta"))
	if rerr != nil {
		t.Fatal(rerr)
	}
	var onDisk WriteBackMeta
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatal(err)
	}
	if onDisk.Kind != PendingConflict || onDisk.Generation != gen {
		t.Fatalf("old-name durable meta after prepare = kind %v gen %d, want the flipped marker at gen %d", onDisk.Kind, onDisk.Generation, gen)
	}
	// The prepared destination keeps the LIVE kind (durable only — memory
	// publishes at Commit — so read it back the way a restart would).
	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if meta, found := recovered.GetMeta("/a"); !found || meta == nil || meta.Kind != PendingConflict {
		t.Fatalf("recovered old-name entry = %+v found=%v, want the preserved marker", meta, found)
	}
	if meta, found := recovered.GetMeta("/b"); !found || meta == nil || meta.Kind == PendingConflict {
		t.Fatalf("recovered prepared entry = %+v found=%v, want the live kind preserved", meta, found)
	}
}
