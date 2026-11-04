// Crash-safety and concurrency rows for the generation-fenced conflict-marker
// transfer used by writeback's rename: the transfer must keep metadata and
// payload paired throughout, must not clobber a destination that already has
// its own entry, and must exclude same-path publications for its whole
// lifetime.

package staging

import (
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
// guard on the failed-move path: when the destination carried its own entry,
// publishing the source's marker over it would mislabel the destination's
// own bytes.
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
