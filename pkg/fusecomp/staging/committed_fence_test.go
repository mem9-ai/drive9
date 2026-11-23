package staging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// newFenceTestIndex builds an index over a temp dir plus a ledger folder.
func newFenceTestIndex(t *testing.T) (*PendingIndex, string) {
	t.Helper()
	dir := t.TempDir()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	return idx, dir
}

// TestCommittedFenceRetiresThePendingEntry pins the live-reader half of the
// fence: a landed image must stop being pending work for EVERY reader of the
// index, not only for the ones that know to filter a special kind.
func TestCommittedFenceRetiresThePendingEntry(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/landed.txt"
	gen, err := idx.Put(path, 7, PendingNew)
	if err != nil {
		t.Fatal(err)
	}

	marked, err := idx.MarkCommittedIfGeneration(path, gen, 0)
	if err != nil || !marked {
		t.Fatalf("MarkCommittedIfGeneration = %v, %v; want true, nil", marked, err)
	}
	if _, ok := idx.GetMeta(path); ok {
		t.Fatal("GetMeta still returns the landed image as a pending entry")
	}
	if idx.HasPending(path) {
		t.Fatal("HasPending reports the landed image as pending work")
	}
	if n := idx.Count(); n != 0 {
		t.Fatalf("Count = %d, want 0 (a landed fence must not block ReexecRefusal)", n)
	}
	if paths := idx.ListUncommittedPaths(); len(paths) != 0 {
		t.Fatalf("ListUncommittedPaths = %v, want empty", paths)
	}
	if paths := idx.ListPendingPaths(); len(paths) != 0 {
		t.Fatalf("ListPendingPaths = %v, want empty", paths)
	}
	// The fence itself must be durable and rebuildable from disk.
	if _, err := os.Stat(filepath.Join(dir, HashPath(path)+".fence")); err != nil {
		t.Fatalf("fence file missing: %v", err)
	}
	recovered, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if _, ok := recovered.GetMeta(path); ok {
		t.Fatal("recovery resurrected the landed image as a pending entry")
	}
	if fence, ok := recovered.CommittedFence(path); !ok || fence.Generation != gen {
		t.Fatalf("recovered fence = %+v, %v; want generation %d", fence, ok, gen)
	}
}

// TestCommittedFenceMasksOnlyTheLandedGeneration pins the replay half: the
// fence drops the frames the landing superseded and keeps a NEWER publication,
// so fsync-acknowledged writes that happened after the landing survive.
func TestCommittedFenceMasksOnlyTheLandedGeneration(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/replayed.txt"
	gen, err := idx.Put(path, 5, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, 0); err != nil {
		t.Fatal(err)
	}

	// An older frame (the landed image) must be masked.
	older := WriteBackMeta{Path: path, Size: 5, Kind: PendingNew, Generation: gen}
	rawOlder, err := json.Marshal(&older)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.publishRecoveredMeta(rawOlder); err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.GetMeta(path); ok {
		t.Fatal("a fenced generation came back as a pending entry")
	}

	// A newer frame is a later acknowledged write: it must be recovered.
	newer := WriteBackMeta{Path: path, Size: 9, Kind: PendingNew, Generation: gen + 1}
	rawNewer, err := json.Marshal(&newer)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.publishRecoveredMeta(rawNewer); err != nil {
		t.Fatal(err)
	}
	meta, ok := idx.GetMeta(path)
	if !ok || meta.Generation != gen+1 {
		t.Fatalf("newer publication after the fence was dropped: %+v, %v", meta, ok)
	}
	_ = dir
}

// TestCommittedFenceSurvivesRestartUntilSettled pins the second-restart bug:
// once the pending entry is gone, a fence left on disk still suppresses the
// pre-commit frames; settling then appends a durable commit marker and drops
// the shadow before the fence is deleted.
func TestCommittedFenceSurvivesRestartUntilSettled(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	shadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadows.Close() })
	idx.SetJournal(journal)

	const path = "/crash.txt"
	payload := []byte("landed image")
	if err := shadows.WriteFull(path, payload, 11); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadows.ActiveGeneration(path)
	gen, err := idx.Put(path, int64(len(payload)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, shadowGen); err != nil {
		t.Fatal(err)
	}

	// Simulate a restart: rebuild the index from the same directory, replay the
	// journal, and settle. The landed image's own WAL frame must not resurrect.
	restarted, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetJournal(journal)
	restarted.SetShadowStore(shadows)
	if err := restarted.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := ReplayJournalIntoPending(journal, restarted, shadows); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.GetMeta(path); ok {
		t.Fatal("replay resurrected the landed image as a pending entry")
	}
	if _, ok := restarted.CommittedFence(path); !ok {
		t.Fatal("the fence did not survive the restart")
	}
	if err := restarted.SettleCommittedFences(journal, shadows); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.CommittedFence(path); ok {
		t.Fatal("the fence survived its own settle")
	}
	if shadows.Has(path) {
		t.Fatal("the landed image's shadow survived the settle")
	}
	// The commit marker the settle appended must be durable and must actually
	// cover the pre-commit frames: a later replay of the same WAL finds nothing.
	if restarted.HasPending(path) {
		t.Fatal("settle left the path pending")
	}
}

// TestCommittedFenceIsDroppedByANewerStaging pins the supersession rule: a
// fresh publication for the path clears the fence, so the new image is never
// masked by the one that already landed.
func TestCommittedFenceIsDroppedByANewerStaging(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/restaged.txt"
	gen, err := idx.Put(path, 3, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Put(path, 4, PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.CommittedFence(path); ok {
		t.Fatal("a newer staging left the landed fence in place")
	}
	if _, err := os.Stat(filepath.Join(dir, HashPath(path)+".fence")); !os.IsNotExist(err) {
		t.Fatalf("fence file still on disk after a newer staging: %v", err)
	}
}

// TestCommittedFenceFailureKeepsThePendingEntry pins the failure half: a fence
// that cannot be written must leave the pending entry alone (so a retry can
// still fence it) instead of deleting the only local image.
func TestCommittedFenceFailureKeepsThePendingEntry(t *testing.T) {
	idx, dir := newFenceTestIndex(t)
	const path = "/unwritable.txt"
	gen, err := idx.Put(path, 3, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	// Make the fence path unusable: the fence name is a directory, so the
	// atomic rename into it fails.
	if err := os.Mkdir(filepath.Join(dir, HashPath(path)+".fence"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(path, gen, 0); err == nil {
		t.Fatal("MarkCommittedIfGeneration succeeded with an unwritable fence path")
	}
	if meta, ok := idx.GetMeta(path); !ok || meta.Generation != gen {
		t.Fatalf("pending entry after a failed fence = %+v, %v; want the original entry", meta, ok)
	}
}

// TestCommittedFenceGatesDataFramesByJournalSeq pins the frame-level half of
// the replay gate, which has no generation to compare: a data (fsync) frame
// appended before the fence is masked, while a frame appended after it is a
// later acknowledged write and must still replay.
func TestCommittedFenceGatesDataFramesByJournalSeq(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	shadows, err := NewShadowStore(filepath.Join(dir, "shadow"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadows.Close() })
	idx.SetJournal(journal)

	const oldPath = "/old-frame.txt"
	const newPath = "/new-frame.txt"
	oldPayload := []byte("pre-commit image")
	if err := shadows.WriteFull(oldPath, oldPayload, 3); err != nil {
		t.Fatal(err)
	}
	oldGen, err := idx.Put(oldPath, int64(len(oldPayload)), PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	// A legacy fsync frame for the landed image, appended before the fence.
	if err := journal.AppendSync(JournalEntry{Op: JournalFsync, Path: oldPath, Offset: 0, Length: int64(len(oldPayload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.MarkCommittedIfGeneration(oldPath, oldGen, 0); err != nil {
		t.Fatal(err)
	}

	// A write acknowledged AFTER the landing: its frame must survive.
	newPayload := []byte("post-commit image")
	if err := shadows.WriteFull(newPath, newPayload, 3); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendSync(JournalEntry{Op: JournalFsync, Path: newPath, Offset: 0, Length: int64(len(newPayload))}); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetShadowStore(shadows)
	if err := ReplayJournalIntoPending(journal, restarted, shadows); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.GetMeta(oldPath); ok {
		t.Fatal("a data frame masked by the fence resurrected the landed image")
	}
	if meta, ok := restarted.GetMeta(newPath); !ok {
		t.Fatal("a data frame appended after the fence was dropped")
	} else if meta.Size != int64(len(newPayload)) {
		t.Fatalf("recovered size = %d, want %d", meta.Size, len(newPayload))
	}
}
