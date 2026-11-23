package writeback

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestCancelQueuedForRenameReportsDurabilityFailure pins the call site, not the
// primitive: the rename marker must reach the disk before it is treated as
// written, and a marker that cannot be synced must surface as an error instead
// of a silent success.
func TestCancelQueuedForRenameReportsDurabilityFailure(t *testing.T) {
	dir := t.TempDir()
	j, err := staging.NewJournal(filepath.Join(dir, "marker.wal"))
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadow.Close() })
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, shadow, pending, j, 1, 8)

	if err := cq.CancelQueuedForRename("/renamed.txt"); err != nil {
		t.Fatalf("rename marker: %v", err)
	}
	if got := j.DurableSeq(); got < 1 {
		t.Fatalf("DurableSeq after the rename marker = %d, want >= 1 (the marker must be durable)", got)
	}

	if err := j.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}
	if err := cq.CancelQueuedForRename("/renamed-again.txt"); err == nil {
		t.Fatal("rename marker on a closed journal = nil, want an error")
	}
}

// TestOnCommitSuccessCleansUpWhenTheMarkerCannotSync pins the branch that keeps
// a durability fault from turning into a replay source: the upload already
// landed, so the local shadow/pending state must still be dropped even when the
// commit marker could not be made durable.
func TestOnCommitSuccessCleansUpWhenTheMarkerCannotSync(t *testing.T) {
	const path = "/committed.txt"
	payload := []byte("committed image")

	j, err := staging.NewJournal(filepath.Join(t.TempDir(), "marker.wal"))
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadow.Close() })
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, payload, 5); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, shadow, pending, j, 1, 8)

	// A closed journal makes every marker append fail deterministically.
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path:            path,
		BaseRev:         0,
		Size:            int64(len(payload)),
		Kind:            staging.PendingNew,
		ShadowGen:       shadowGen,
		PendingIndexGen: pendingGen,
	}
	if err := cq.onCommitSuccess(entry, 0, 1); err != nil {
		t.Fatalf("onCommitSuccess after the upload landed = %v, want nil (the fault is logged, cleanup continues)", err)
	}
	if _, ok := shadow.ContentSize(path); ok {
		t.Fatal("shadow kept after a marker that could not sync: replay would resurrect it")
	}
	if _, ok := pending.GetMeta(path); ok {
		t.Fatal("pending meta kept after a marker that could not sync: replay would re-enqueue it")
	}
}

// TestUploadSyncReportsPrunedStagedImage pins the barrier: a staged pair that
// was torn and pruned means the only copy is gone, so the synchronous upload
// must fail instead of reporting a durable close.
func TestUploadSyncReportsPrunedStagedImage(t *testing.T) {
	const path = "/torn.txt"
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("staged image")
	if err := cache.Put(path, payload, int64(len(payload)), staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	// Drop the in-memory copy so the next view load reads the .dat file, then
	// rewrite it so its checksum no longer matches the metadata binding.
	cache.DropInMemoryPayload(path)
	if err := os.WriteFile(cache.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewWriteBackUploader(nil, cache, 1)
	if _, err := u.UploadSyncWithRevision(context.Background(), path); err == nil {
		t.Fatal("UploadSyncWithRevision over a pruned staged image = nil, want an error")
	}
}

// TestUploadSyncKeepsFailingAfterThePairIsPruned pins the retry: once the only
// staged copy was pruned as damaged, a retried barrier must not read the
// resulting "nothing staged" as "already uploaded".
func TestUploadSyncKeepsFailingAfterThePairIsPruned(t *testing.T) {
	const path = "/pruned-retry.txt"
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("staged image")
	if err := cache.Put(path, payload, int64(len(payload)), staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	cache.DropInMemoryPayload(path)
	if err := os.WriteFile(cache.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewWriteBackUploader(nil, cache, 1)
	ctx := context.Background()
	if _, err := u.UploadSyncWithRevision(ctx, path); err == nil {
		t.Fatal("first UploadSyncWithRevision over a torn pair = nil, want an error")
	}
	if _, err := u.UploadSyncWithRevision(ctx, path); err == nil {
		t.Fatal("retried barrier reported success after the pair was pruned and never replaced")
	}
}
