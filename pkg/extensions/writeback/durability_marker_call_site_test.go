package writeback

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
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
	base := client.New("https://fixture.invalid", "")
	view, err := base.WithView(client.ViewBinding{LayerID: "layer_01j8z4r9w3m5n7p9q2s4t6v8xz", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("main", func(t *testing.T) { checkCommitCleanupAfterMarkerFailure(t, base) })
	t.Run("layer", func(t *testing.T) { checkCommitCleanupAfterMarkerFailure(t, view) })
}

func checkCommitCleanupAfterMarkerFailure(t *testing.T, c *client.Client) {
	t.Helper()
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
	cq := NewCommitQueue(c, shadow, pending, j, 1, 8)

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
// must fail — and the cache must have recorded the loss (under its path lock,
// before the read returns) where a retried barrier can still see it.
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
	// The cache is the single recorder: mirror what Writeback wires.
	recorded := make(map[string]error)
	cache.OnViewLost = func(p string, err error) {
		recorded[p] = fmt.Errorf("%w: %w", errStagedViewLost, err)
	}
	// Drop the in-memory copy so the next view load reads the .dat file, then
	// rewrite it so its checksum no longer matches the metadata binding.
	cache.DropInMemoryPayload(path)
	if err := os.WriteFile(cache.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewWriteBackUploader(nil, cache, 1)
	u.TerminalFailureForPath = func(p string) error { return recorded[p] }
	ctx := context.Background()
	if _, err := u.UploadSyncWithRevision(ctx, path); err == nil {
		t.Fatal("first UploadSyncWithRevision over a torn pair = nil, want an error")
	}
	if _, ok := recorded[path]; !ok {
		t.Fatalf("the prune was not recorded: %+v", recorded)
	}
	// The retry finds nothing staged, but the recorded loss must keep the
	// barrier failing instead of reading the prune as "already uploaded".
	if _, err := u.UploadSyncWithRevision(ctx, path); err == nil {
		t.Fatal("retried barrier reported success after the pair was pruned and never replaced")
	}
}

// TestRetainedShadowIsFencedAsCommitted pins the retained-shadow branch of the
// commit-marker failure path: when the marker cannot be made durable and this
// landing deliberately kept the shadow, the path gets a durable landed fence
// and its pending entry is retired. Recovery skips the landed image instead of
// re-uploading it from the surviving shadow, every live reader sees the path as
// clean, and the fence is not a failure — the path's barriers keep reporting
// success.
func TestRetainedShadowIsFencedAsCommitted(t *testing.T) {
	const path = "/retained.txt"
	payload := []byte("retained image")

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
	pending.SetJournal(j)
	cq := NewCommitQueue(nil, shadow, pending, j, 1, 8)
	// The staging handle kept newer bytes for this landing.
	cq.retainedShadows.Store(path, shadowGen)
	// A previous append gives the fence a real sequence to capture, then a
	// closed journal makes the commit marker non-durable deterministically.
	if err := j.AppendSync(staging.JournalEntry{Op: staging.JournalFsync, Path: path, Length: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	seqAtFence := j.CurrentSeq()
	if seqAtFence == 0 {
		t.Fatal("expected a non-zero journal sequence before the fence")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path:            path,
		Size:            int64(len(payload)),
		Kind:            staging.PendingNew,
		ShadowGen:       shadowGen,
		PendingIndexGen: pendingGen,
	}
	if err := cq.onCommitSuccess(entry, 0, 1); err != nil {
		t.Fatalf("onCommitSuccess = %v, want nil (the fault is logged, the landing is fenced)", err)
	}

	// The landed image is not pending work: every live reader must see a clean
	// path, or rename/unlink/overlay paths would treat it as an upload to
	// settle.
	if meta, ok := pending.GetMeta(path); ok {
		t.Fatalf("pending entry survived the fence: %+v", meta)
	}
	if pending.HasPending(path) {
		t.Fatal("HasPending reports the landed image as pending work")
	}
	if n := pending.Count(); n != 0 {
		t.Fatalf("pending count = %d after the fence, want 0", n)
	}
	// The fence itself must be durable and generation-scoped, and it must not
	// read as a terminal failure: the upload landed.
	fence, ok := pending.CommittedFence(path)
	if !ok {
		t.Fatal("no landed fence was recorded for the path")
	}
	if fence.Generation != pendingGen || fence.ShadowGen != shadowGen {
		t.Fatalf("fence = %+v, want generation %d and shadow %d", fence, pendingGen, shadowGen)
	}
	// The fence must cover every frame appended before it. It is allowed to be
	// higher: the failed commit-marker append still consumed a sequence.
	if fence.JournalSeq < seqAtFence {
		t.Fatalf("fence journal seq = %d, want at least %d (the frames it masks)", fence.JournalSeq, seqAtFence)
	}
	if err := cq.PathTerminalFailure(path); err != nil {
		t.Fatalf("PathTerminalFailure after a landed commit = %v, want nil", err)
	}
}

// TestSyncBarrierFailsAfterARestartLostThePair pins the write-sync case (no
// commit queue): the loss record is the cache's durable marker, so a barrier in
// a NEW process still fails after a plain read pruned the only copy.
func TestSyncBarrierFailsAfterARestartLostThePair(t *testing.T) {
	dir := t.TempDir()
	const path = "/restart-torn.txt"
	cache, err := staging.NewWriteBackCache(dir)
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
	// A plain read prunes the pair; the cache keeps the durable marker.
	if _, ok := cache.Get(path); ok {
		t.Fatal("Get served a torn pair")
	}

	// A fresh process: new cache over the same directory, no commit queue.
	restarted, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = restarted.ListPending()
	u := NewWriteBackUploader(nil, restarted, 1)
	u.TerminalFailureForPath = restarted.ViewLoss
	if _, err := u.UploadSyncWithRevision(context.Background(), path); err == nil {
		t.Fatal("barrier reported success after a restart lost the staged pair")
	}
	// A fresh staging supersedes the record, so the same barrier succeeds once
	// there is content again.
	if err := restarted.Put(path, payload, int64(len(payload)), staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ViewLoss(path); err != nil {
		t.Fatalf("a fresh staging left the loss record: %v", err)
	}
}

// TestCancelPrefixRemovesShadowsWhenTheMarkerCannotSync pins the subtree
// fallback: when the cancel marker cannot be made durable (no journal, or a
// closed one), the shadow removal itself must be the authority, so a queued
// entry's staged image really goes instead of surviving as a replay source.
func TestCancelPrefixRemovesShadowsWhenTheMarkerCannotSync(t *testing.T) {
	for _, withJournal := range []bool{false, true} {
		t.Run(map[bool]string{false: "no journal", true: "closed journal"}[withJournal], func(t *testing.T) {
			shadow, err := staging.NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { shadow.Close() })
			pending, err := staging.NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var j *staging.Journal
			if withJournal {
				j, err = staging.NewJournal(filepath.Join(t.TempDir(), "marker.wal"))
				if err != nil {
					t.Fatal(err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cq := NewCommitQueue(nil, shadow, pending, j, 1, 8)

			const path = "/tree/a.bin"
			if err := shadow.WriteFull(path, []byte("staged"), 3); err != nil {
				t.Fatal(err)
			}
			gen, err := pending.Put(path, 6, staging.PendingNew)
			if err != nil {
				t.Fatal(err)
			}
			entry := &CommitEntry{
				Path:            path,
				Size:            6,
				Kind:            staging.PendingNew,
				ShadowGen:       shadow.ActiveGeneration(path),
				PendingIndexGen: gen,
			}
			// Park the entry in the queue so the subtree cancel has something to
			// fence (a worker is not running with a nil client).
			cq.mu.Lock()
			cq.queue = append(cq.queue, entry)
			cq.mu.Unlock()

			cq.CancelPrefix("/tree/")
			if err := shadow.VerifyRemoved(path); err != nil {
				t.Fatalf("shadow survived the subtree cancel: %v", err)
			}
			if _, ok := pending.GetMeta(path); ok {
				t.Fatal("the cancelled entry's pending metadata survived")
			}
		})
	}
}
