package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/semantic"
)

func setMetaStringPtr(s string) *string { return &s }

// setMetaClaimableEmbedTask returns the claimable embed task, or nil when no
// claimable task exists. The claim timestamp is nudged forward because TiDB
// DATETIME(3) rounds available_at up to the next millisecond, which can
// otherwise land just after time.Now().
func setMetaClaimableEmbedTask(t *testing.T, b *Dat9Backend, now time.Time) *semantic.Task {
	t.Helper()
	task, found, err := b.Store().ClaimSemanticTask(context.Background(), now.Add(time.Second), time.Minute, semantic.TaskTypeEmbed)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return nil
	}
	return task
}

// A description-only setmeta must re-queue the embed task for the current
// revision even when a terminal (acked) task row for that revision already
// exists: EnqueueSemanticTask would dedupe it away, ForceRequeueSemanticTask
// re-queues it in place.
func TestSetFileMetadataRequeuesCompletedEmbedTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{AppSemanticTasksEnabled: true})
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/d.txt", []byte("body"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, "old description"); err != nil {
		t.Fatal(err)
	}

	// Simulate the worker completing the embed task for revision 1.
	task := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if task == nil || task.ResourceVersion != 1 {
		t.Fatalf("initial embed task = %+v, want queued task at revision 1", task)
	}
	if err := b.Store().AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		t.Fatal(err)
	}

	rev, err := b.SetFileMetadataCtx(ctx, "/d.txt", FileMetadataUpdate{Description: setMetaStringPtr("new description")})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("setmeta revision = %d, want unchanged 1", rev)
	}

	// The terminal row was re-queued for the same revision.
	requeued := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if requeued == nil {
		t.Fatal("embed task was not re-queued after description setmeta")
	}
	if requeued.ResourceID != task.ResourceID || requeued.ResourceVersion != 1 {
		t.Fatalf("requeued task = %+v, want same file at revision 1", requeued)
	}

	// The stale description embedding state was cleared.
	node, err := b.Store().GetNode(ctx, "/d.txt")
	if err != nil {
		t.Fatal(err)
	}
	sem, err := b.Store().GetSemantic(ctx, node.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if sem.Description != "new description" {
		t.Fatalf("description = %q, want %q", sem.Description, "new description")
	}
	if sem.DescriptionEmbeddingRevision != nil {
		t.Fatalf("description_embedding_revision = %v, want cleared", *sem.DescriptionEmbeddingRevision)
	}
}

// A tags-only setmeta must not touch the description or enqueue embed work.
func TestSetFileMetadataTagsOnlyEnqueuesNothing(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{AppSemanticTasksEnabled: true})
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/t.txt", []byte("body"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, "kept description"); err != nil {
		t.Fatal(err)
	}
	// Drain the initial embed task so any new enqueue is observable.
	task := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if task == nil {
		t.Fatal("initial embed task missing")
	}
	if err := b.Store().AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		t.Fatal(err)
	}

	if _, err := b.SetFileMetadataCtx(ctx, "/t.txt", FileMetadataUpdate{Tags: map[string]string{"owner": "alice"}}); err != nil {
		t.Fatal(err)
	}
	if task := setMetaClaimableEmbedTask(t, b, time.Now().UTC()); task != nil {
		t.Fatalf("tags-only setmeta enqueued embed task %+v", task)
	}

	node, err := b.Store().GetNode(ctx, "/t.txt")
	if err != nil {
		t.Fatal(err)
	}
	sem, err := b.Store().GetSemantic(ctx, node.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if sem.Description != "kept description" {
		t.Fatalf("description = %q, want unchanged", sem.Description)
	}
	tags, err := b.Store().GetFileTags(ctx, node.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 1 || tags["owner"] != "alice" {
		t.Fatalf("tags = %+v, want owner=alice", tags)
	}
}

// Clearing the description clears the embedding state and does not enqueue
// new embed work.
func TestSetFileMetadataClearDescriptionEnqueuesNothing(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{AppSemanticTasksEnabled: true})
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/c.txt", []byte("body"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, "doomed description"); err != nil {
		t.Fatal(err)
	}
	task := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if task == nil {
		t.Fatal("initial embed task missing")
	}
	if err := b.Store().AckSemanticTask(ctx, task.TaskID, task.Receipt); err != nil {
		t.Fatal(err)
	}

	if _, err := b.SetFileMetadataCtx(ctx, "/c.txt", FileMetadataUpdate{Description: setMetaStringPtr("")}); err != nil {
		t.Fatal(err)
	}
	if task := setMetaClaimableEmbedTask(t, b, time.Now().UTC()); task != nil {
		t.Fatalf("clear-description setmeta enqueued embed task %+v", task)
	}

	node, err := b.Store().GetNode(ctx, "/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	sem, err := b.Store().GetSemantic(ctx, node.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if sem.Description != "" || sem.DescriptionEmbeddingRevision != nil {
		t.Fatalf("semantic = %+v, want cleared description and embedding state", sem)
	}
}

// A description setmeta while the embed task is being processed must
// invalidate the in-flight lease and leave the task queued again, so the new
// description is embedded even when the old worker already wrote its vector
// and is about to ack.
func TestSetFileMetadataForceRequeuesInFlightEmbedTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{AppSemanticTasksEnabled: true})
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/inflight.txt", []byte("body"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, "old description"); err != nil {
		t.Fatal(err)
	}
	task := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if task == nil {
		t.Fatal("initial embed task missing")
	}
	// The task is now processing under an active lease (claim held).

	if _, err := b.SetFileMetadataCtx(ctx, "/inflight.txt", FileMetadataUpdate{Description: setMetaStringPtr("new description")}); err != nil {
		t.Fatal(err)
	}

	// The in-flight owner's lease is invalidated: ack with the old receipt
	// must not complete the row.
	if err := b.Store().AckSemanticTask(ctx, task.TaskID, task.Receipt); err == nil {
		t.Fatal("ack with invalidated receipt succeeded, want error")
	}
	// The row is claimable again for the same revision.
	requeued := setMetaClaimableEmbedTask(t, b, time.Now().UTC())
	if requeued == nil {
		t.Fatal("in-flight embed task was not re-queued after description setmeta")
	}
	if requeued.ResourceVersion != 1 || requeued.Receipt == task.Receipt {
		t.Fatalf("requeued task = %+v, want revision 1 under a fresh lease", requeued)
	}
}

func TestSetFileMetadataErrors(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/f.txt", []byte("body"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetFileMetadataCtx(ctx, "/missing.txt", FileMetadataUpdate{Tags: map[string]string{"a": "b"}}); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("missing file err = %v, want ErrNotFound", err)
	}
	if _, err := b.SetFileMetadataCtx(ctx, "/", FileMetadataUpdate{Tags: map[string]string{"a": "b"}}); !errors.Is(err, datastore.ErrInvalidRootDentry) {
		t.Fatalf("root err = %v, want ErrInvalidRootDentry", err)
	}
	if err := b.Store().InsertNode(ctx, &datastore.FileNode{
		NodeID: "n-dir", Path: "/dir", ParentPath: "/", Name: "dir", IsDirectory: true, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetFileMetadataCtx(ctx, "/dir", FileMetadataUpdate{Tags: map[string]string{"a": "b"}}); !errors.Is(err, ErrSetMetadataOnDirectory) {
		t.Fatalf("directory err = %v, want ErrSetMetadataOnDirectory", err)
	}
}
