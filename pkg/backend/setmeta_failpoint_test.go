//go:build failpoint

package backend

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/pkg/datastore"
)

// TestSetFileMetadataRejectsDeleteRecreateRace pins the dentry fence: when the
// path is deleted and recreated between path→inode resolution and the
// metadata transaction, setmeta must fail with ErrRevisionConflict and must
// not mutate the replacement inode.
func TestSetFileMetadataRejectsDeleteRecreateRace(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{AppSemanticTasksEnabled: true})
	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/race.txt", []byte("old"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1,
		map[string]string{"owner": "old"}, "old description"); err != nil {
		t.Fatal(err)
	}
	oldNode, err := b.Store().GetNode(ctx, "/race.txt")
	if err != nil {
		t.Fatal(err)
	}

	var fired atomic.Bool
	if err := failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/backend/setmetaAfterResolve", func(path string) {
		if path != "/race.txt" || !fired.CompareAndSwap(false, true) {
			return
		}
		if _, err := b.Store().DeleteFileWithRefCheck(ctx, "/race.txt"); err != nil {
			t.Errorf("race delete: %v", err)
			return
		}
		now := time.Now().UTC()
		if err := b.Store().InsertFile(ctx, &datastore.File{
			FileID: "f-race-new", StorageType: datastore.StorageDB9, StorageRef: "inline",
			Revision: 1, Status: datastore.StatusConfirmed, Description: "replacement description",
			CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Errorf("race recreate file: %v", err)
			return
		}
		if err := b.Store().InsertNode(ctx, &datastore.FileNode{
			NodeID: "n-race-new", Path: "/race.txt", ParentPath: "/", Name: "race.txt",
			FileID: "f-race-new", CreatedAt: now,
		}); err != nil {
			t.Errorf("race recreate node: %v", err)
		}
	}); err != nil {
		t.Fatalf("enable failpoint: %v", err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/backend/setmetaAfterResolve")
	})

	_, err = b.SetFileMetadataCtx(ctx, "/race.txt", FileMetadataUpdate{
		Tags:        map[string]string{"owner": "attacker"},
		Description: setMetaStringPtr("attacker description"),
	})
	if !errors.Is(err, datastore.ErrRevisionConflict) {
		t.Fatalf("setmeta after delete+recreate err = %v, want ErrRevisionConflict", err)
	}
	if !fired.Load() {
		t.Fatal("failpoint did not fire")
	}

	// The replacement inode is untouched: no user tags, original description.
	tags, err := b.Store().GetFileTags(ctx, "f-race-new")
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 0 {
		t.Fatalf("replacement inode tags = %+v, want none", tags)
	}
	sem, err := b.Store().GetSemantic(ctx, "f-race-new")
	if err != nil {
		t.Fatal(err)
	}
	if sem.Description != "replacement description" {
		t.Fatalf("replacement description = %q, want unchanged", sem.Description)
	}

	// The old (deleted) inode was not tagged either.
	tags, err = b.Store().GetFileTags(ctx, oldNode.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 0 {
		t.Fatalf("old inode tags = %+v, want none", tags)
	}

	// A retry against the live dentry succeeds on the replacement inode.
	if _, err := b.SetFileMetadataCtx(ctx, "/race.txt", FileMetadataUpdate{
		Tags: map[string]string{"owner": "alice"},
	}); err != nil {
		t.Fatalf("setmeta retry: %v", err)
	}
	tags, err = b.Store().GetFileTags(ctx, "f-race-new")
	if err != nil {
		t.Fatal(err)
	}
	if tags["owner"] != "alice" {
		t.Fatalf("replacement inode tags after retry = %+v, want owner=alice", tags)
	}
}
