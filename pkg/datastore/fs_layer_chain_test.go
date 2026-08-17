package datastore

import (
	"context"
	"errors"
	"testing"
)

func TestForkFSLayerTipPinSeesUncheckpointedEntries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "parent-tip", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID:     "parent-tip",
		Path:        "/repo/a.txt",
		Op:          FSLayerEntryOpUpsert,
		Kind:        FSLayerEntryKindFile,
		ContentBlob: []byte("hello"),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}
	// No checkpoint: durable_seq stays 0; tip pin must still see entry_seq=1.
	child, err := s.ForkFSLayer(ctx, FSLayerForkOptions{
		ChildLayerID:  "child-tip",
		ParentLayerID: "parent-tip",
		Name:          "child",
	})
	if err != nil {
		t.Fatalf("ForkFSLayer: %v", err)
	}
	if child.ParentLayerID != "parent-tip" {
		t.Fatalf("ParentLayerID=%q", child.ParentLayerID)
	}
	if child.OriginSeq != 1 {
		t.Fatalf("OriginSeq=%d, want 1 (MAX entry_seq)", child.OriginSeq)
	}
	if child.Depth != 1 {
		t.Fatalf("Depth=%d, want 1", child.Depth)
	}
	if child.RootLayerID != "parent-tip" {
		t.Fatalf("RootLayerID=%q, want parent-tip", child.RootLayerID)
	}
	entry, hit, err := s.ResolveFSLayerPath(ctx, "child-tip", "/repo/a.txt")
	if err != nil {
		t.Fatalf("ResolveFSLayerPath: %v", err)
	}
	if !hit || entry == nil || string(entry.ContentBlob) != "hello" {
		t.Fatalf("child should see parent tip entry, hit=%v entry=%+v", hit, entry)
	}
}

func TestForkFSLayerCheckpointPin(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "parent-cp", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "parent-cp", Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("v1"),
	}); err != nil {
		t.Fatalf("upsert1: %v", err)
	}
	cp := FSLayerCheckpoint{CheckpointID: "cp-1", LayerID: "parent-cp", Label: "v1"}
	if err := s.CreateFSLayerCheckpoint(ctx, &cp); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "parent-cp", Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("v2"),
	}); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	child, err := s.ForkFSLayer(ctx, FSLayerForkOptions{
		ChildLayerID:  "child-cp",
		ParentLayerID: "parent-cp",
		CheckpointID:  "cp-1",
	})
	if err != nil {
		t.Fatalf("ForkFSLayer: %v", err)
	}
	if child.OriginSeq != 1 {
		t.Fatalf("OriginSeq=%d, want 1 (checkpoint durable_seq)", child.OriginSeq)
	}
	entry, hit, err := s.ResolveFSLayerPath(ctx, "child-cp", "/repo/a.txt")
	if err != nil || !hit {
		t.Fatalf("resolve: hit=%v err=%v", hit, err)
	}
	if string(entry.ContentBlob) != "v1" {
		t.Fatalf("content=%q, want v1 (checkpoint pin)", entry.ContentBlob)
	}
}

func TestForkHidesParentWritesAfterPin(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-hide", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "p-hide", Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("before"),
	}); err != nil {
		t.Fatalf("upsert before: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "c-hide", ParentLayerID: "p-hide"}); err != nil {
		t.Fatalf("fork: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "p-hide", Path: "/repo/b.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("after"),
	}); err != nil {
		t.Fatalf("upsert after: %v", err)
	}
	_, hit, err := s.ResolveFSLayerPath(ctx, "c-hide", "/repo/b.txt")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if hit {
		t.Fatal("child must not see parent writes after fork pin")
	}
}

func TestDeleteFSLayerHasChildrenGate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-del", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "c-del", ParentLayerID: "p-del"}); err != nil {
		t.Fatalf("fork: %v", err)
	}
	err := s.DeleteFSLayer(ctx, "p-del", DeleteFSLayerOptions{})
	if !errors.Is(err, ErrFSLayerHasChildren) {
		t.Fatalf("delete err=%v, want ErrFSLayerHasChildren", err)
	}
	if err := s.DeleteFSLayer(ctx, "p-del", DeleteFSLayerOptions{Cascade: true}); err != nil {
		t.Fatalf("cascade delete: %v", err)
	}
	parent, err := s.GetFSLayer(ctx, "p-del")
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if parent.State != FSLayerStateAbandoned {
		t.Fatalf("parent state=%s, want abandoned", parent.State)
	}
	child, err := s.GetFSLayer(ctx, "c-del")
	if err != nil {
		t.Fatalf("get child: %v", err)
	}
	if child.State != FSLayerStateAbandoned {
		t.Fatalf("child state=%s, want abandoned", child.State)
	}
}

func TestStillPinsAbandonedMiddleWithActiveGrandchild(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "root-pin", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "mid-pin", ParentLayerID: "root-pin"}); err != nil {
		t.Fatalf("fork mid: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "leaf-pin", ParentLayerID: "mid-pin"}); err != nil {
		t.Fatalf("fork leaf: %v", err)
	}
	if err := s.RollbackFSLayer(ctx, "mid-pin"); err != nil {
		t.Fatalf("abandon mid: %v", err)
	}
	pins, err := s.FSLayerStillPins(ctx, "root-pin")
	if err != nil {
		t.Fatalf("still pins: %v", err)
	}
	if !pins {
		t.Fatal("root should still pin via active grandchild")
	}
	err = s.DeleteFSLayer(ctx, "root-pin", DeleteFSLayerOptions{})
	if !errors.Is(err, ErrFSLayerHasChildren) {
		t.Fatalf("delete root err=%v, want ErrFSLayerHasChildren", err)
	}
}

func TestListFSLayerChainAndMerged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "root-m", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "root-m", Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("root"),
	}); err != nil {
		t.Fatalf("upsert root: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "child-m", ParentLayerID: "root-m"}); err != nil {
		t.Fatalf("fork: %v", err)
	}
	if err := s.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "child-m", Path: "/repo/b.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("child"),
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}
	chain, err := s.ListFSLayerChain(ctx, "child-m")
	if err != nil {
		t.Fatalf("chain: %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("chain len=%d, want 2", len(chain))
	}
	if chain[0].Layer.LayerID != "root-m" || chain[1].Layer.LayerID != "child-m" {
		t.Fatalf("chain order wrong: %+v", chain)
	}
	merged, err := s.ListFSLayerChainMergedEntries(ctx, "child-m")
	if err != nil {
		t.Fatalf("merged: %v", err)
	}
	if len(merged) != 2 {
		t.Fatalf("merged len=%d, want 2", len(merged))
	}
}

func TestForkRejectsCheckpointFromOtherLayer(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-cp-a", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-cp-b", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create b: %v", err)
	}
	if err := s.CreateFSLayerCheckpoint(ctx, &FSLayerCheckpoint{CheckpointID: "cp-b", LayerID: "p-cp-b"}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	_, err := s.ForkFSLayer(ctx, FSLayerForkOptions{
		ChildLayerID:  "c-cp-mismatch",
		ParentLayerID: "p-cp-a",
		CheckpointID:  "cp-b",
	})
	if !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("err=%v, want ErrFSLayerStateConflict", err)
	}
}

func TestDeleteFSLayerRejectsCommitting(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-commit-del", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.BeginFSLayerCommit(ctx, "p-commit-del"); err != nil {
		t.Fatalf("begin commit: %v", err)
	}
	err := s.DeleteFSLayer(ctx, "p-commit-del", DeleteFSLayerOptions{})
	if !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("delete err=%v, want ErrFSLayerStateConflict", err)
	}
}

func TestDeleteFSLayerCascadeThroughAbandonedMiddle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "root-cas", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "mid-cas", ParentLayerID: "root-cas"}); err != nil {
		t.Fatalf("fork mid: %v", err)
	}
	if _, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "leaf-cas", ParentLayerID: "mid-cas"}); err != nil {
		t.Fatalf("fork leaf: %v", err)
	}
	if err := s.RollbackFSLayer(ctx, "mid-cas"); err != nil {
		t.Fatalf("abandon mid: %v", err)
	}
	if err := s.DeleteFSLayer(ctx, "root-cas", DeleteFSLayerOptions{Cascade: true}); err != nil {
		t.Fatalf("cascade delete: %v", err)
	}
	for _, id := range []string{"root-cas", "mid-cas", "leaf-cas"} {
		got, err := s.GetFSLayer(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got.State != FSLayerStateAbandoned {
			t.Fatalf("%s state=%s, want abandoned", id, got.State)
		}
	}
}

func TestForkRejectsCommittedParent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-committed", BaseRootPath: "/repo", State: FSLayerStateCommitted}); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := s.ForkFSLayer(ctx, FSLayerForkOptions{ChildLayerID: "c-committed", ParentLayerID: "p-committed"})
	if !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("err=%v, want ErrFSLayerStateConflict", err)
	}
}
