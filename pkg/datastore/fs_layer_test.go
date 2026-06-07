package datastore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestFSLayerRoundTripAndState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	layer := FSLayer{
		LayerID:        "layer-1",
		BaseRootPath:   "/work",
		Name:           "agent task",
		Tags:           map[string]string{"task": "auth", "env": "dev"},
		DurabilityMode: FSLayerDurabilityRestoreSafe,
		ActorID:        "actor-1",
	}
	if err := s.CreateFSLayer(ctx, &layer); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	got, err := s.GetFSLayer(ctx, "layer-1")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	if got.BaseRootPath != "/work/" || got.Name != "agent task" || got.State != FSLayerStateActive || got.ActorID != "actor-1" {
		t.Fatalf("unexpected layer: %+v", got)
	}
	if got.Tags["task"] != "auth" || got.Tags["env"] != "dev" {
		t.Fatalf("unexpected layer tags: %+v", got.Tags)
	}
	byName, err := s.ResolveFSLayerRef(ctx, "agent task")
	if err != nil {
		t.Fatalf("ResolveFSLayerRef name: %v", err)
	}
	if byName.LayerID != "layer-1" {
		t.Fatalf("ResolveFSLayerRef name = %+v, want layer-1", byName)
	}
	byTag, err := s.ResolveFSLayerRef(ctx, "task=auth")
	if err != nil {
		t.Fatalf("ResolveFSLayerRef tag: %v", err)
	}
	if byTag.LayerID != "layer-1" {
		t.Fatalf("ResolveFSLayerRef tag = %+v, want layer-1", byTag)
	}
	byTagKey, err := s.ResolveFSLayerRef(ctx, "tag:env")
	if err != nil {
		t.Fatalf("ResolveFSLayerRef tag key: %v", err)
	}
	if byTagKey.LayerID != "layer-1" {
		t.Fatalf("ResolveFSLayerRef tag key = %+v, want layer-1", byTagKey)
	}
	layers, err := s.ListFSLayers(ctx)
	if err != nil {
		t.Fatalf("ListFSLayers: %v", err)
	}
	if len(layers) != 1 || layers[0].LayerID != "layer-1" {
		t.Fatalf("layers = %+v, want layer-1", layers)
	}
	if err := s.RollbackFSLayer(ctx, "layer-1"); err != nil {
		t.Fatalf("RollbackFSLayer: %v", err)
	}
	rolledBack, err := s.GetFSLayer(ctx, "layer-1")
	if err != nil {
		t.Fatalf("GetFSLayer after rollback: %v", err)
	}
	if rolledBack.State != FSLayerStateAbandoned || rolledBack.SealedAt == nil {
		t.Fatalf("rollback layer = %+v, want abandoned with sealed_at", rolledBack)
	}
}

func TestFSLayerRefAmbiguous(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"layer-a", "layer-b"} {
		if err := s.CreateFSLayer(ctx, &FSLayer{
			LayerID:      id,
			BaseRootPath: "/work",
			Name:         "shared",
			Tags:         map[string]string{"task": "shared"},
		}); err != nil {
			t.Fatalf("CreateFSLayer %s: %v", id, err)
		}
	}
	if _, err := s.ResolveFSLayerRef(ctx, "shared"); !errors.Is(err, ErrFSLayerRefAmbiguous) {
		t.Fatalf("ResolveFSLayerRef ambiguous name err=%v, want ErrFSLayerRefAmbiguous", err)
	}
	if _, err := s.ResolveFSLayerRef(ctx, "task=shared"); !errors.Is(err, ErrFSLayerRefAmbiguous) {
		t.Fatalf("ResolveFSLayerRef ambiguous tag err=%v, want ErrFSLayerRefAmbiguous", err)
	}
}

func TestFSLayerEntriesAndCheckpoint(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "layer-entries", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	dir := FSLayerEntry{
		LayerID: "layer-entries",
		Path:    "/repo/tmp",
		Op:      FSLayerEntryOpMkdir,
		Kind:    FSLayerEntryKindDir,
	}
	if err := s.UpsertFSLayerEntry(ctx, &dir); err != nil {
		t.Fatalf("UpsertFSLayerEntry dir: %v", err)
	}
	file := FSLayerEntry{
		LayerID:     "layer-entries",
		Path:        "/repo/tmp/a.txt",
		Op:          FSLayerEntryOpUpsert,
		Kind:        FSLayerEntryKindFile,
		ContentBlob: []byte("hello"),
		ContentType: "text/plain",
		SizeBytes:   5,
	}
	if err := s.UpsertFSLayerEntry(ctx, &file); err != nil {
		t.Fatalf("UpsertFSLayerEntry file: %v", err)
	}
	gotDir, err := s.GetFSLayerEntry(ctx, "layer-entries", "/repo/tmp/")
	if err != nil {
		t.Fatalf("GetFSLayerEntry dir: %v", err)
	}
	if gotDir.Path != "/repo/tmp/" || gotDir.Kind != FSLayerEntryKindDir || gotDir.Mode != 0o755 {
		t.Fatalf("dir entry = %+v", gotDir)
	}
	gotFile, err := s.GetFSLayerEntry(ctx, "layer-entries", "/repo/tmp/a.txt")
	if err != nil {
		t.Fatalf("GetFSLayerEntry file: %v", err)
	}
	if gotFile.EntrySeq != 2 || !bytes.Equal(gotFile.ContentBlob, []byte("hello")) || gotFile.ParentPath != "/repo/tmp/" {
		t.Fatalf("file entry = %+v", gotFile)
	}
	entries, err := s.ListFSLayerEntries(ctx, "layer-entries")
	if err != nil {
		t.Fatalf("ListFSLayerEntries: %v", err)
	}
	if len(entries) != 2 || entries[0].Path != "/repo/tmp/" || entries[1].Path != "/repo/tmp/a.txt" {
		t.Fatalf("entries = %+v", entries)
	}
	checkpoint := FSLayerCheckpoint{
		CheckpointID: "ckpt-1",
		LayerID:      "layer-entries",
		Label:        "restore point",
	}
	if err := s.CreateFSLayerCheckpoint(ctx, &checkpoint); err != nil {
		t.Fatalf("CreateFSLayerCheckpoint: %v", err)
	}
	gotCheckpoint, err := s.GetFSLayerCheckpoint(ctx, "ckpt-1")
	if err != nil {
		t.Fatalf("GetFSLayerCheckpoint: %v", err)
	}
	if gotCheckpoint.DurableSeq != 2 || gotCheckpoint.Label != "restore point" {
		t.Fatalf("checkpoint = %+v, want durable_seq=2", gotCheckpoint)
	}
	gotLayer, err := s.GetFSLayer(ctx, "layer-entries")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	if gotLayer.DurableSeq != 2 {
		t.Fatalf("layer durable_seq=%d, want 2", gotLayer.DurableSeq)
	}
}

func TestFSLayerNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.GetFSLayer(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFSLayer missing err=%v, want ErrNotFound", err)
	}
	if err := s.RollbackFSLayer(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RollbackFSLayer missing err=%v, want ErrNotFound", err)
	}
}
