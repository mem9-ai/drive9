package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/datastore"
)

func TestFSLayerAPIFlow(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	layer, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:        "layer-api",
		BaseRootPath:   "/repo",
		Name:           "api layer",
		Tags:           map[string]string{"task": "api", "env": "test"},
		DurabilityMode: "restore-safe",
		ActorID:        "actor-api",
	})
	if err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if layer.LayerID != "layer-api" || layer.BaseRootPath != "/repo/" || layer.Name != "api layer" || layer.State != "active" {
		t.Fatalf("created layer = %+v", layer)
	}
	if layer.Tags["task"] != "api" || layer.Tags["env"] != "test" {
		t.Fatalf("created layer tags = %+v", layer.Tags)
	}

	if _, err := c.GetFSLayer(ctx, "api layer"); err != nil {
		t.Fatalf("GetFSLayer by name: %v", err)
	}
	if _, err := c.GetFSLayer(ctx, "tag:task=api"); err != nil {
		t.Fatalf("GetFSLayer by tag: %v", err)
	}

	entry, err := c.UpsertFSLayerEntry(ctx, "api layer", client.FSLayerEntryRequest{
		Path:    "/repo/new.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("layered"),
		Mode:    0o644,
	})
	if err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}
	if entry.LayerID != "layer-api" || entry.Path != "/repo/new.txt" || entry.SizeBytes != 7 {
		t.Fatalf("entry = %+v", entry)
	}
	gotEntry, err := c.GetFSLayerEntry(ctx, "task=api", "/repo/new.txt")
	if err != nil {
		t.Fatalf("GetFSLayerEntry: %v", err)
	}
	if !bytes.Equal(gotEntry.Content, []byte("layered")) {
		t.Fatalf("entry content = %q, want layered", gotEntry.Content)
	}
	entries, err := c.DiffFSLayer(ctx, "task=api")
	if err != nil {
		t.Fatalf("DiffFSLayer: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "/repo/new.txt" || entries[0].Op != "upsert" {
		t.Fatalf("entries = %+v", entries)
	}

	checkpoint, err := c.CheckpointFSLayer(ctx, "task=api", client.FSLayerCheckpointRequest{
		CheckpointID: "ckpt-api",
		Label:        "restore point",
	})
	if err != nil {
		t.Fatalf("CheckpointFSLayer: %v", err)
	}
	if checkpoint.CheckpointID != "ckpt-api" || checkpoint.DurableSeq != 1 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
	gotCheckpoint, err := c.GetFSLayerCheckpoint(ctx, "ckpt-api")
	if err != nil {
		t.Fatalf("GetFSLayerCheckpoint: %v", err)
	}
	if gotCheckpoint.LayerID != "layer-api" || gotCheckpoint.DurableSeq != 1 {
		t.Fatalf("got checkpoint = %+v", gotCheckpoint)
	}

	layers, err := c.ListFSLayers(ctx)
	if err != nil {
		t.Fatalf("ListFSLayers: %v", err)
	}
	if len(layers) != 1 || layers[0].LayerID != "layer-api" {
		t.Fatalf("layers = %+v", layers)
	}

	commit, err := c.CommitFSLayer(ctx, "tag:task=api")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.LayerID != "layer-api" || commit.Applied != 1 {
		t.Fatalf("commit = %+v", commit)
	}
	data, err := s.fallback.ReadCtx(ctx, "/repo/new.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx committed file: %v", err)
	}
	if !bytes.Equal(data, []byte("layered")) {
		t.Fatalf("committed data = %q, want layered", data)
	}

	rollbackLayer, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-rollback",
		BaseRootPath: "/repo",
		Name:         "rollback layer",
		Tags:         map[string]string{"task": "rollback"},
	})
	if err != nil {
		t.Fatalf("CreateFSLayer rollback: %v", err)
	}
	if rollbackLayer.LayerID != "layer-rollback" {
		t.Fatalf("rollback layer = %+v", rollbackLayer)
	}
	if err := c.RollbackFSLayer(ctx, "task=rollback"); err != nil {
		t.Fatalf("RollbackFSLayer: %v", err)
	}
	got, err := c.GetFSLayer(ctx, "layer-rollback")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	if got.State != "abandoned" {
		t.Fatalf("layer state=%s, want abandoned", got.State)
	}
}

func TestFSLayerCommitWhiteoutDirectoryRemovesTree(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/repo/delete-dir/gone.txt", bytes.NewReader([]byte("gone")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write baseline file status=%d, want 200", resp.StatusCode)
	}

	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-dir-whiteout",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-dir-whiteout", client.FSLayerEntryRequest{
		Path: "/repo/delete-dir/",
		Op:   "whiteout",
		Kind: "dir",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry dir whiteout: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-dir-whiteout")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 1 {
		t.Fatalf("commit = %+v, want committed applied=1", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/delete-dir/"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("deleted dir stat err=%v, want ErrNotFound", err)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/delete-dir/gone.txt"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("deleted child stat err=%v, want ErrNotFound", err)
	}
}

func TestFSLayerDiffMissingLayerReturnsNotFound(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	c := client.New(ts.URL, "")
	if _, err := c.DiffFSLayer(context.Background(), "missing"); !client.IsNotFound(err) {
		t.Fatalf("DiffFSLayer missing err=%v, want not found", err)
	}
}

func TestFSLayerRejectsEntryOutsideBaseRoot(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-scope",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	_, err := c.UpsertFSLayerEntry(ctx, "layer-scope", client.FSLayerEntryRequest{
		Path:    "/other/owned.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("owned"),
	})
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("UpsertFSLayerEntry outside root err=%v, want 400", err)
	}
}

func TestFSLayerCommitRollsBackAppliedEntriesOnFailure(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-rollback-commit",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-rollback-commit", client.FSLayerEntryRequest{
		Path:    "/repo/first.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("first"),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry first: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-rollback-commit", client.FSLayerEntryRequest{
		Path:       "/repo/second.txt",
		Op:         "upsert",
		Kind:       "file",
		StorageRef: "external-not-supported",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry second: %v", err)
	}
	if _, err := c.CommitFSLayer(ctx, "layer-rollback-commit"); err == nil {
		t.Fatal("CommitFSLayer unexpectedly succeeded")
	}
	if _, err := s.fallback.ReadCtx(ctx, "/repo/first.txt", 0, -1); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("first entry after failed commit err=%v, want ErrNotFound", err)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("auto-created parent after failed commit err=%v, want ErrNotFound", err)
	}
	layer, err := c.GetFSLayer(ctx, "layer-rollback-commit")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	if layer.State != "conflicted" {
		t.Fatalf("layer state=%s, want conflicted", layer.State)
	}
}

func TestFSLayerCommitPreflightChecksBaseInodeID(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.fallback.MkdirCtx(ctx, "/repo", 0o755); err != nil {
		t.Fatalf("MkdirCtx: %v", err)
	}
	conflicts := preflightFSLayerCommit(ctx, s.fallback, []datastore.FSLayerEntry{{
		Path:        "/repo/",
		Op:          datastore.FSLayerEntryOpChmod,
		Kind:        datastore.FSLayerEntryKindDir,
		BaseInodeID: "stale-inode",
		Mode:        0o700,
	}})
	if len(conflicts) != 1 || conflicts[0].Reason != "base inode changed" {
		t.Fatalf("conflicts=%+v, want base inode changed", conflicts)
	}
}

func TestValidateFSLayerCommitSnapshotsRejectsIncomplete(t *testing.T) {
	err := validateFSLayerCommitSnapshots([]fsLayerBaseSnapshot{{
		Path:       "/repo/a.txt",
		SnapshotOK: false,
	}})
	if err == nil {
		t.Fatal("validateFSLayerCommitSnapshots succeeded, want error")
	}
}
