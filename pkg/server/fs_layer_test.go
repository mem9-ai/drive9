package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
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

func TestFSLayerObjectUploadReadAndCommitLargeFile(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-large-object",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	payload := bytes.Repeat([]byte("x"), 2<<20)
	payload = append(payload, []byte("tail")...)
	entry, err := c.UploadFSLayerFile(ctx, "layer-large-object", "/repo/large.bin", bytes.NewReader(payload), int64(len(payload)), 0, 0o644, true)
	if err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}
	if entry.StorageType != "s3" || entry.StorageRef == "" || entry.SizeBytes != int64(len(payload)) {
		t.Fatalf("uploaded entry = %+v", entry)
	}
	got, err := c.ReadFSLayerFile(ctx, "layer-large-object", "/repo/large.bin", nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadFSLayerFile payload mismatch: got %d bytes want %d", len(got), len(payload))
	}
	grep, err := c.GrepWithLayer("tail", "/repo", 10, "layer-large-object")
	if err != nil {
		t.Fatalf("GrepWithLayer large object: %v", err)
	}
	if len(grep) != 1 || grep[0].Path != "/repo/large.bin" {
		t.Fatalf("GrepWithLayer large object = %+v, want /repo/large.bin", grep)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-large-object")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 1 {
		t.Fatalf("commit = %+v, want committed applied=1", commit)
	}
	committed, err := s.fallback.ReadCtx(ctx, "/repo/large.bin", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx committed: %v", err)
	}
	if !bytes.Equal(committed, payload) {
		t.Fatalf("committed payload mismatch: got %d bytes want %d", len(committed), len(payload))
	}
}

func TestFSLayerCommitRetrySkipsAlreadyAppliedEntries(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-retry",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	content := []byte("already applied")
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-retry", client.FSLayerEntryRequest{
		Path:      "/repo/retry.txt",
		Op:        "upsert",
		Kind:      "file",
		Content:   content,
		SizeBytes: int64(len(content)),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/retry.txt", content, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("simulate partial apply: %v", err)
	}
	if err := s.fallback.Store().SetFSLayerState(ctx, "layer-retry", datastore.FSLayerStateCommitting); err != nil {
		t.Fatalf("SetFSLayerState committing: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-retry")
	if err != nil {
		t.Fatalf("CommitFSLayer retry: %v", err)
	}
	if commit.Status != "committed" {
		t.Fatalf("commit = %+v, want committed", commit)
	}
}

func TestFSLayerCommitDirectoryRename(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if err := s.fallback.MkdirCtx(ctx, "/repo/old/", 0o755); err != nil {
		t.Fatalf("MkdirCtx old: %v", err)
	}
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/old/child.txt", []byte("child"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write child: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-dir-rename",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-dir-rename", client.FSLayerEntryRequest{
		Path:        "/repo/old/",
		Op:          "rename",
		Kind:        "dir",
		ContentText: "/repo/new/",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rename dir: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-dir-rename")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 1 {
		t.Fatalf("commit = %+v, want committed applied=1", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/old/child.txt"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("old child stat err=%v, want ErrNotFound", err)
	}
	got, err := s.fallback.ReadCtx(ctx, "/repo/new/child.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx new child: %v", err)
	}
	if !bytes.Equal(got, []byte("child")) {
		t.Fatalf("new child content = %q, want child", got)
	}
}

func TestFSLayerSearchOverlayMergesLayerEntries(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/base.txt", []byte("needle base"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write base: %v", err)
	}
	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{
		LayerID:      "layer-search",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if err := s.fallback.Store().UpsertFSLayerEntry(ctx, &datastore.FSLayerEntry{
		LayerID:     "layer-search",
		Path:        "/repo/base.txt",
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		ContentBlob: []byte("overlay without match"),
		SizeBytes:   int64(len("overlay without match")),
	}); err != nil {
		t.Fatalf("Upsert base overlay: %v", err)
	}
	if err := s.fallback.Store().UpsertFSLayerEntry(ctx, &datastore.FSLayerEntry{
		LayerID:     "layer-search",
		Path:        "/repo/layer.txt",
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		ContentBlob: []byte("needle layer"),
		SizeBytes:   int64(len("needle layer")),
	}); err != nil {
		t.Fatalf("Upsert layer file: %v", err)
	}
	base := []datastore.SearchResult{{Path: "/repo/base.txt", Name: "base.txt", SizeBytes: 11}}
	grep, err := overlayFSLayerGrep(ctx, s.fallback, "layer-search", "needle", "/repo/", 20, base)
	if err != nil {
		t.Fatalf("overlayFSLayerGrep: %v", err)
	}
	if len(grep) != 1 || grep[0].Path != "/repo/layer.txt" {
		t.Fatalf("grep = %+v, want only layer file", grep)
	}
	find, err := overlayFSLayerFind(ctx, s.fallback, "layer-search", &datastore.FindFilter{PathPrefix: "/repo/"}, base)
	if err != nil {
		t.Fatalf("overlayFSLayerFind: %v", err)
	}
	gotFind := map[string]bool{}
	for _, result := range find {
		gotFind[result.Path] = true
	}
	if len(find) != 2 || !gotFind["/repo/base.txt"] || !gotFind["/repo/layer.txt"] {
		t.Fatalf("find = %+v, want overlaid base and layer file", find)
	}
}

func TestFSLayerCommitRejectsNonEmptyDirectoryWhiteout(t *testing.T) {
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
	if err == nil {
		t.Fatalf("CommitFSLayer err=nil commit=%+v, want conflict", commit)
	}
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusConflict {
		t.Fatalf("CommitFSLayer err=%v, want conflict", err)
	}
	if commit == nil || len(commit.Conflicts) != 1 || commit.Conflicts[0].Reason != "directory whiteout requires empty directory" {
		t.Fatalf("commit conflict = %+v", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/delete-dir/gone.txt"); err != nil {
		t.Fatalf("baseline child should remain, stat err=%v", err)
	}
}

func TestFSLayerCommitAllowsEmptyDirectoryWhiteout(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if err := s.fallback.MkdirCtx(ctx, "/repo/empty/", 0o755); err != nil {
		t.Fatalf("MkdirCtx: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-empty-whiteout",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-empty-whiteout", client.FSLayerEntryRequest{
		Path: "/repo/empty/",
		Op:   "whiteout",
		Kind: "dir",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry dir whiteout: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-empty-whiteout")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 1 {
		t.Fatalf("commit = %+v, want committed applied=1", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/empty/"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("deleted dir stat err=%v, want ErrNotFound", err)
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

func TestFSLayerAcceptsDirectoryRenameEntry(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-dir-rename",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	entry, err := c.UpsertFSLayerEntry(ctx, "layer-dir-rename", client.FSLayerEntryRequest{
		Path:        "/repo/old-dir/",
		Op:          "rename",
		Kind:        "dir",
		ContentText: "/repo/new-dir/",
	})
	if err != nil {
		t.Fatalf("UpsertFSLayerEntry directory rename: %v", err)
	}
	if entry.Path != "/repo/old-dir/" || entry.ContentText != "/repo/new-dir/" {
		t.Fatalf("directory rename entry path=%q target=%q", entry.Path, entry.ContentText)
	}
}

func TestFSLayerCommitRejectsRenameTargetConflict(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/repo/source.txt", []byte("source"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := s.fallback.WriteCtx(ctx, "/repo/target.txt", []byte("target"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write target: %v", err)
	}

	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-rename-conflict",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-rename-conflict", client.FSLayerEntryRequest{
		Path:        "/repo/source.txt",
		Op:          "rename",
		Kind:        "file",
		ContentText: "/repo/target.txt",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rename: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-rename-conflict")
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("CommitFSLayer err=%v, want conflict", err)
	}
	if commit == nil || len(commit.Conflicts) != 1 || commit.Conflicts[0].Path != "/repo/target.txt" || commit.Conflicts[0].Reason != "rename target exists" {
		t.Fatalf("commit conflicts = %+v, want rename target exists", commit)
	}
	data, err := s.fallback.ReadCtx(ctx, "/repo/target.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx target: %v", err)
	}
	if !bytes.Equal(data, []byte("target")) {
		t.Fatalf("target data = %q, want target", data)
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
	}}, false)
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
