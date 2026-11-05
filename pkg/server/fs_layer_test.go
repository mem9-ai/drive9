package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

func TestFSLayerPathQueryPreservesControlWhitespace(t *testing.T) {
	const path = "/repo/line\nbreak\tname\r"
	req := httptest.NewRequest(http.MethodGet, "/v1/layers/layer/entries?path="+url.QueryEscape(path), nil)
	if got := fsLayerPathQuery(req); got != path {
		t.Fatalf("path query = %q, want %q", got, path)
	}
}

func TestFSLayerPathQueryPreservesEdgeSpaces(t *testing.T) {
	for _, path := range []string{"/repo/ leading", "/repo/trailing ", "/repo/ both "} {
		req := httptest.NewRequest(http.MethodGet, "/v1/layers/layer/entries?path="+url.QueryEscape(path), nil)
		if got := fsLayerPathQuery(req); got != path {
			t.Fatalf("path query = %q, want %q", got, path)
		}
	}
}

func TestFSLayerRenameTargetPreservesControlWhitespace(t *testing.T) {
	entry := &datastore.FSLayerEntry{ContentText: "/repo/target\t\r\n"}
	if got := fsLayerRenameTarget(entry); got != entry.ContentText {
		t.Fatalf("rename target = %q, want %q", got, entry.ContentText)
	}
}

func TestFSLayerRenameTargetPreservesSpaces(t *testing.T) {
	entry := &datastore.FSLayerEntry{ContentText: "  "}
	if got := fsLayerRenameTarget(entry); got != entry.ContentText {
		t.Fatalf("rename target = %q, want %q", got, entry.ContentText)
	}
}

func TestFSLayerObjectPathPreservesTrailingControlWhitespace(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-control-whitespace",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	const path = "/repo/name\t"
	if _, err := c.UploadFSLayerFile(ctx, "layer-control-whitespace", path, strings.NewReader("payload"), 7, 0, 0o644, true); err != nil {
		t.Fatalf("UploadFSLayerFile: %v", err)
	}
	entry, err := c.GetFSLayerEntry(ctx, "layer-control-whitespace", path)
	if err != nil {
		t.Fatalf("GetFSLayerEntry: %v", err)
	}
	if entry.Path != path {
		t.Fatalf("entry path = %q, want %q", entry.Path, path)
	}
	data, err := c.ReadFSLayerFile(ctx, "layer-control-whitespace", path, nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("payload = %q, want payload", data)
	}
}

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

func TestFSLayerCommitReplaysUpsertBeforeChmod(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-upsert-chmod",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-chmod", client.FSLayerEntryRequest{
		Path:    "/repo/chmod.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("layered chmod"),
		Mode:    0o644,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry upsert: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-chmod", client.FSLayerEntryRequest{
		Path: "/repo/chmod.txt",
		Op:   "chmod",
		Kind: "file",
		Mode: 0o600,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry chmod: %v", err)
	}
	current, err := c.DiffFSLayer(ctx, "layer-upsert-chmod")
	if err != nil {
		t.Fatalf("DiffFSLayer: %v", err)
	}
	if len(current) != 1 || current[0].Op != "chmod" {
		t.Fatalf("current diff = %+v, want latest chmod entry", current)
	}
	replay, err := c.ReplayFSLayer(ctx, "layer-upsert-chmod")
	if err != nil {
		t.Fatalf("ReplayFSLayer: %v", err)
	}
	if len(replay) != 2 || replay[0].Op != "upsert" || replay[1].Op != "chmod" {
		t.Fatalf("replay = %+v, want upsert then chmod", replay)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-upsert-chmod")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 2 {
		t.Fatalf("commit = %+v, want committed applied=2", commit)
	}
	data, err := s.fallback.ReadCtx(ctx, "/repo/chmod.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx: %v", err)
	}
	if !bytes.Equal(data, []byte("layered chmod")) {
		t.Fatalf("data = %q, want layered chmod", data)
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/repo/chmod.txt")
	if err != nil {
		t.Fatalf("StatNodeCtx: %v", err)
	}
	if nf.File == nil || nf.File.Mode&0o777 != 0o600 {
		t.Fatalf("mode = %+v, want 0600", nf.File)
	}
}

func TestFSLayerCommitReplaysSamePathUpsertsWithoutBaseRevConflict(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-same-path-upserts",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-same-path-upserts", client.FSLayerEntryRequest{
		Path:    "/repo/repeated.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("first"),
		Mode:    0o644,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry first: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-same-path-upserts", client.FSLayerEntryRequest{
		Path:    "/repo/repeated.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("second"),
		Mode:    0o600,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry second: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-same-path-upserts")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 2 {
		t.Fatalf("commit = %+v, want committed applied=2", commit)
	}
	data, err := s.fallback.ReadCtx(ctx, "/repo/repeated.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx: %v", err)
	}
	if !bytes.Equal(data, []byte("second")) {
		t.Fatalf("data = %q, want second", data)
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/repo/repeated.txt")
	if err != nil {
		t.Fatalf("StatNodeCtx: %v", err)
	}
	if nf.File == nil || nf.File.Mode&0o777 != 0o600 {
		t.Fatalf("mode = %+v, want 0600", nf.File)
	}
}

func TestFSLayerCommitAllowsSupersededUpsertModeBeforeWhiteout(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-upsert-whiteout",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-whiteout", client.FSLayerEntryRequest{
		Path:    "/repo/temp.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("temp"),
		Mode:    0o600,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry upsert: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-whiteout", client.FSLayerEntryRequest{
		Path: "/repo/temp.txt",
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry whiteout: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-upsert-whiteout")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 2 {
		t.Fatalf("commit = %+v, want committed applied=2", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/temp.txt"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("temp stat err=%v, want ErrNotFound", err)
	}
}

func TestFSLayerCommitReplaysUpsertBeforeRename(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/move.txt", []byte("base"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write base: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-upsert-rename",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-rename", client.FSLayerEntryRequest{
		Path:    "/repo/move.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("layered move"),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry upsert: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-upsert-rename", client.FSLayerEntryRequest{
		Path:        "/repo/move.txt",
		Op:          "rename",
		Kind:        "file",
		ContentText: "/repo/moved.txt",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rename: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-upsert-rename")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 2 {
		t.Fatalf("commit = %+v, want committed applied=2", commit)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/repo/move.txt"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("source stat err=%v, want ErrNotFound", err)
	}
	data, err := s.fallback.ReadCtx(ctx, "/repo/moved.txt", 0, -1)
	if err != nil {
		t.Fatalf("ReadCtx moved: %v", err)
	}
	if !bytes.Equal(data, []byte("layered move")) {
		t.Fatalf("moved data = %q, want layered move", data)
	}
}

func TestFSLayerCommitReplaysMkdirBeforeChmod(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-mkdir-chmod",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-mkdir-chmod", client.FSLayerEntryRequest{
		Path: "/repo/mode-dir/",
		Op:   "mkdir",
		Kind: "dir",
		Mode: 0o755,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry mkdir: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-mkdir-chmod", client.FSLayerEntryRequest{
		Path: "/repo/mode-dir/",
		Op:   "chmod",
		Kind: "dir",
		Mode: 0o700,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry chmod: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-mkdir-chmod")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v", err)
	}
	if commit.Status != "committed" || commit.Applied != 2 {
		t.Fatalf("commit = %+v, want committed applied=2", commit)
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/repo/mode-dir/")
	if err != nil {
		t.Fatalf("StatNodeCtx mode-dir: %v", err)
	}
	if !nf.Node.IsDirectory {
		t.Fatalf("dir node = %+v, want directory", nf.Node)
	}
}

func TestFSLayerCommitRejectsCommittingWithoutFence(t *testing.T) {
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
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-retry", client.FSLayerEntryRequest{
		Path:    "/repo/retry.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("pending"),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}
	if err := s.fallback.Store().SetFSLayerState(ctx, "layer-retry", datastore.FSLayerStateCommitting); err != nil {
		t.Fatalf("SetFSLayerState committing: %v", err)
	}
	if commit, err := c.CommitFSLayer(ctx, "layer-retry"); !errors.Is(err, client.ErrConflict) {
		t.Fatalf("CommitFSLayer committing commit=%+v err=%v, want conflict", commit, err)
	}
}

func TestFSLayerChildObjectReadFallsBackToMain(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/base.txt", []byte("from-main"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write main: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-obj-main-parent",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.ForkFSLayer(ctx, "layer-obj-main-parent", client.FSLayerForkRequest{
		LayerID: "layer-obj-main-child",
	}); err != nil {
		t.Fatalf("ForkFSLayer: %v", err)
	}
	got, err := c.ReadFSLayerFile(ctx, "layer-obj-main-child", "/repo/base.txt", nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile child main fallback: %v", err)
	}
	if !bytes.Equal(got, []byte("from-main")) {
		t.Fatalf("child read = %q, want from-main", got)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-obj-main-child", client.FSLayerEntryRequest{
		Path: "/repo/base.txt",
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		t.Fatalf("whiteout: %v", err)
	}
	if _, err := c.ReadFSLayerFile(ctx, "layer-obj-main-child", "/repo/base.txt", nil); err == nil {
		t.Fatal("whiteout read err=nil, want not found")
	}
}

func TestFSLayerRootObjectReadFallsBackToMain(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/only-main.txt", []byte("root-main"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write main: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-obj-root-fallback",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	got, err := c.ReadFSLayerFile(ctx, "layer-obj-root-fallback", "/repo/only-main.txt", nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile root main fallback: %v", err)
	}
	if !bytes.Equal(got, []byte("root-main")) {
		t.Fatalf("root read = %q, want root-main", got)
	}
}

func TestFSLayerChildObjectReadHonorsExplicitZeroMaxSeq(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-obj-parent",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-obj-parent", client.FSLayerEntryRequest{
		Path:    "/repo/a.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("v1"),
		Mode:    0o644,
	}); err != nil {
		t.Fatalf("parent upsert: %v", err)
	}
	if _, err := c.ForkFSLayer(ctx, "layer-obj-parent", client.FSLayerForkRequest{
		LayerID: "layer-obj-child",
	}); err != nil {
		t.Fatalf("fork: %v", err)
	}
	if _, err := c.CheckpointFSLayer(ctx, "layer-obj-child", client.FSLayerCheckpointRequest{
		CheckpointID: "cp-child-zero",
	}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-obj-child", client.FSLayerEntryRequest{
		Path:    "/repo/a.txt",
		Op:      "upsert",
		Kind:    "file",
		Content: []byte("v2"),
		Mode:    0o644,
	}); err != nil {
		t.Fatalf("child upsert: %v", err)
	}
	zero := int64(0)
	got, err := c.ReadFSLayerFile(ctx, "layer-obj-child", "/repo/a.txt", &zero)
	if err != nil {
		t.Fatalf("ReadFSLayerFile max_seq=0: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("max_seq=0 content=%q, want v1 (checkpoint tip, not later child write)", got)
	}
	tip, err := c.ReadFSLayerFile(ctx, "layer-obj-child", "/repo/a.txt", nil)
	if err != nil {
		t.Fatalf("ReadFSLayerFile tip: %v", err)
	}
	if !bytes.Equal(tip, []byte("v2")) {
		t.Fatalf("tip content=%q, want v2", tip)
	}
}

func TestFSLayerOverlayMatchesMainRejectsSizeOnlyEquality(t *testing.T) {
	e := &datastore.FSLayerEntry{
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		ContentBlob: []byte("aaaa"),
		SizeBytes:   4,
		Mode:        0o644,
	}
	nf := &datastore.NodeWithFile{
		File: &datastore.File{SizeBytes: 4, Mode: 0o644},
	}
	if fsLayerOverlayMatchesMain(e, nf) {
		t.Fatal("size-only inline blob must not match main")
	}
}

func TestPlanFSLayerCommitVsMainKeepsFileRecreateAfterRenameVacates(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(ctx, "/repo/a.txt", []byte("hello"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	overlay := map[string]datastore.FSLayerOverlayNode{
		"/repo/b.txt": {
			Path:       "/repo/b.txt",
			RenameFrom: "/repo/a.txt",
			Kind:       datastore.FSLayerEntryKindFile,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/a.txt",
				Op:          datastore.FSLayerEntryOpRename,
				Kind:        datastore.FSLayerEntryKindFile,
				ContentText: "/repo/b.txt",
			},
		},
		"/repo/a.txt": {
			Path:    "/repo/a.txt",
			HasBody: true,
			Kind:    datastore.FSLayerEntryKindFile,
			Mode:    0o644,
			Entry: datastore.FSLayerEntry{
				Path:           "/repo/a.txt",
				Op:             datastore.FSLayerEntryOpUpsert,
				Kind:           datastore.FSLayerEntryKindFile,
				ContentBlob:    []byte("hello"),
				ChecksumSHA256: "unused-equal-size-must-not-matter",
				SizeBytes:      5,
				Mode:           0o644,
			},
		},
	}
	// Give the overlay entry the live checksum so a naive match would skip.
	nf, err := s.fallback.StatNodeCtx(ctx, "/repo/a.txt")
	if err != nil {
		t.Fatalf("stat a.txt: %v", err)
	}
	if nf.File != nil {
		n := overlay["/repo/a.txt"]
		n.Entry.ChecksumSHA256 = nf.File.ChecksumSHA256
		overlay["/repo/a.txt"] = n
	}
	entries, err := planFSLayerCommitVsMain(ctx, s.fallback, overlay)
	if err != nil {
		t.Fatalf("planFSLayerCommitVsMain: %v", err)
	}
	var sawRename, sawUpsert bool
	for _, e := range entries {
		if e.Op == datastore.FSLayerEntryOpRename && e.Path == "/repo/a.txt" {
			sawRename = true
		}
		if e.Op == datastore.FSLayerEntryOpUpsert && e.Path == "/repo/a.txt" {
			sawUpsert = true
		}
	}
	if !sawRename || !sawUpsert {
		t.Fatalf("entries=%+v, want rename plus identical-body recreate", entries)
	}
}

func TestPlanFSLayerCommitVsMainKeepsSymlinkRecreateAfterRenameVacates(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.fallback.CreateSymlinkCtx(ctx, "/repo/link", "target.txt"); err != nil {
		t.Fatalf("CreateSymlinkCtx: %v", err)
	}
	overlay := map[string]datastore.FSLayerOverlayNode{
		"/repo/link2": {
			Path:       "/repo/link2",
			RenameFrom: "/repo/link",
			Kind:       datastore.FSLayerEntryKindSymlink,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/link",
				Op:          datastore.FSLayerEntryOpRename,
				Kind:        datastore.FSLayerEntryKindSymlink,
				ContentText: "/repo/link2",
			},
		},
		"/repo/link": {
			Path:    "/repo/link",
			HasBody: true,
			Kind:    datastore.FSLayerEntryKindSymlink,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/link",
				Op:          datastore.FSLayerEntryOpSymlink,
				Kind:        datastore.FSLayerEntryKindSymlink,
				ContentText: "target.txt",
			},
		},
	}
	entries, err := planFSLayerCommitVsMain(ctx, s.fallback, overlay)
	if err != nil {
		t.Fatalf("planFSLayerCommitVsMain: %v", err)
	}
	var sawRename, sawSymlink bool
	for _, e := range entries {
		if e.Op == datastore.FSLayerEntryOpRename && e.Path == "/repo/link" {
			sawRename = true
		}
		if e.Op == datastore.FSLayerEntryOpSymlink && e.Path == "/repo/link" {
			sawSymlink = true
		}
	}
	if !sawRename || !sawSymlink {
		t.Fatalf("entries=%+v, want rename plus identical symlink recreate", entries)
	}
}

func TestPlanFSLayerCommitVsMainKeepsMkdirAfterRenameVacatesDir(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.fallback.MkdirCtx(ctx, "/repo/", 0o755); err != nil {
		t.Fatalf("mkdir /repo/: %v", err)
	}
	if err := s.fallback.MkdirCtx(ctx, "/repo/old/", 0o755); err != nil {
		t.Fatalf("mkdir /repo/old/: %v", err)
	}
	overlay := map[string]datastore.FSLayerOverlayNode{
		"/repo/new/": {
			Path:       "/repo/new/",
			RenameFrom: "/repo/old/",
			Kind:       datastore.FSLayerEntryKindDir,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/old/",
				Op:          datastore.FSLayerEntryOpRename,
				Kind:        datastore.FSLayerEntryKindDir,
				ContentText: "/repo/new/",
			},
		},
		"/repo/old/": {
			Path:    "/repo/old/",
			HasBody: true,
			Kind:    datastore.FSLayerEntryKindDir,
			Mode:    0o755,
			Entry:   datastore.FSLayerEntry{Path: "/repo/old/", Op: datastore.FSLayerEntryOpMkdir, Kind: datastore.FSLayerEntryKindDir, Mode: 0o755},
		},
	}
	entries, err := planFSLayerCommitVsMain(ctx, s.fallback, overlay)
	if err != nil {
		t.Fatalf("planFSLayerCommitVsMain: %v", err)
	}
	var sawRename, sawMkdir bool
	for _, e := range entries {
		if e.Op == datastore.FSLayerEntryOpRename && e.Path == "/repo/old/" {
			sawRename = true
		}
		if e.Op == datastore.FSLayerEntryOpMkdir && e.Path == "/repo/old/" {
			sawMkdir = true
		}
	}
	if !sawRename || !sawMkdir {
		t.Fatalf("entries=%+v, want rename vacate plus mkdir recreate", entries)
	}
}

func TestPlanFSLayerCommitVsMainKeepsChmodAfterMissingRenameDest(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	overlay := map[string]datastore.FSLayerOverlayNode{
		"/repo/b.txt": {
			Path:       "/repo/b.txt",
			RenameFrom: "/repo/a.txt",
			Kind:       datastore.FSLayerEntryKindFile,
			Mode:       0o600,
			ModeSet:    true,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/a.txt",
				Op:          datastore.FSLayerEntryOpRename,
				Kind:        datastore.FSLayerEntryKindFile,
				ContentText: "/repo/b.txt",
				Mode:        0o600,
			},
		},
		"/repo/a.txt": {
			Path:             "/repo/a.txt",
			Whiteout:         true,
			ConsumedByRename: true,
			Kind:             datastore.FSLayerEntryKindFile,
			Entry:            datastore.FSLayerEntry{Path: "/repo/a.txt", Op: datastore.FSLayerEntryOpWhiteout},
		},
	}
	entries, err := planFSLayerCommitVsMain(ctx, s.fallback, overlay)
	if err != nil {
		t.Fatalf("planFSLayerCommitVsMain: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries=%+v, want rename then dest chmod", entries)
	}
	if entries[0].Op != datastore.FSLayerEntryOpRename || entries[0].Path != "/repo/a.txt" {
		t.Fatalf("first=%+v, want rename", entries[0])
	}
	if entries[1].Op != datastore.FSLayerEntryOpChmod || entries[1].Path != "/repo/b.txt" || entries[1].Mode&0o777 != 0o600 {
		t.Fatalf("second=%+v, want chmod dest 0600", entries[1])
	}
}

func TestPlanFSLayerCommitVsMainKeepsMissingSourceRename(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	overlay := map[string]datastore.FSLayerOverlayNode{
		"/repo/b.txt": {
			Path:       "/repo/b.txt",
			RenameFrom: "/repo/a.txt",
			Kind:       datastore.FSLayerEntryKindFile,
			Entry: datastore.FSLayerEntry{
				Path:        "/repo/a.txt",
				Op:          datastore.FSLayerEntryOpRename,
				Kind:        datastore.FSLayerEntryKindFile,
				ContentText: "/repo/b.txt",
			},
		},
		"/repo/a.txt": {
			Path:             "/repo/a.txt",
			Whiteout:         true,
			ConsumedByRename: true,
			Kind:             datastore.FSLayerEntryKindFile,
			Entry:            datastore.FSLayerEntry{Path: "/repo/a.txt", Op: datastore.FSLayerEntryOpWhiteout},
		},
	}
	entries, err := planFSLayerCommitVsMain(ctx, s.fallback, overlay)
	if err != nil {
		t.Fatalf("planFSLayerCommitVsMain: %v", err)
	}
	if len(entries) != 1 || entries[0].Op != datastore.FSLayerEntryOpRename || entries[0].Path != "/repo/a.txt" {
		t.Fatalf("entries=%+v, want kept missing-source rename", entries)
	}
}

func TestFSLayerChildCommitDirectoryRenameMovesDescendants(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if err := s.fallback.MkdirCtx(ctx, "/repo/", 0o755); err != nil {
		t.Fatalf("MkdirCtx /repo/: %v", err)
	}
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-parent-dir",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-parent-dir", client.FSLayerEntryRequest{
		Path: "/repo/old/",
		Op:   "mkdir",
		Kind: "dir",
		Mode: 0o755,
	}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-parent-dir", client.FSLayerEntryRequest{
		Path:      "/repo/old/child.txt",
		Op:        "upsert",
		Kind:      "file",
		Content:   []byte("child"),
		SizeBytes: 5,
		Mode:      0o644,
	}); err != nil {
		t.Fatalf("upsert child: %v", err)
	}
	if _, err := c.ForkFSLayer(ctx, "layer-parent-dir", client.FSLayerForkRequest{
		LayerID: "layer-child-dir",
		Name:    "child",
	}); err != nil {
		t.Fatalf("ForkFSLayer: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-child-dir", client.FSLayerEntryRequest{
		Path:        "/repo/old/",
		Op:          "rename",
		Kind:        "dir",
		ContentText: "/repo/new/",
	}); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	commit, err := c.CommitFSLayer(ctx, "layer-child-dir")
	if err != nil {
		t.Fatalf("CommitFSLayer: %v commit=%+v", err, commit)
	}
	if commit.Status != "committed" {
		t.Fatalf("commit = %+v, want committed", commit)
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
	grep, err := s.overlayFSLayerGrep(ctx, s.fallback, nil, "layer-search", "needle", "/repo/", 20, base)
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

func TestFSLayerCreateRequiresScopedWriteOnBaseRoot(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/layers", bytes.NewReader([]byte(`{"layer_id":"layer-denied","base_root_path":"/repo"}`)))
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/other",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	req = req.WithContext(withScope(req.Context(), scope))
	rr := httptest.NewRecorder()
	s.handleFSLayerCreate(rr, req, s.fallback.Store())
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	if _, err := s.fallback.Store().GetFSLayer(context.Background(), "layer-denied"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("layer-denied err=%v, want ErrNotFound", err)
	}
}

func TestFSLayerEntryMutationRequiresScopedPathPermission(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{
		LayerID:      "layer-auth",
		BaseRootPath: "/",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	layer, err := s.fallback.Store().GetFSLayer(ctx, "layer-auth")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-auth/entries", bytes.NewReader([]byte(`{
		"path": "/repo/source.txt",
		"op": "rename",
		"kind": "file",
		"content_text": "/other/target.txt"
	}`)))
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/repo",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true},
		}},
	}
	req = req.WithContext(withScope(req.Context(), scope))
	rr := httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, s.fallback.Store(), layer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	entries, err := s.fallback.Store().ListFSLayerEntryLog(ctx, "layer-auth")
	if err != nil {
		t.Fatalf("ListFSLayerEntryLog: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries=%+v, want none after denied mutation", entries)
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
	// A directory whiteout that is empty at preflight time but not at apply
	// time fails mid-commit, which is the failure this test rolls back.
	// (References the server could never mint are rejected at upsert time
	// instead — see TestFSLayerEntryRejectsUnissuedStorageRef.)
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-rollback-commit", client.FSLayerEntryRequest{
		Path: "/repo/",
		Op:   "whiteout",
		Kind: "dir",
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

func TestFSLayerEntryUpsertBaseRevisionCAS(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	putBase := func(body string) {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/base.txt", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("write base: %d", resp.StatusCode)
		}
	}
	upsertEntry := func(baseRevision int64) int {
		payload := fmt.Sprintf(`{"path":"/base.txt","op":"upsert","kind":"file","base_revision":%d,"content":"bGF5ZXIgZGF0YQ=="}`, baseRevision)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/layers/layer-cas/entries", strings.NewReader(payload))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	putBase("v1")
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/layers", strings.NewReader(`{"layer_id":"layer-cas","base_root_path":"/"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		t.Fatalf("layer create: %d", resp.StatusCode)
	}

	// Upsert against the current base revision is accepted.
	if code := upsertEntry(1); code != http.StatusOK {
		t.Fatalf("upsert with current base revision: %d, want 200", code)
	}

	// Base moves to revision 2; the stale base_revision=1 upsert must be
	// rejected synchronously (no silent splice into the layer).
	putBase("v2")
	if code := upsertEntry(1); code != http.StatusConflict {
		t.Fatalf("upsert with stale base revision: %d, want 409", code)
	}

	// The fresh base revision is accepted again.
	if code := upsertEntry(2); code != http.StatusOK {
		t.Fatalf("upsert with fresh base revision: %d, want 200", code)
	}
}

// A layer entry's storage_ref is caller-supplied and is later fed to the
// object store, so only references the server itself mints may be stored. An
// unvalidated ref let a caller point at another tenant's object, or at an
// arbitrary host file on the local backend.
func TestFSLayerEntryRejectsUnissuedStorageRef(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	c := client.New(ts.URL, "")
	if _, err := c.CreateFSLayer(ctx, client.FSLayerCreateRequest{
		LayerID:      "layer-storage-ref",
		BaseRootPath: "/repo",
	}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	for _, ref := range []string{
		"external-not-supported",
		"../../etc/passwd",
		"/etc/passwd",
		"blobs/../other/file",
		"layers/another-layer/object",
	} {
		if _, err := c.UpsertFSLayerEntry(ctx, "layer-storage-ref", client.FSLayerEntryRequest{
			Path:       "/repo/file.txt",
			Op:         "upsert",
			Kind:       "file",
			StorageRef: ref,
		}); err == nil {
			t.Fatalf("UpsertFSLayerEntry accepted storage_ref %q", ref)
		}
	}
	// The layer's own payload prefix is still accepted.
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-storage-ref", client.FSLayerEntryRequest{
		Path:       "/repo/file.txt",
		Op:         "upsert",
		Kind:       "file",
		StorageRef: "layers/layer-storage-ref/object-1",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rejected a server-issued ref: %v", err)
	}
	if _, err := c.UpsertFSLayerEntry(ctx, "layer-storage-ref", client.FSLayerEntryRequest{
		Path:       "/repo/blob.txt",
		Op:         "upsert",
		Kind:       "file",
		StorageRef: "blobs/file-1",
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rejected a blob ref: %v", err)
	}
}

// The commit rollback buffer is the tenant's to inflate; it must be bounded or
// the least-privileged token that can commit a layer can drive the server OOM.
func TestFSLayerSnapshotBudget(t *testing.T) {
	if err := checkFSLayerSnapshotBudget(1, 1); err != nil {
		t.Fatalf("small snapshot rejected: %v", err)
	}

	if err := checkFSLayerSnapshotBudget(maxFSLayerCommitSnapshots, maxFSLayerCommitSnapshotBytes); err != nil {
		t.Fatalf("at-limit snapshot rejected: %v", err)
	}
	if err := checkFSLayerSnapshotBudget(maxFSLayerCommitSnapshots+1, 0); err == nil {
		t.Fatal("path-count budget not enforced")
	}
	if err := checkFSLayerSnapshotBudget(1, maxFSLayerCommitSnapshotBytes+1); err == nil {
		t.Fatal("byte budget not enforced")
	}
}

// A base file bigger than the remaining snapshot budget must be refused before
// its bytes are read: the backend materializes a whole file even when a length
// is requested, so checking the size first is what makes the byte cap real.
func TestFSLayerSnapshotRefusesOversizedBaseFileWithoutReadingIt(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	content := bytes.Repeat([]byte("a"), 4096)
	if _, err := s.fallback.WriteCtx(ctx, "/big.bin", content, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write base file: %v", err)
	}

	snap := snapshotFSLayerBasePath(ctx, s.fallback, "/big.bin", 1024)
	if !snap.TooLarge {
		t.Fatal("oversized base file was not reported as TooLarge")
	}
	if len(snap.Data) != 0 {
		t.Fatalf("oversized base file was read into memory (%d bytes)", len(snap.Data))
	}

	// Within budget the same file snapshots normally.
	snap = snapshotFSLayerBasePath(ctx, s.fallback, "/big.bin", int64(len(content)))
	if snap.TooLarge || !snap.SnapshotOK {
		t.Fatalf("in-budget snapshot = %+v", snap)
	}
	if !bytes.Equal(snap.Data, content) {
		t.Fatalf("snapshot data = %d bytes, want %d", len(snap.Data), len(content))
	}
}

// A "blobs/<fileID>" reference names a file entity, not a path. A scoped caller
// that knows an out-of-scope file's stable id (stat exposes it as resource_id)
// must not be able to alias it onto an in-scope layer path and read the bytes
// back through the layer object endpoint.
func TestFSLayerStorageRefRequiresScopedReadOnSource(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// A file outside the token's scope, at a path the token cannot read.
	if _, err := s.fallback.WriteCtx(ctx, "/secret/payroll.txt", []byte("classified"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	node, err := s.fallback.StatNodeCtx(ctx, "/secret/payroll.txt")
	if err != nil {
		t.Fatalf("stat source file: %v", err)
	}
	fileID := node.File.FileID
	if fileID == "" {
		t.Fatal("source file has no file id")
	}
	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-src", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	layer, err := s.fallback.Store().GetFSLayer(ctx, "layer-src")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}

	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/repo",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	upsert := func(path string) int {
		body := fmt.Sprintf(`{"path":%q,"op":"upsert","kind":"file","storage_ref":"blobs/%s"}`, path, fileID)
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-src/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scope))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, s.fallback.Store(), layer)
		return rr.Code
	}

	// The destination is in scope, but the referenced source is not.
	if code := upsert("/repo/leak.txt"); code != http.StatusForbidden {
		t.Fatalf("upsert status=%d, want 403 (out-of-scope source)", code)
	}

	// The same reference is legitimate when the source itself is in scope.
	if _, err := s.fallback.WriteCtx(ctx, "/repo/in-scope.txt", []byte("fine"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write in-scope file: %v", err)
	}
	inScope, err := s.fallback.StatNodeCtx(ctx, "/repo/in-scope.txt")
	if err != nil {
		t.Fatalf("stat in-scope file: %v", err)
	}
	body := fmt.Sprintf(`{"path":"/repo/allowed.txt","op":"upsert","kind":"file","storage_ref":"blobs/%s"}`, inScope.File.FileID)
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-src/entries", bytes.NewReader([]byte(body)))
	req = req.WithContext(withScope(req.Context(), scope))
	rr := httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, s.fallback.Store(), layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("in-scope upsert status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
}

// A fork tenant inherits the parent's storage namespace, so a blob id that
// belongs to the parent resolves in the same bucket for the child. An owner key
// (unscoped) must not be able to alias a parent blob onto its own layer path:
// existence in the fork's own metadata is what authorizes it.
func TestFSLayerStorageRefForkOwnerKeyStaysInTenantNamespace(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-fork", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	layer, err := s.fallback.Store().GetFSLayer(ctx, "layer-fork")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}

	// An owner key of a forked tenant: full access, but the storage namespace is
	// shared with the parent.
	forkOwner := &TenantScope{IsScoped: false, IsFork: true, ParentTenantID: "tenant-parent"}
	upsert := func(scope *TenantScope, path, fileID string) int {
		body := fmt.Sprintf(`{"path":%q,"op":"upsert","kind":"file","storage_ref":"blobs/%s"}`, path, fileID)
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-fork/entries", bytes.NewReader([]byte(body)))
		if scope != nil {
			req = req.WithContext(withScope(req.Context(), scope))
		}
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, s.fallback.Store(), layer)
		return rr.Code
	}

	// A blob id that exists only in the parent's namespace has no dentry here.
	if code := upsert(forkOwner, "/repo/stolen.txt", "file-from-parent-tenant"); code != http.StatusForbidden {
		t.Fatalf("fork owner upsert status=%d, want 403 (blob outside the fork's metadata)", code)
	}

	// The same reference is legitimate once the file exists in this tenant.
	if _, err := s.fallback.WriteCtx(ctx, "/repo/own.txt", []byte("mine"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write own file: %v", err)
	}
	own, err := s.fallback.StatNodeCtx(ctx, "/repo/own.txt")
	if err != nil {
		t.Fatalf("stat own file: %v", err)
	}
	if code := upsert(forkOwner, "/repo/copy.txt", own.File.FileID); code != http.StatusOK {
		t.Fatalf("fork owner upsert of its own blob status=%d, want 200", code)
	}

	// A live tenant keeps the cheaper contract: its namespace is exclusive, so an
	// owner key needs no per-blob check.
	liveOwner := &TenantScope{IsScoped: false}
	if code := upsert(liveOwner, "/repo/live.txt", "file-from-parent-tenant"); code != http.StatusOK {
		t.Fatalf("live owner upsert status=%d, want 200", code)
	}
}

// The read path must deny the same alias, not just entry creation.
func TestFSLayerObjectReadForkOwnerKeyRejectsForeignBlob(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-fork-read", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	layer, err := s.fallback.Store().GetFSLayer(ctx, "layer-fork-read")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	// Store the entry directly so the read path is exercised on its own.
	if err := s.fallback.Store().UpsertFSLayerEntry(ctx, &datastore.FSLayerEntry{
		LayerID:     layer.LayerID,
		Path:        "/repo/stolen.txt",
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		StorageType: string(datastore.StorageS3),
		StorageRef:  "blobs/file-from-parent-tenant",
		SizeBytes:   4,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}

	forkOwner := &TenantScope{IsScoped: false, IsFork: true, ParentTenantID: "tenant-parent"}
	req := httptest.NewRequest(http.MethodGet, "/v1/layers/layer-fork-read/object?path=%2Frepo%2Fstolen.txt", nil)
	req = req.WithContext(withScope(req.Context(), forkOwner))
	rr := httptest.NewRecorder()
	s.handleFSLayerObjectRead(rr, req, s.fallback, s.fallback.Store(), layer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("read status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
}

// An overwrite keeps the file id but writes the bytes to a generated object key,
// so "blobs/<key>" is not always a file id. A fork owner key must still be able
// to reference such a blob, and a foreign key must still be refused.
func TestFSLayerStorageRefForkResolvesOverwrittenBlobKey(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	if _, err := s.fallback.WriteCtx(ctx, "/repo/large.bin", bytes.Repeat([]byte("x"), 8<<10), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write file: %v", err)
	}
	node, err := s.fallback.StatNodeCtx(ctx, "/repo/large.bin")
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	if node.File == nil || node.File.FileID == "" {
		t.Fatal("file has no id")
	}
	// Force the overwrite shape: same file identity, generated object key.
	overwriteKey := "blobs/" + token.NewID()
	refHash := datastore.StorageRefHash(overwriteKey)
	if _, err := s.fallback.Store().DB().ExecContext(ctx,
		`UPDATE contents SET storage_type = ?, storage_ref = ?, storage_ref_hash = ? WHERE inode_id = ?`,
		datastore.StorageS3, overwriteKey, refHash, node.File.FileID); err != nil {
		t.Fatalf("rewrite storage_ref: %v", err)
	}

	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-ow", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	layer, err := s.fallback.Store().GetFSLayer(ctx, "layer-ow")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}

	forkOwner := &TenantScope{IsScoped: false, IsFork: true, ParentTenantID: "tenant-parent"}
	upsert := func(blobKey string) int {
		body := fmt.Sprintf(`{"path":"/repo/alias.bin","op":"upsert","kind":"file","storage_ref":"blobs/%s"}`, blobKey)
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-ow/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), forkOwner))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, s.fallback.Store(), layer)
		return rr.Code
	}

	if code := upsert(strings.TrimPrefix(overwriteKey, "blobs/")); code != http.StatusOK {
		t.Fatalf("overwrite blob reference status=%d body=%s, want 200", code, "")
	}
	// A key with no file behind it in this tenant is still refused.
	if code := upsert(token.NewID()); code != http.StatusForbidden {
		t.Fatalf("foreign blob status=%d, want 403", code)
	}
}

// The grep overlay reads layer-entry bytes, so it has to apply the same
// storage_ref source check as the object-read endpoint. Skipping it would leak a
// match/no-match verdict for content the caller cannot read.
func TestOverlayFSLayerGrepHonorsStorageRefScope(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Above the inline threshold so the file is backed by an S3 object.
	secretBody := append(bytes.Repeat([]byte("a"), 64<<10), []byte("needle in secret")...)
	if _, err := s.fallback.WriteCtx(ctx, "/secret/out.txt", secretBody, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	secret, err := s.fallback.StatNodeCtx(ctx, "/secret/out.txt")
	if err != nil {
		t.Fatalf("stat secret file: %v", err)
	}
	if secret.File == nil || !strings.HasPrefix(secret.File.StorageRef, "blobs/") {
		t.Fatalf("secret file storage_ref = %q, want an S3 blob", secret.File.StorageRef)
	}
	if err := s.fallback.Store().CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-grep-scope", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	// An owner-created alias: the entry lives at a path the scoped key can read,
	// but its bytes are the out-of-scope file's blob.
	if err := s.fallback.Store().UpsertFSLayerEntry(ctx, &datastore.FSLayerEntry{
		LayerID:     "layer-grep-scope",
		Path:        "/repo/alias.txt",
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		StorageType: string(datastore.StorageS3),
		StorageRef:  secret.File.StorageRef,
		SizeBytes:   secret.File.SizeBytes,
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry: %v", err)
	}

	scoped := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/repo", Ops: map[FSOp]bool{FSOpRead: true}}},
	}
	results, err := s.overlayFSLayerGrep(ctx, s.fallback, scoped, "layer-grep-scope", "needle", "/repo/", 20, nil)
	if err != nil {
		t.Fatalf("overlayFSLayerGrep: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("grep leaked an out-of-scope match: %+v", results)
	}

	// The same entry matches once the source is inside the caller's scope.
	wider := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/", Ops: map[FSOp]bool{FSOpRead: true}}},
	}
	results, err = s.overlayFSLayerGrep(ctx, s.fallback, wider, "layer-grep-scope", "needle", "/repo/", 20, nil)
	if err != nil {
		t.Fatalf("overlayFSLayerGrep(wider): %v", err)
	}
	if len(results) != 1 || results[0].Path != "/repo/alias.txt" {
		t.Fatalf("grep results = %+v, want the aliased entry for an in-scope source", results)
	}
}

// The storage_ref resolution relies on an invariant the backend owns: a
// blobs/<key> object is written once and never rewritten in place, so a key that
// a fork's metadata can reach always holds the bytes that file id had. This pins
// that invariant with a real overwrite rather than a fabricated row.
func TestOverwriteMintsNewBlobKeyAndKeepsOriginalBytes(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	if _, err := s.fallback.WriteCtx(ctx, "/repo/bin.dat", bytes.Repeat([]byte("one"), 64<<10), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("create: %v", err)
	}
	created, err := s.fallback.StatNodeCtx(ctx, "/repo/bin.dat")
	if err != nil {
		t.Fatalf("stat after create: %v", err)
	}
	originalRef := created.File.StorageRef
	if !strings.HasPrefix(originalRef, "blobs/") {
		t.Fatalf("storage_ref = %q, want an S3 blob", originalRef)
	}
	originalBytes, err := s.fallback.ReadCtx(ctx, "/repo/bin.dat", 0, -1)
	if err != nil {
		t.Fatalf("read original: %v", err)
	}

	if _, err := s.fallback.WriteCtx(ctx, "/repo/bin.dat", bytes.Repeat([]byte("two"), 64<<10), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	overwritten, err := s.fallback.StatNodeCtx(ctx, "/repo/bin.dat")
	if err != nil {
		t.Fatalf("stat after overwrite: %v", err)
	}
	if overwritten.File.FileID != created.File.FileID {
		t.Fatalf("file id changed on overwrite (%s -> %s); the resolution assumes it is stable",
			created.File.FileID, overwritten.File.FileID)
	}
	if overwritten.File.StorageRef == originalRef {
		t.Fatalf("overwrite reused blob key %q; the storage-ref fallback assumes a fresh key per write", originalRef)
	}
	// The original object must still hold the original bytes: a fork whose
	// metadata still points at originalRef reads history, not the new content.
	staleBytes, err := s.fallback.ReadFSLayerEntryData(ctx, &datastore.FSLayerEntry{
		StorageType: string(datastore.StorageS3),
		StorageRef:  originalRef,
	})
	if err != nil {
		t.Fatalf("original blob is gone after overwrite: %v", err)
	}
	if !bytes.Equal(staleBytes, originalBytes) {
		t.Fatalf("the original blob was rewritten in place (%d bytes, want the pre-overwrite %d)",
			len(staleBytes), len(originalBytes))
	}
	again, err := s.fallback.ReadCtx(ctx, "/repo/bin.dat", 0, -1)
	if err != nil {
		t.Fatalf("read after overwrite: %v", err)
	}
	if bytes.Equal(again, originalBytes) {
		t.Fatal("overwrite did not change the visible content")
	}
}
