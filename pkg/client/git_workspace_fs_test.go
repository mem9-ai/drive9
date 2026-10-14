package client

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

const fsTestCommit = "00000000000000000000000000000000000000c1"

func newFSClient(t *testing.T) (*Client, *testbackend.Backend) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	return New(ts.URL, ""), b
}

func TestGitWorkspaceFSUpsertListGetDelete(t *testing.T) {
	c, b := newFSClient(t)
	ctx := context.Background()

	ws, err := c.GitWorkspaceFSUpsert(ctx, GitWorkspaceRequest{
		RootPath:   "/repo",
		RepoURL:    "https://example.com/r.git",
		BranchName: "main",
		HeadCommit: "abc123",
		Mode:       "fast-blobless",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ws.WorkspaceID, "gitws-") {
		t.Fatalf("workspace id = %q", ws.WorkspaceID)
	}
	if _, ok := b.Node(gitws.ManifestPath(ws.WorkspaceID)); !ok {
		t.Fatal("manifest missing")
	}
	if n, _ := b.Node(gitws.StatePath(ws.WorkspaceID)); string(n.Data) != "dormant\n" {
		t.Fatalf("initial state = %q", n.Data)
	}

	list, err := c.GitWorkspaceFSList(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v %v", list, err)
	}
	got, err := c.GitWorkspaceFSGetByRoot(ctx, "/repo")
	if err != nil || got.WorkspaceID != ws.WorkspaceID || got.Mode != "fast-blobless" {
		t.Fatalf("get by root = %+v %v", got, err)
	}
	if _, err := c.GitWorkspaceFSGetByRoot(ctx, "/nope"); !IsGitWorkspaceFSNotFound(err) {
		t.Fatalf("missing root err = %v", err)
	}

	// A second upsert on the same id refreshes fields without dropping
	// the creation stamp or unknown keys.
	manifest := map[string]any{"workspace_id": ws.WorkspaceID, "root_path": "/repo", "future_field": 42}
	if err := writeRawJSON(t, c, gitws.ManifestPath(ws.WorkspaceID), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GitWorkspaceFSUpsert(ctx, GitWorkspaceRequest{
		WorkspaceID: ws.WorkspaceID,
		RootPath:    "/repo",
		HeadCommit:  "def456",
		Mode:        "fast",
	}); err != nil {
		t.Fatal(err)
	}
	body, err := c.ReadCtx(ctx, gitws.ManifestPath(ws.WorkspaceID))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "\"future_field\":42") {
		t.Fatalf("unknown key stripped by rewrite: %s", body)
	}

	if err := c.GitWorkspaceFSDelete(ctx, ws.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	if list, err = c.GitWorkspaceFSList(ctx); err != nil || len(list) != 0 {
		t.Fatalf("list after delete = %v %v", list, err)
	}
	// The store subtree is swept best-effort after deregistration.
	if _, ok := b.Node(gitws.ManifestPath(ws.WorkspaceID)); ok {
		t.Fatal("manifest survived deletion")
	}
}

func TestGitWorkspaceFSTreeIndexRoundtrip(t *testing.T) {
	c, _ := newFSClient(t)
	ctx := context.Background()
	ws, err := c.GitWorkspaceFSUpsert(ctx, GitWorkspaceRequest{RootPath: "/repo", HeadCommit: fsTestCommit})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []GitTreeNode{
		{Path: "src", Kind: "dir", Mode: "040755"},
		{Path: "src/main.go", Kind: "file", Mode: "100644", ObjectSHA: "aa", SizeBytes: 7},
	}
	if err := c.GitWorkspaceFSPutTreeIndex(ctx, ws.WorkspaceID, fsTestCommit, nodes); err != nil {
		t.Fatal(err)
	}
	back, err := c.GitWorkspaceFSGetTreeIndex(ctx, ws.WorkspaceID)
	if err != nil || len(back) != 2 {
		t.Fatalf("tree index roundtrip = %v %v", back, err)
	}
	if back[1].Path != "src/main.go" || back[1].ObjectSHA != "aa" || back[1].SizeBytes != 7 {
		t.Fatalf("tree index entry = %+v", back[1])
	}
}

func TestGitWorkspaceFSPacksAndGitStateRoundtrip(t *testing.T) {
	c, _ := newFSClient(t)
	ctx := context.Background()
	ws, err := c.GitWorkspaceFSUpsert(ctx, GitWorkspaceRequest{RootPath: "/repo", HeadCommit: fsTestCommit})
	if err != nil {
		t.Fatal(err)
	}

	packID, err := c.GitWorkspaceFSPutObjectPack(ctx, ws.WorkspaceID, []byte("PACKDATA"))
	if err != nil || packID == "" {
		t.Fatalf("put pack = %q %v", packID, err)
	}
	packs, err := c.GitWorkspaceFSListObjectPacks(ctx, ws.WorkspaceID)
	if err != nil || len(packs) != 1 || packs[0].PackID != packID {
		t.Fatalf("packs = %v %v", packs, err)
	}
	got, err := c.GitWorkspaceFSGetObjectPack(ctx, ws.WorkspaceID, packID)
	if err != nil || string(got.Content) != "PACKDATA" {
		t.Fatalf("pack get = %v %v", got, err)
	}
	// Content addressing: a retried upload of the same bytes dedupes.
	if _, err := c.GitWorkspaceFSPutObjectPack(ctx, ws.WorkspaceID, []byte("PACKDATA")); err != nil {
		t.Fatal(err)
	}
	if packs, _ = c.GitWorkspaceFSListObjectPacks(ctx, ws.WorkspaceID); len(packs) != 1 {
		t.Fatalf("deduped packs = %d", len(packs))
	}

	if err := c.GitWorkspaceFSPutGitState(ctx, ws.WorkspaceID, fsTestCommit, []byte("ARCHIVE")); err != nil {
		t.Fatal(err)
	}
	state, err := c.GitWorkspaceFSGetGitState(ctx, ws.WorkspaceID)
	if err != nil || state == nil || string(state.Content) != "ARCHIVE" || state.StorageType != "tar.gz-no-objects" {
		t.Fatalf("git state = %+v %v", state, err)
	}
	// A tampered body fails the checksum: plant different bytes at the
	// name the pointer selects.
	if err := writeGitStateBody(t, c, ws.WorkspaceID, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", []byte("TAMPERED")); err != nil {
		t.Fatal(err)
	}
	good := state.ChecksumSHA256
	state2, err := c.GitWorkspaceFSGetGitState(ctx, ws.WorkspaceID)
	if err == nil || state2 != nil {
		t.Fatalf("pointing at tampered body not detected: %v", err)
	}
	_ = good
}

func TestGitWorkspaceFSTreeFileUpload(t *testing.T) {
	c, b := newFSClient(t)
	ctx := context.Background()
	ws, err := c.GitWorkspaceFSUpsert(ctx, GitWorkspaceRequest{RootPath: "/repo", HeadCommit: fsTestCommit})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.GitWorkspaceFSPutTreeFile(ctx, ws.WorkspaceID, fsTestCommit, "src/main.go", strings.NewReader("package main"), 12); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node(gitws.TreePath(ws.WorkspaceID, fsTestCommit, "src/main.go")); !ok || string(n.Data) != "package main" {
		t.Fatalf("tree file = %q %v", n.Data, ok)
	}
	if err := c.GitWorkspaceFSPutTreeSymlink(ctx, ws.WorkspaceID, fsTestCommit, "link", "/target"); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node(gitws.TreePath(ws.WorkspaceID, fsTestCommit, "link")); !ok || n.Mode&0o170000 == 0 {
		t.Fatalf("tree symlink missing or not a symlink: %+v", n)
	}
}

func writeRawJSON(t *testing.T, c *Client, path string, v any) error {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return c.WriteCtx(context.Background(), path, body)
}

// writeGitStateBody plants one archive body under the store directly.
func writeGitStateBody(t *testing.T, c *Client, id, checksum string, content []byte) error {
	t.Helper()
	body, err := json.Marshal(gitws.GitStateMeta{
		CheckpointCommit: fsTestCommit,
		StorageType:      gitws.GitStateStorageTarGzNoObjects,
		ChecksumSHA256:   checksum,
		SizeBytes:        int64(len(content)),
		CreatedAt:        gitws.NowUTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRawJSON(t, c, gitws.GitStateMetaPath(id), body); err != nil {
		return err
	}
	return c.WriteCtx(context.Background(), gitws.GitStateBodyPath(id, checksum), content)
}
