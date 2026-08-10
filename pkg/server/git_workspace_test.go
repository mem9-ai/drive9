package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func ensureGitWorkspaceSchema(t *testing.T, s *Server) {
	t.Helper()
	for _, stmt := range schema.GitWorkspaceTiDBSchemaStatements() {
		if _, err := s.fallback.Store().DB().Exec(stmt); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate column") {
				continue
			}
			t.Fatal(err)
		}
	}
}

func writeGitWorkspaceIndexForTest(t *testing.T, s *Server, body []byte) {
	t.Helper()
	ctx := context.Background()
	// Ensure parent dirs exist via unconditional writes of empty placeholders is
	// not needed: WriteCtxIfRevision creates intermediate path nodes.
	if _, _, err := s.fallback.WriteCtxIfRevisionWithTagsResult(
		ctx,
		gitWorkspaceIndexPath,
		body,
		0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
		-1,
		nil,
		"",
	); err != nil {
		t.Fatalf("seed git workspace index: %v", err)
	}
}

func TestGitWorkspaceDeleteRemovesIndexEntry(t *testing.T) {
	s := newTestServer(t)
	ensureGitWorkspaceSchema(t, s)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	c := client.New(ts.URL, "")
	ctx := context.Background()
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   "/repo/",
		RepoURL:    "https://example.test/repo.git",
		RemoteName: "origin",
		HeadCommit: "1111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}

	idxBody, err := json.MarshalIndent(gitWorkspaceIndexDoc{
		Version: 1,
		Workspaces: []gitWorkspaceIndexDocEntry{
			{WorkspaceID: ws.WorkspaceID, RootPath: "/repo/"},
			{WorkspaceID: "ws-other", RootPath: "/other/"},
		},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeGitWorkspaceIndexForTest(t, s, idxBody)

	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace: %v", err)
	}

	data, err := s.fallback.ReadCtx(ctx, gitWorkspaceIndexPath, 0, -1)
	if err != nil {
		t.Fatalf("read index after delete: %v", err)
	}
	var idx gitWorkspaceIndexDoc
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("parse index: %v", err)
	}
	if len(idx.Workspaces) != 1 || idx.Workspaces[0].WorkspaceID != "ws-other" {
		t.Fatalf("index after delete = %+v, want only ws-other", idx.Workspaces)
	}
}

func TestGitWorkspaceDeleteIdempotentCleansStaleIndex(t *testing.T) {
	// Convergent repair: workspace row already non-live, index still lists it.
	// A second DELETE must succeed and remove the stale index entry.
	s := newTestServer(t)
	ensureGitWorkspaceSchema(t, s)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	c := client.New(ts.URL, "")
	ctx := context.Background()
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   "/repo2/",
		RepoURL:    "https://example.test/repo2.git",
		RemoteName: "origin",
		HeadCommit: "2222222222222222222222222222222222222222",
	})
	if err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	// Soft-delete row only (bypass handler) to simulate partial prior failure.
	if err := s.fallback.Store().DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("store.DeleteGitWorkspace: %v", err)
	}
	idxBody, err := json.MarshalIndent(gitWorkspaceIndexDoc{
		Version: 1,
		Workspaces: []gitWorkspaceIndexDocEntry{
			{WorkspaceID: ws.WorkspaceID, RootPath: "/repo2/"},
		},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeGitWorkspaceIndexForTest(t, s, idxBody)

	// API DELETE must be idempotent and finish index cleanup.
	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace repair: %v", err)
	}
	data, err := s.fallback.ReadCtx(ctx, gitWorkspaceIndexPath, 0, -1)
	if err != nil {
		t.Fatalf("read index after repair delete: %v", err)
	}
	var idx gitWorkspaceIndexDoc
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("parse index: %v", err)
	}
	if len(idx.Workspaces) != 0 {
		t.Fatalf("index after repair = %+v, want empty workspaces", idx.Workspaces)
	}

	// Third call still OK (fully clean).
	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace already clean: %v", err)
	}
}


func TestGitWorkspaceDeleteWithoutIndexIsOK(t *testing.T) {
	// Old deployments / never --fast: no index file. DELETE must still succeed.
	s := newTestServer(t)
	ensureGitWorkspaceSchema(t, s)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	c := client.New(ts.URL, "")
	ctx := context.Background()
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   "/repo3/",
		RepoURL:    "https://example.test/repo3.git",
		RemoteName: "origin",
		HeadCommit: "3333333333333333333333333333333333333333",
	})
	if err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace without index: %v", err)
	}
}

func TestGitWorkspaceDeleteIndexFailureStillReturns200(t *testing.T) {
	// Soft-delete is the durable state; index is an arming hint. A permanent
	// index parse failure must not turn DELETE into 500/EIO for the caller.
	s := newTestServer(t)
	ensureGitWorkspaceSchema(t, s)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	c := client.New(ts.URL, "")
	ctx := context.Background()
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   "/repo-bad-idx/",
		RepoURL:    "https://example.test/repo-bad-idx.git",
		RemoteName: "origin",
		HeadCommit: "4444444444444444444444444444444444444444",
	})
	if err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	writeGitWorkspaceIndexForTest(t, s, []byte("{"))

	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace with bad index: %v (want 200)", err)
	}
	got, err := s.fallback.Store().GetGitWorkspace(ctx, ws.WorkspaceID)
	if err != nil {
		t.Fatalf("GetGitWorkspace after DELETE: %v", err)
	}
	if got.Status != datastore.GitWorkspaceStatusDeleted {
		t.Fatalf("workspace status = %q, want %q", got.Status, datastore.GitWorkspaceStatusDeleted)
	}
	// Idempotent retry still 200 (cleanup re-attempted, still fails parse, still 200).
	if err := c.DeleteGitWorkspace(ctx, ws.WorkspaceID); err != nil {
		t.Fatalf("DeleteGitWorkspace retry: %v", err)
	}
}

func TestGitWorkspaceUpsertRejectsSelfLinkedWorkspace(t *testing.T) {
	s := newTestServer(t)
	ensureGitWorkspaceSchema(t, s)
	ts := httptest.NewServer(s)
	defer ts.Close()

	c := client.New(ts.URL, "")
	mainWS, err := c.UpsertGitWorkspace(context.Background(), client.GitWorkspaceRequest{
		RootPath:   "/repo/",
		RepoURL:    "https://example.test/repo.git",
		RemoteName: "origin",
		HeadCommit: "1111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("UpsertGitWorkspace main: %v", err)
	}

	body, err := json.Marshal(client.GitWorkspaceRequest{
		RootPath:          "/repo/",
		RepoURL:           "https://example.test/repo.git",
		RemoteName:        "origin",
		HeadCommit:        "1111111111111111111111111111111111111111",
		WorkspaceKind:     "linked",
		CommonWorkspaceID: mainWS.WorkspaceID,
		WorktreeName:      "repo-wt",
		GitDirRel:         "worktrees/repo-wt",
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ts.URL+"/v1/git-workspaces", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", resp.StatusCode, gotBody)
	}
	if !strings.Contains(string(gotBody), "cannot reference itself") {
		t.Fatalf("body = %s, want self-link error", gotBody)
	}
}
