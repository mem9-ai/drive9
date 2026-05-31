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

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

func TestGitWorkspaceUpsertRejectsSelfLinkedWorkspace(t *testing.T) {
	s := newTestServer(t)
	for _, stmt := range schema.GitWorkspaceTiDBSchemaStatements() {
		if _, err := s.fallback.Store().DB().Exec(stmt); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate column") {
				continue
			}
			t.Fatal(err)
		}
	}
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
	gotBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", resp.StatusCode, gotBody)
	}
	if !strings.Contains(string(gotBody), "cannot reference itself") {
		t.Fatalf("body = %s, want self-link error", gotBody)
	}
}
