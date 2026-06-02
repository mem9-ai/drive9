package datastore

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

func TestGitWorkspaceLinkedMetadataRoundTrip(t *testing.T) {
	s := newTestStore(t)
	initGitWorkspaceTestSchema(t, s)
	ctx := context.Background()
	main := GitWorkspace{
		WorkspaceID: "base",
		RootPath:    "/repo/",
		RepoURL:     "https://github.com/example/repo.git",
		RemoteName:  "origin",
		BranchName:  "main",
		BaseCommit:  strings.Repeat("1", 40),
		HeadCommit:  strings.Repeat("1", 40),
		Mode:        GitWorkspaceModeFast,
		Kind:        GitWorkspaceKindMain,
	}
	if err := s.UpsertGitWorkspace(ctx, main); err != nil {
		t.Fatalf("UpsertGitWorkspace main: %v", err)
	}
	linked := GitWorkspace{
		WorkspaceID:  "wt",
		RootPath:     "/repo-wt/",
		RepoURL:      main.RepoURL,
		RemoteName:   "origin",
		BranchName:   "feature",
		BaseCommit:   strings.Repeat("2", 40),
		HeadCommit:   strings.Repeat("2", 40),
		Mode:         GitWorkspaceModeFastBlobless,
		Kind:         GitWorkspaceKindLinked,
		CommonID:     main.WorkspaceID,
		WorktreeName: "wt",
		GitDirRel:    "worktrees/wt",
	}
	if err := s.UpsertGitWorkspace(ctx, linked); err != nil {
		t.Fatalf("UpsertGitWorkspace linked: %v", err)
	}
	got, err := s.GetGitWorkspaceByRoot(ctx, "/repo-wt")
	if err != nil {
		t.Fatalf("GetGitWorkspaceByRoot linked: %v", err)
	}
	if got.Kind != GitWorkspaceKindLinked || got.CommonID != "base" || got.WorktreeName != "wt" || got.GitDirRel != "worktrees/wt" {
		t.Fatalf("linked metadata = %+v", got)
	}
}

func TestGitObjectPackRoundTrip(t *testing.T) {
	s := newTestStore(t)
	initGitObjectPackTestSchema(t, s)
	ctx := context.Background()
	content := []byte("PACK test")
	pack := GitObjectPack{
		WorkspaceID:    "ws1",
		PackID:         "pack1",
		ChecksumSHA256: "sum1",
		SizeBytes:      int64(len(content)),
		ContentBlob:    content,
	}
	if err := s.UpsertGitObjectPack(ctx, pack); err != nil {
		t.Fatalf("UpsertGitObjectPack: %v", err)
	}
	got, err := s.GetGitObjectPack(ctx, "ws1", "pack1")
	if err != nil {
		t.Fatalf("GetGitObjectPack: %v", err)
	}
	if got.WorkspaceID != "ws1" || got.PackID != "pack1" || got.ChecksumSHA256 != "sum1" || got.SizeBytes != int64(len(content)) {
		t.Fatalf("unexpected pack metadata: %+v", got)
	}
	if !bytes.Equal(got.ContentBlob, content) {
		t.Fatalf("pack content = %q, want %q", got.ContentBlob, content)
	}
	packs, err := s.ListGitObjectPacks(ctx, "ws1")
	if err != nil {
		t.Fatalf("ListGitObjectPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackID != "pack1" || len(packs[0].ContentBlob) != 0 {
		t.Fatalf("listed packs = %+v, want metadata only", packs)
	}
}

func initGitWorkspaceTestSchema(t *testing.T, s *Store) {
	t.Helper()
	for _, stmt := range schema.GitWorkspaceTiDBSchemaStatements() {
		if _, err := s.DB().Exec(stmt); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exist") {
				continue
			}
			t.Fatal(err)
		}
	}
}

func initGitObjectPackTestSchema(t *testing.T, s *Store) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS git_workspace_object_packs (
			workspace_id    VARCHAR(64) NOT NULL,
			pack_id         VARCHAR(64) NOT NULL,
			checksum_sha256 VARCHAR(128) NOT NULL DEFAULT '',
			size_bytes      BIGINT NOT NULL DEFAULT 0,
			content_blob    LONGBLOB,
			created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (workspace_id, pack_id)
		)`,
		`CREATE INDEX idx_git_object_packs_created ON git_workspace_object_packs(workspace_id, created_at)`,
	}
	for _, stmt := range stmts {
		if _, err := s.DB().Exec(stmt); err != nil {
			if strings.Contains(err.Error(), "Duplicate key name") {
				continue
			}
			t.Fatal(err)
		}
	}
}
