package datastore

import (
	"bytes"
	"context"
	"testing"
)

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
			t.Fatal(err)
		}
	}
}
