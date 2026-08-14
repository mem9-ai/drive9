package datastore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
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
	initGitWorkspaceTestSchema(t, s)
	ctx := context.Background()
	if err := s.UpsertGitWorkspace(ctx, GitWorkspace{
		WorkspaceID: "ws1",
		RootPath:    "/repo-pack/",
		RepoURL:     "https://example.test/repo.git",
		RemoteName:  "origin",
		HeadCommit:  strings.Repeat("1", 40),
		Status:      GitWorkspaceStatusLive,
	}); err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
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

func TestDeleteGitWorkspaceWipesSubresourcesAndFencesWrites(t *testing.T) {
	s := newTestStore(t)
	initGitWorkspaceTestSchema(t, s)
	ctx := context.Background()
	const wsID = "ws-del"
	if err := s.UpsertGitWorkspace(ctx, GitWorkspace{
		WorkspaceID: wsID,
		RootPath:    "/repo-del/",
		RepoURL:     "https://example.test/repo-del.git",
		RemoteName:  "origin",
		HeadCommit:  strings.Repeat("a", 40),
	}); err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	if err := s.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
		WorkspaceID: wsID,
		Path:        "dirty.txt",
		Op:          GitOverlayOpUpsert,
		Kind:        GitOverlayKindFile,
		ContentBlob: []byte("stale"),
	}); err != nil {
		t.Fatalf("UpsertGitOverlayEntry: %v", err)
	}
	if err := s.UpsertGitState(ctx, GitState{WorkspaceID: wsID, CheckpointCommit: strings.Repeat("a", 40)}); err != nil {
		t.Fatalf("UpsertGitState: %v", err)
	}
	if err := s.ReplaceGitTreeNodes(ctx, wsID, strings.Repeat("a", 40), []GitTreeNode{{
		Path: "README.md", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: strings.Repeat("b", 40),
	}}); err != nil {
		t.Fatalf("ReplaceGitTreeNodes: %v", err)
	}
	if err := s.UpsertGitObjectPack(ctx, GitObjectPack{
		WorkspaceID: wsID, PackID: strings.Repeat("c", 64), ChecksumSHA256: strings.Repeat("c", 64), ContentBlob: []byte("P"),
	}); err != nil {
		t.Fatalf("UpsertGitObjectPack: %v", err)
	}

	if err := s.DeleteGitWorkspace(ctx, wsID); err != nil {
		t.Fatalf("DeleteGitWorkspace: %v", err)
	}
	if _, err := s.GetGitOverlayEntry(ctx, wsID, "dirty.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("overlay after delete: %v, want NotFound", err)
	}
	if _, err := s.GetGitState(ctx, wsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("git-state after delete: %v, want NotFound", err)
	}
	if _, err := s.ListGitTreeNodes(ctx, wsID, strings.Repeat("a", 40)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tree after delete: %v, want NotFound", err)
	}
	if _, err := s.GetGitObjectPack(ctx, wsID, strings.Repeat("c", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pack after delete: %v, want NotFound", err)
	}
	if err := s.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
		WorkspaceID: wsID, Path: "after-delete.txt", Op: GitOverlayOpUpsert, Kind: GitOverlayKindFile,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("overlay write after delete: %v, want NotFound", err)
	}

	const newID = "ws-del-new"
	if err := s.ReplaceDeletedGitWorkspace(ctx, wsID, GitWorkspace{
		WorkspaceID: newID,
		RootPath:    "/repo-del/",
		RepoURL:     "https://example.test/repo-del.git",
		RemoteName:  "origin",
		HeadCommit:  strings.Repeat("d", 40),
	}); err != nil {
		t.Fatalf("ReplaceDeletedGitWorkspace: %v", err)
	}
	if _, err := s.GetGitWorkspace(ctx, wsID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old id after replace: %v, want NotFound", err)
	}
	got, err := s.GetGitWorkspaceByRoot(ctx, "/repo-del/")
	if err != nil {
		t.Fatalf("GetGitWorkspaceByRoot after replace: %v", err)
	}
	if got.WorkspaceID != newID || got.Status != GitWorkspaceStatusLive {
		t.Fatalf("revived workspace = %+v, want id=%s live", got, newID)
	}
	if _, err := s.GetGitOverlayEntry(ctx, newID, "dirty.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recreate inherited overlay: %v, want NotFound", err)
	}
	if err := s.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
		WorkspaceID: wsID, Path: "stale-runtime.txt", Op: GitOverlayOpUpsert, Kind: GitOverlayKindFile,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old-generation overlay write: %v, want NotFound", err)
	}
}

func TestDeleteGitWorkspaceConcurrentWithOverlayWrite(t *testing.T) {
	s := newTestStore(t)
	initGitWorkspaceTestSchema(t, s)
	ctx := context.Background()
	const wsID = "ws-race"
	if err := s.UpsertGitWorkspace(ctx, GitWorkspace{
		WorkspaceID: wsID,
		RootPath:    "/repo-race/",
		RepoURL:     "https://example.test/repo-race.git",
		RemoteName:  "origin",
		HeadCommit:  strings.Repeat("e", 40),
	}); err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
				WorkspaceID: wsID,
				Path:        "race.txt",
				Op:          GitOverlayOpUpsert,
				Kind:        GitOverlayKindFile,
				ContentBlob: []byte{byte(i)},
			})
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.DeleteGitWorkspace(ctx, wsID); err != nil {
			t.Errorf("DeleteGitWorkspace: %v", err)
		}
	}()
	wg.Wait()

	got, err := s.GetGitWorkspace(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitWorkspace: %v", err)
	}
	if got.Status != GitWorkspaceStatusDeleted {
		t.Fatalf("status = %q, want deleted", got.Status)
	}
	if _, err := s.GetGitOverlayEntry(ctx, wsID, "race.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("overlay after concurrent delete: %v, want NotFound", err)
	}

	if err := s.UpsertGitWorkspace(ctx, GitWorkspace{
		WorkspaceID: wsID,
		RootPath:    "/repo-race/",
		RepoURL:     "https://example.test/repo-race.git",
		RemoteName:  "origin",
		HeadCommit:  strings.Repeat("f", 40),
		Status:      GitWorkspaceStatusLive,
	}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if _, err := s.GetGitOverlayEntry(ctx, wsID, "race.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recreate inherited concurrent overlay: %v, want NotFound", err)
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
