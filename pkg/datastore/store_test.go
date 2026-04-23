package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	initDatastoreSchema(t, testDSN)
	testmysql.ResetDB(t, s.DB())
	t.Cleanup(func() { _ = s.Close() })
	return s
}

var seq int

func genID() string {
	seq++
	return fmt.Sprintf("id-%04d", seq)
}

func setEmbeddingRevision(t *testing.T, s *Store, fileID string, revision int64) {
	t.Helper()
	if _, err := s.DB().Exec(`UPDATE files SET embedding_revision = ? WHERE file_id = ?`, revision, fileID); err != nil {
		t.Fatalf("set embedding_revision for %s: %v", fileID, err)
	}
}

func requireEmbeddingRevision(t *testing.T, got *int64, want int64) {
	t.Helper()
	if got == nil {
		t.Fatalf("embedding_revision=nil, want %d", want)
	}
	if *got != want {
		t.Fatalf("embedding_revision=%d, want %d", *got, want)
	}
}

func TestInsertAndGetNode(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	node := &FileNode{
		NodeID: "n1", Path: "/data/file.txt", ParentPath: "/data/",
		Name: "file.txt", FileID: "f1", CreatedAt: now,
	}
	if err := s.InsertNode(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetNode(context.Background(), "/data/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "n1" || got.FileID != "f1" || got.Name != "file.txt" {
		t.Errorf("unexpected node: %+v", got)
	}
}

func TestGetNodeNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetNode(context.Background(), "/nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestInsertAndGetFile(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	f := &File{
		FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		ContentType: "text/plain", SizeBytes: 100, Revision: 1,
		Status: StatusConfirmed, ContentText: "hello world",
		CreatedAt: now, ConfirmedAt: &now,
	}
	if err := s.InsertFile(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 7)
	got, err := s.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.StorageType != StorageDB9 || got.SizeBytes != 100 || got.ContentText != "hello world" {
		t.Errorf("unexpected file: %+v", got)
	}
	requireEmbeddingRevision(t, got.EmbeddingRevision, 7)
}

func TestStat(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 42, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 11)
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/a.txt", ParentPath: "/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	nf, err := s.Stat(context.Background(), "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Node.Path != "/a.txt" || nf.File == nil || nf.File.SizeBytes != 42 {
		t.Errorf("unexpected stat: node=%+v file=%+v", nf.Node, nf.File)
	}
	requireEmbeddingRevision(t, nf.File.EmbeddingRevision, 11)
}

func TestListDir(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d1", Path: "/data/", ParentPath: "/", Name: "data", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 13)
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/data/a.txt", ParentPath: "/data/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d2", Path: "/data/sub/", ParentPath: "/data/", Name: "sub", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	entries, err := s.ListDir(context.Background(), "/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Node.Name != "a.txt" || entries[1].Node.Name != "sub" {
		t.Errorf("unexpected entries: %+v, %+v", entries[0].Node, entries[1].Node)
	}
	if entries[0].File == nil {
		t.Fatal("expected file entry for a.txt")
	}
	requireEmbeddingRevision(t, entries[0].File.EmbeddingRevision, 13)
}

func TestUpdateFileSearchText(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    2,
		Status:      StatusConfirmed,
		ContentText: "old text",
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateFileSearchText(context.Background(), "f1", 2, "new text")
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("expected revision-gated update to succeed")
	}
	if got := mustFile(t, s, "f1").ContentText; got != "new text" {
		t.Fatalf("content_text=%q, want %q", got, "new text")
	}
}

func TestUpdateFileSearchTextRejectsStaleRevision(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    3,
		Status:      StatusConfirmed,
		ContentText: "old text",
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateFileSearchText(context.Background(), "f1", 2, "new text")
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("stale revision should not update content_text")
	}
	if got := mustFile(t, s, "f1").ContentText; got != "old text" {
		t.Fatalf("content_text=%q, want %q", got, "old text")
	}
}

func TestUpdateFileSearchTextTxRollsBackWithOuterTransaction(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    2,
		Status:      StatusConfirmed,
		ContentText: "old text",
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		updated, err := s.UpdateFileSearchTextTx(tx, "f1", 2, "new text")
		if err != nil {
			return err
		}
		if !updated {
			t.Fatal("expected transactional search-text update to succeed")
		}
		return context.Canceled
	})
	if err != context.Canceled {
		t.Fatalf("rollback error=%v, want %v", err, context.Canceled)
	}
	if got := mustFile(t, s, "f1").ContentText; got != "old text" {
		t.Fatalf("content_text=%q, want %q after rollback", got, "old text")
	}
}

func TestReplaceFileTagsTxAndGetFileTags(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f-tags",
		StorageType: StorageDB9,
		StorageRef:  "inline",
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, "f-tags", map[string]string{
			"owner": "alice",
			"topic": "cat",
		})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx(initial): %v", err)
	}

	tags, err := s.GetFileTags(context.Background(), "f-tags")
	if err != nil {
		t.Fatalf("GetFileTags(initial): %v", err)
	}
	if tags["owner"] != "alice" || tags["topic"] != "cat" || len(tags) != 2 {
		t.Fatalf("initial tags = %+v, want owner/topic", tags)
	}

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, "f-tags", map[string]string{
			"owner": "bob",
		})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx(replace): %v", err)
	}
	tags, err = s.GetFileTags(context.Background(), "f-tags")
	if err != nil {
		t.Fatalf("GetFileTags(replace): %v", err)
	}
	if tags["owner"] != "bob" || len(tags) != 1 {
		t.Fatalf("replaced tags = %+v, want only owner=bob", tags)
	}

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, "f-tags", map[string]string{})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx(clear): %v", err)
	}
	tags, err = s.GetFileTags(context.Background(), "f-tags")
	if err != nil {
		t.Fatalf("GetFileTags(clear): %v", err)
	}
	if len(tags) != 0 {
		t.Fatalf("cleared tags = %+v, want empty", tags)
	}
}

func TestZeroCopyCp(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageS3, StorageRef: "blobs/f1",
		SizeBytes: 1000000, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/a.bin", ParentPath: "/", Name: "a.bin", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n2", Path: "/b.bin", ParentPath: "/", Name: "b.bin", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	count, err := s.RefCount(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected refcount 2, got %d", count)
	}
}

func TestDeleteWithRefCount(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 50, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 17)
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/a.txt", ParentPath: "/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n2", Path: "/b.txt", ParentPath: "/", Name: "b.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	deleted, err := s.DeleteFileWithRefCheck(context.Background(), "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Error("expected nil (file should survive, refcount > 0)")
	}

	deleted, err = s.DeleteFileWithRefCheck(context.Background(), "/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if deleted == nil || deleted.Status != StatusDeleted {
		t.Errorf("expected DELETED file record, got %+v", deleted)
	}
	requireEmbeddingRevision(t, deleted.EmbeddingRevision, 17)
}

func TestDeleteDirRecursive(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d1", Path: "/data/", ParentPath: "/", Name: "data", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 19)
	if err := s.InsertFile(context.Background(), &File{FileID: "f2", StorageType: StorageDB9, StorageRef: "/blobs/f2",
		SizeBytes: 20, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f2", 23)
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/data/a.txt", ParentPath: "/data/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n2", Path: "/data/b.txt", ParentPath: "/data/", Name: "b.txt", FileID: "f2", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n3", Path: "/shared.txt", ParentPath: "/", Name: "shared.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	orphaned, err := s.DeleteDirRecursive(context.Background(), "/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(orphaned) != 1 || orphaned[0].FileID != "f2" {
		t.Fatalf("expected 1 orphaned (f2), got %d", len(orphaned))
	}
	requireEmbeddingRevision(t, orphaned[0].EmbeddingRevision, 23)

	_, err = s.GetNode(context.Background(), "/data/")
	if err != ErrNotFound {
		t.Error("expected /data/ deleted")
	}
	_, err = s.GetNode(context.Background(), "/shared.txt")
	if err != nil {
		t.Error("expected /shared.txt to survive")
	}
}

func TestEnsureParentDirs(t *testing.T) {
	s := newTestStore(t)
	if err := s.EnsureParentDirs(context.Background(), "/a/b/c/file.txt", genID); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/a/", "/a/b/", "/a/b/c/"} {
		n, err := s.GetNode(context.Background(), p)
		if err != nil {
			t.Errorf("expected dir at %s: %v", p, err)
			continue
		}
		if !n.IsDirectory {
			t.Errorf("expected %s to be directory", p)
		}
	}
	// Idempotent
	if err := s.EnsureParentDirs(context.Background(), "/a/b/c/file.txt", genID); err != nil {
		t.Fatal(err)
	}
}

func TestRenameFile(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/old.txt", ParentPath: "/", Name: "old.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	if err := s.UpdateNodePath(context.Background(), "/old.txt", "/new.txt", "/", "new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetNode(context.Background(), "/old.txt"); err != ErrNotFound {
		t.Error("old path should be gone")
	}
	got, _ := s.GetNode(context.Background(), "/new.txt")
	if got.Name != "new.txt" || got.FileID != "f1" {
		t.Errorf("unexpected: %+v", got)
	}
}

func TestRenameDir(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d1", Path: "/old/", ParentPath: "/", Name: "old", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/old/a.txt", ParentPath: "/old/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d2", Path: "/old/sub/", ParentPath: "/old/", Name: "sub", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n2", Path: "/old/sub/b.txt", ParentPath: "/old/sub/", Name: "b.txt", FileID: "f2", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	count, err := s.RenameDir(context.Background(), "/old/", "/new/")
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Errorf("expected 4 updated, got %d", count)
	}
	if _, err := s.GetNode(context.Background(), "/old/"); err != ErrNotFound {
		t.Error("/old/ should be gone")
	}
	for _, p := range []string{"/new/", "/new/a.txt", "/new/sub/", "/new/sub/b.txt"} {
		if _, err := s.GetNode(context.Background(), p); err != nil {
			t.Errorf("expected %s: %v", p, err)
		}
	}
}

func TestUpdateFileContent(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}

	newRev, err := s.UpdateFileContent(context.Background(), "f1", StorageDB9, "/blobs/f1-v2", "text/plain", "abc123", "new content", []byte("blob"), 42, "")
	if err != nil {
		t.Fatal(err)
	}
	if newRev != 2 {
		t.Errorf("expected newRev=2, got %d", newRev)
	}
	got, _ := s.GetFile(context.Background(), "f1")
	if got.Revision != 2 || got.SizeBytes != 42 || got.ContentText != "new content" {
		t.Errorf("unexpected: %+v", got)
	}
}
