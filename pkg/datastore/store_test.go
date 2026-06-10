package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	initDatastoreSchema(t, testDSN)
	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := s.DB().Exec(`UPDATE semantic SET embedding_revision = ? WHERE inode_id = ?`, revision, fileID); err != nil {
		t.Fatalf("set semantic embedding_revision for %s: %v", fileID, err)
	}
}

func requireEmbeddingRevision(t *testing.T, got *int64, want int64) {
	t.Helper()
	if got == nil {
		t.Fatalf("embedding_revision=nil, want %d", want)
		return
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

func TestStatLite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	// Insert a file with content_blob and content_text populated.
	if err := s.InsertFile(ctx, &File{
		FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		ContentBlob: []byte("hello world"), ContentText: "hello world",
		SizeBytes: 11, Revision: 3, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE files SET description = 'test desc' WHERE file_id = 'f1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE semantic SET description = 'test desc' WHERE inode_id = 'f1'`); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/lite.txt", ParentPath: "/", Name: "lite.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	// StatLite should return metadata but NOT blob/text/description.
	nf, err := s.StatLite(ctx, "/lite.txt")
	if err != nil {
		t.Fatalf("StatLite: %v", err)
	}
	if nf.Node.Path != "/lite.txt" {
		t.Fatalf("path=%q, want /lite.txt", nf.Node.Path)
	}
	if nf.File == nil {
		t.Fatal("StatLite: file is nil")
	}
	if nf.File.SizeBytes != 11 {
		t.Fatalf("size=%d, want 11", nf.File.SizeBytes)
	}
	if nf.File.Revision != 3 {
		t.Fatalf("revision=%d, want 3", nf.File.Revision)
	}
	if nf.File.ConfirmedAt == nil {
		t.Fatal("confirmed_at is nil")
	}
	// These fields must be zero/empty — lite path does not fetch them.
	if len(nf.File.ContentBlob) != 0 {
		t.Fatalf("ContentBlob should be empty in lite path, got %d bytes", len(nf.File.ContentBlob))
	}
	if nf.File.ContentText != "" {
		t.Fatalf("ContentText should be empty in lite path, got %q", nf.File.ContentText)
	}
	if nf.File.Description != "" {
		t.Fatalf("Description should be empty in lite path, got %q", nf.File.Description)
	}
	if nf.File.StorageType != "" {
		t.Fatalf("StorageType should be empty in lite path, got %q", nf.File.StorageType)
	}
}

func TestStatPathFallbackLite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertFile(ctx, &File{
		FileID: "f2", StorageType: StorageDB9, StorageRef: "/blobs/f2",
		ContentBlob: []byte("data"), SizeBytes: 4, Revision: 7,
		Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n2", Path: "/fb.txt", ParentPath: "/", Name: "fb.txt", FileID: "f2", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	// Primary path match.
	nf, err := s.StatPathFallbackLite(ctx, "/fb.txt", "/nonexist")
	if err != nil {
		t.Fatalf("StatPathFallbackLite: %v", err)
	}
	if nf.File == nil || nf.File.Revision != 7 {
		t.Fatalf("unexpected file: %+v", nf.File)
	}
	if len(nf.File.ContentBlob) != 0 {
		t.Fatal("lite fallback should not return ContentBlob")
	}

	// Fallback path match.
	nf2, err := s.StatPathFallbackLite(ctx, "/nonexist", "/fb.txt")
	if err != nil {
		t.Fatalf("StatPathFallbackLite fallback: %v", err)
	}
	if nf2.File == nil || nf2.File.Revision != 7 {
		t.Fatalf("fallback: unexpected file: %+v", nf2.File)
	}
}

func TestStatForRead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertFile(ctx, &File{
		FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		ContentBlob: []byte("inline data here"), SizeBytes: 16, Revision: 5,
		Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/read.txt", ParentPath: "/", Name: "read.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	nf, err := s.StatForRead(ctx, "/read.txt")
	if err != nil {
		t.Fatalf("StatForRead: %v", err)
	}
	if nf.File == nil {
		t.Fatal("file is nil")
	}
	// StatForRead MUST return storage_type + content_blob for inline reads.
	if nf.File.StorageType != StorageDB9 {
		t.Fatalf("storage_type=%q, want db9", nf.File.StorageType)
	}
	if string(nf.File.ContentBlob) != "inline data here" {
		t.Fatalf("content_blob=%q, want 'inline data here'", nf.File.ContentBlob)
	}
	if nf.File.SizeBytes != 16 {
		t.Fatalf("size=%d, want 16", nf.File.SizeBytes)
	}
	if nf.File.Revision != 5 {
		t.Fatalf("revision=%d, want 5", nf.File.Revision)
	}
	// But it should NOT return text/description/embedding fields.
	if nf.File.ContentText != "" {
		t.Fatalf("ContentText should be empty, got %q", nf.File.ContentText)
	}
	if nf.File.Description != "" {
		t.Fatalf("Description should be empty, got %q", nf.File.Description)
	}
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

func TestInsertNodeRejectsRootDentry(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{"/", "root-alias"} {
		err := s.InsertNode(context.Background(), &FileNode{
			NodeID:      "root-" + name,
			Path:        "/",
			ParentPath:  "/",
			Name:        name,
			IsDirectory: true,
			CreatedAt:   time.Now(),
		})
		if !errors.Is(err, ErrInvalidRootDentry) {
			t.Fatalf("InsertNode root dentry name %q error = %v, want %v", name, err, ErrInvalidRootDentry)
		}
	}
}

func TestStoreRejectsRootPathWrites(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	err := s.InsertNodeTx(s.DB(), &FileNode{
		NodeID:      "root-tx",
		Path:        "/",
		ParentPath:  "/",
		Name:        "root-alias",
		IsDirectory: true,
		CreatedAt:   now,
	})
	if !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("InsertNodeTx root path error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if err := s.UpdateNodePath(context.Background(), "/src.txt", "/", "/", "root-alias"); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("UpdateNodePath root destination error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if err := s.UpdateNodePath(context.Background(), "/", "/dst.txt", "/", "dst.txt"); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("UpdateNodePath root source error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if _, err := s.RenameFileReplacingTarget(context.Background(), "/src.txt", "/", "/", "root-alias"); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("RenameFileReplacingTarget root destination error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if _, err := s.RenameFileReplacingTarget(context.Background(), "/", "/dst.txt", "/", "dst.txt"); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("RenameFileReplacingTarget root source error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if err := s.LinkFileNodeTx(context.Background(), s.DB(), "/src.txt", "/", "/", "root-alias", "link-root", now); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("LinkFileNodeTx root destination error = %v, want %v", err, ErrInvalidRootDentry)
	}
	if err := s.LinkFileNodeTx(context.Background(), s.DB(), "/", "/dst.txt", "/", "dst.txt", "link-dst", now); !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("LinkFileNodeTx root source error = %v, want %v", err, ErrInvalidRootDentry)
	}
}

func TestListDirSkipsHistoricalRootDentry(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if _, err := s.DB().Exec(`
		INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, created_at)
		VALUES (?, ?, ?, ?, 1, ?)`,
		"root-self", "/", "/", "root-alias", now); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{
		NodeID:      "d1",
		Path:        "/data/",
		ParentPath:  "/",
		Name:        "data",
		IsDirectory: true,
		CreatedAt:   now,
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := s.ListDir(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Node.Name != "data" {
		t.Fatalf("ListDir entries = %+v, want only data", entries)
	}
}

func TestListNodes(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertInode(context.Background(), &Inode{InodeID: "i1", SizeBytes: 0, Revision: 1, Mode: 0o755, Status: StatusConfirmed, CreatedAt: now, Mtime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d1", Path: "/data/", ParentPath: "/", Name: "data", IsDirectory: true, InodeID: "i1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/data/a.txt", ParentPath: "/data/", Name: "a.txt", FileID: "f1", InodeID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	nodes, err := s.ListNodes(context.Background(), "/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Name != "a.txt" {
		t.Errorf("name=%q, want a.txt", nodes[0].Name)
	}
	if nodes[0].InodeID != "f1" {
		t.Errorf("inode_id=%q, want f1", nodes[0].InodeID)
	}
}

func TestListNodesSkipsHistoricalRootDentry(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if _, err := s.DB().Exec(`
		INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, created_at)
		VALUES (?, ?, ?, ?, 1, ?)`,
		"root-self", "/", "/", "root-alias", now); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{
		NodeID:      "d1",
		Path:        "/data/",
		ParentPath:  "/",
		Name:        "data",
		IsDirectory: true,
		CreatedAt:   now,
	}); err != nil {
		t.Fatal(err)
	}

	nodes, err := s.ListNodes(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Name != "data" {
		t.Fatalf("ListNodes entries = %+v, want only data", nodes)
	}
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

func TestReplaceFileTagsByPrefixTxPreservesOtherTags(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f-prefix-tags",
		StorageType: StorageDB9,
		StorageRef:  "inline",
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, "f-prefix-tags", map[string]string{
			"album":                  "Inbox",
			"drive9.image.schema":    "old_schema",
			"drive9.image.tag.en.0":  "old",
			"drive9.thumbnail.ready": "true",
		})
	})
	if err != nil {
		t.Fatalf("ReplaceFileTagsTx(initial): %v", err)
	}

	err = s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsByPrefixTx(tx, "f-prefix-tags", "drive9.image.", map[string]string{
			"drive9.image.schema":   "structured_v1",
			"drive9.image.tag.en.0": "autumn road",
		})
	})
	if err != nil {
		t.Fatalf("ReplaceFileTagsByPrefixTx(replace): %v", err)
	}
	tags, err := s.GetFileTags(context.Background(), "f-prefix-tags")
	if err != nil {
		t.Fatalf("GetFileTags(replace): %v", err)
	}
	want := map[string]string{
		"album":                  "Inbox",
		"drive9.image.schema":    "structured_v1",
		"drive9.image.tag.en.0":  "autumn road",
		"drive9.thumbnail.ready": "true",
	}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags after prefix replace = %+v, want %+v", tags, want)
	}

	err = s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ReplaceFileTagsByPrefixTx(tx, "f-prefix-tags", "drive9.image.", nil)
	})
	if err != nil {
		t.Fatalf("ReplaceFileTagsByPrefixTx(clear): %v", err)
	}
	tags, err = s.GetFileTags(context.Background(), "f-prefix-tags")
	if err != nil {
		t.Fatalf("GetFileTags(clear): %v", err)
	}
	want = map[string]string{
		"album":                  "Inbox",
		"drive9.thumbnail.ready": "true",
	}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags after prefix clear = %+v, want %+v", tags, want)
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

func TestLinkFileNodeSharesFileAndRefCount(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 50, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{
		NodeID: "n1", Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		FileID: "f1", InodeID: "f1", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.LinkFileNode(ctx, "/a.txt", "/b.txt", "/", "b.txt", "n2", now); err != nil {
		t.Fatalf("LinkFileNode: %v", err)
	}
	dst, err := s.GetNode(ctx, "/b.txt")
	if err != nil {
		t.Fatalf("GetNode(dst): %v", err)
	}
	if dst.FileID != "f1" || dst.InodeID != "f1" {
		t.Fatalf("dst identity = file %q inode %q, want f1/f1", dst.FileID, dst.InodeID)
	}
	count, err := s.RefCount(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("refcount = %d, want 2", count)
	}
	counts, err := s.RefCounts(ctx, []string{"f1", "f1", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["f1"] != 2 {
		t.Fatalf("batch refcount f1 = %d, want 2", counts["f1"])
	}
	if _, ok := counts["missing"]; ok {
		t.Fatal("batch refcount returned missing file")
	}
	if err := s.LinkFileNode(ctx, "/a.txt", "/b.txt", "/", "b.txt", "n3", now); !errors.Is(err, ErrPathConflict) {
		t.Fatalf("duplicate LinkFileNode error = %v, want ErrPathConflict", err)
	}

	deleted, err := s.DeleteFileWithRefCheck(ctx, "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if deleted != nil {
		t.Fatalf("first unlink deleted file record: %+v", deleted)
	}
	deleted, err = s.DeleteFileWithRefCheck(ctx, "/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if deleted == nil || deleted.Status != StatusDeleted {
		t.Fatalf("last unlink deleted = %+v, want DELETED file", deleted)
	}
}

func TestLinkFileNodeRejectsDirectorySource(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir", Path: "/dir/", ParentPath: "/", Name: "dir", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.LinkFileNode(ctx, "/dir/", "/dir-link", "/", "dir-link", "n2", now); !errors.Is(err, ErrInvalidLinkTarget) {
		t.Fatalf("LinkFileNode directory error = %v, want ErrInvalidLinkTarget", err)
	}
}

func TestLinkFileNodeRejectsMissingParent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 50, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{
		NodeID: "n1", Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		FileID: "f1", InodeID: "f1", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.LinkFileNode(ctx, "/a.txt", "/missing/b.txt", "/missing/", "b.txt", "n2", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LinkFileNode missing parent error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetNode(ctx, "/missing/b.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing-parent link created dst: %v", err)
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
	if _, err := s.GetFileGCTaskByFileID(context.Background(), "f1"); err != ErrNotFound {
		t.Fatalf("expected no gc task while f1 still has refs, got %v", err)
	}

	deleted, err = s.DeleteFileWithRefCheck(context.Background(), "/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if deleted == nil || deleted.Status != StatusDeleted {
		t.Errorf("expected DELETED file record, got %+v", deleted)
	}
	requireEmbeddingRevision(t, deleted.EmbeddingRevision, 17)
	task, err := s.GetFileGCTaskByFileID(context.Background(), "f1")
	if err != nil {
		t.Fatalf("get gc task: %v", err)
	}
	if task.Status != FileGCTaskQueued || task.StorageRef != "/blobs/f1" {
		t.Fatalf("unexpected gc task: %+v", task)
	}
}

func TestDeleteFileWithRefCheckRejectsDirectory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertNode(ctx, &FileNode{NodeID: "dir", Path: "/dir/", ParentPath: "/", Name: "dir", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteFileWithRefCheck(ctx, "/dir/"); err != ErrNotFound {
		t.Fatalf("DeleteFileWithRefCheck dir error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetNode(ctx, "/dir/"); err != nil {
		t.Fatalf("directory should remain after rejected file delete: %v", err)
	}
}

func TestInsertNodeTxRejectsDeletedFile(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 1, Revision: 1, Status: StatusDeleted, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.InsertNodeTx(tx, &FileNode{
			NodeID: "n1", Path: "/deleted.txt", ParentPath: "/", Name: "deleted.txt",
			FileID: "f1", CreatedAt: now,
		})
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("InsertNodeTx error = %v, want %v", err, ErrNotFound)
	}
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
	if _, err := s.GetFileGCTaskByFileID(context.Background(), "f1"); err != ErrNotFound {
		t.Fatalf("expected no gc task for shared f1, got %v", err)
	}
	task, err := s.GetFileGCTaskByFileID(context.Background(), "f2")
	if err != nil {
		t.Fatalf("get gc task: %v", err)
	}
	if task.Status != FileGCTaskQueued || task.StorageRef != "/blobs/f2" {
		t.Fatalf("unexpected gc task: %+v", task)
	}

	_, err = s.GetNode(context.Background(), "/data/")
	if err != ErrNotFound {
		t.Error("expected /data/ deleted")
	}
	_, err = s.GetNode(context.Background(), "/shared.txt")
	if err != nil {
		t.Error("expected /shared.txt to survive")
	}
}

func TestDeleteDirRecursiveDoesNotDeleteSiblingPrefix(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d1", Path: "/data/", ParentPath: "/", Name: "data", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "d2", Path: "/data-other/", ParentPath: "/", Name: "data-other", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertFile(context.Background(), &File{FileID: "f2", StorageType: StorageDB9, StorageRef: "/blobs/f2",
		SizeBytes: 20, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n1", Path: "/data/a.txt", ParentPath: "/data/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{NodeID: "n2", Path: "/data-other/b.txt", ParentPath: "/data-other/", Name: "b.txt", FileID: "f2", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	orphaned, err := s.DeleteDirRecursive(context.Background(), "/data/")
	if err != nil {
		t.Fatal(err)
	}
	if len(orphaned) != 1 || orphaned[0].FileID != "f1" {
		t.Fatalf("orphaned = %+v, want only f1", orphaned)
	}
	if _, err := s.GetNode(context.Background(), "/data-other/"); err != nil {
		t.Fatalf("sibling directory should survive: %v", err)
	}
	if _, err := s.GetNode(context.Background(), "/data-other/b.txt"); err != nil {
		t.Fatalf("sibling file should survive: %v", err)
	}
	if _, err := s.GetFileGCTaskByFileID(context.Background(), "f2"); err != ErrNotFound {
		t.Fatalf("expected no gc task for sibling f2, got %v", err)
	}
}

func TestFileGCTaskClaimAck(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	task := &FileGCTask{
		TaskID:      "f1",
		FileID:      "f1",
		StorageType: StorageS3,
		StorageRef:  "blobs/f1",
		Status:      FileGCTaskQueued,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	inserted, err := s.EnqueueFileGCTaskTx(s.DB(), task)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected first enqueue to insert")
	}
	inserted, err = s.EnqueueFileGCTaskTx(s.DB(), task)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("expected duplicate enqueue to be a no-op")
	}

	claimed, found, err := s.ClaimFileGCTask(ctx, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected queued task to be claimed")
	}
	if claimed.Status != FileGCTaskProcessing || claimed.AttemptCount != 1 || claimed.Receipt == "" {
		t.Fatalf("claimed task = %+v", claimed)
	}
	if err := s.AckFileGCTask(ctx, claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetFileGCTaskByFileID(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FileGCTaskSucceeded || got.CompletedAt == nil {
		t.Fatalf("acked task = %+v", got)
	}
	if err := s.AckFileGCTask(ctx, claimed.TaskID, claimed.Receipt); err != ErrFileGCTaskLeaseMismatch {
		t.Fatalf("second ack err = %v, want %v", err, ErrFileGCTaskLeaseMismatch)
	}
}

func TestFileGCTaskRetryDeadLettersAtMaxAttempts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	task := &FileGCTask{
		TaskID:      "f1",
		FileID:      "f1",
		StorageType: StorageS3,
		StorageRef:  "blobs/f1",
		Status:      FileGCTaskQueued,
		MaxAttempts: 1,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if _, err := s.EnqueueFileGCTaskTx(s.DB(), task); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimFileGCTask(ctx, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected queued task to be claimed")
	}
	if err := s.RetryFileGCTask(ctx, claimed.TaskID, claimed.Receipt, now.Add(time.Second), "delete failed"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetFileGCTaskByFileID(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FileGCTaskDeadLettered || got.LastError != "delete failed" || got.CompletedAt == nil {
		t.Fatalf("retried task = %+v", got)
	}
}

func TestFileGCTaskDefaultRetriesWithoutDeadLetter(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	task := &FileGCTask{
		TaskID:      "f1",
		FileID:      "f1",
		StorageType: StorageS3,
		StorageRef:  "blobs/f1",
		Status:      FileGCTaskQueued,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if _, err := s.EnqueueFileGCTaskTx(s.DB(), task); err != nil {
		t.Fatal(err)
	}

	claimAt := now.Add(time.Second)
	for i := 0; i < 3; i++ {
		claimed, found, err := s.ClaimFileGCTask(ctx, claimAt, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("attempt %d: expected task to be claimed", i+1)
		}
		retryAt := claimAt.Add(time.Second)
		if err := s.RetryFileGCTask(ctx, claimed.TaskID, claimed.Receipt, retryAt, "still failing"); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetFileGCTaskByFileID(ctx, "f1")
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != FileGCTaskQueued || got.MaxAttempts != 0 || got.CompletedAt != nil {
			t.Fatalf("attempt %d retry task = %+v", i+1, got)
		}
		claimAt = retryAt.Add(time.Second)
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

	// Count directory inodes before the idempotent re-call.
	var before int64
	if err := s.DB().QueryRow(`
		SELECT COUNT(*) FROM inodes i
		JOIN file_nodes fn ON i.inode_id = fn.inode_id
		WHERE fn.is_directory = 1`).Scan(&before); err != nil {
		t.Fatalf("count dir inodes before: %v", err)
	}

	// Idempotent: second call must not create orphan inodes.
	if err := s.EnsureParentDirs(context.Background(), "/a/b/c/file.txt", genID); err != nil {
		t.Fatal(err)
	}

	var after int64
	if err := s.DB().QueryRow(`
		SELECT COUNT(*) FROM inodes i
		JOIN file_nodes fn ON i.inode_id = fn.inode_id
		WHERE fn.is_directory = 1`).Scan(&after); err != nil {
		t.Fatalf("count dir inodes after: %v", err)
	}
	if after != before {
		t.Errorf("orphan inodes leaked: dir inode count before=%d after=%d", before, after)
	}

	// Verify there are no orphan inodes at all.
	var orphanCount int64
	if err := s.DB().QueryRow(`
		SELECT COUNT(*) FROM inodes i
		LEFT JOIN file_nodes fn ON i.inode_id = fn.inode_id
		WHERE fn.inode_id IS NULL`).Scan(&orphanCount); err != nil {
		t.Fatalf("count orphan inodes: %v", err)
	}
	if orphanCount != 0 {
		t.Errorf("found %d orphan inodes (inodes with no file_nodes reference)", orphanCount)
	}
}

func TestEnsureParentDirsConcurrent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.EnsureParentDirs(ctx, "/a/b/c/file.txt", genID); err != nil {
				t.Errorf("ensure parent dirs: %v", err)
			}
		}()
	}
	wg.Wait()

	// Exactly three directory inodes should exist.
	var dirInodeCount int64
	if err := s.DB().QueryRow(`
		SELECT COUNT(*) FROM inodes i
		JOIN file_nodes fn ON i.inode_id = fn.inode_id
		WHERE fn.is_directory = 1`).Scan(&dirInodeCount); err != nil {
		t.Fatalf("count dir inodes: %v", err)
	}
	if dirInodeCount != 3 {
		t.Errorf("dir inode count = %d, want 3", dirInodeCount)
	}

	// No orphan inodes should remain.
	var orphanCount int64
	if err := s.DB().QueryRow(`
		SELECT COUNT(*) FROM inodes i
		LEFT JOIN file_nodes fn ON i.inode_id = fn.inode_id
		WHERE fn.inode_id IS NULL`).Scan(&orphanCount); err != nil {
		t.Fatalf("count orphan inodes: %v", err)
	}
	if orphanCount != 0 {
		t.Errorf("found %d orphan inodes after concurrent creation", orphanCount)
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

func TestRenameFileReplacingTarget(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	for _, f := range []*File{
		{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1", SizeBytes: 4, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now},
		{FileID: "f2", StorageType: StorageDB9, StorageRef: "/blobs/f2", SizeBytes: 3, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now},
	} {
		if err := s.InsertFile(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/config.lock", ParentPath: "/", Name: "config.lock", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n2", Path: "/config", ParentPath: "/", Name: "config", FileID: "f2", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO file_tags (file_id, tag_key, tag_value) VALUES (?, ?, ?)`, "f2", "old", "target"); err != nil {
		t.Fatal(err)
	}

	deleted, err := s.RenameFileReplacingTarget(ctx, "/config.lock", "/config", "/", "config")
	if err != nil {
		t.Fatal(err)
	}
	if deleted == nil || deleted.FileID != "f2" {
		t.Fatalf("deleted = %+v, want f2", deleted)
	}
	if _, err := s.GetNode(ctx, "/config.lock"); err != ErrNotFound {
		t.Fatalf("old path err = %v, want ErrNotFound", err)
	}
	got, err := s.GetNode(ctx, "/config")
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "n1" || got.FileID != "f1" || got.Name != "config" {
		t.Fatalf("renamed target = %+v, want source node/file at /config", got)
	}
	replaced, err := s.GetFile(ctx, "f2")
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Status != StatusDeleted {
		t.Fatalf("replaced file status = %s, want %s", replaced.Status, StatusDeleted)
	}
	var tags int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM file_tags WHERE file_id = ?`, "f2").Scan(&tags); err != nil {
		t.Fatal(err)
	}
	if tags != 0 {
		t.Fatalf("replaced file tags = %d, want 0", tags)
	}
	task, err := s.GetFileGCTaskByFileID(ctx, "f2")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != FileGCTaskQueued || task.StorageRef != "/blobs/f2" {
		t.Fatalf("unexpected gc task for replaced file: %+v", task)
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

func TestUpdateFileContentWithDescription(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	if err := s.InsertFile(context.Background(), &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}

	newRev, err := s.UpdateFileContent(context.Background(), "f1", StorageDB9, "/blobs/f1-v2", "text/plain", "abc123", "new content", []byte("blob"), 42, "my description")
	if err != nil {
		t.Fatal(err)
	}
	if newRev != 2 {
		t.Errorf("expected newRev=2, got %d", newRev)
	}
	got, _ := s.GetFile(context.Background(), "f1")
	if got.Revision != 2 || got.Description != "my description" {
		t.Errorf("unexpected: %+v", got)
	}
	if got.DescriptionEmbeddingRevision != nil {
		t.Errorf("expected description_embedding_revision to be nil after content update, got %v", got.DescriptionEmbeddingRevision)
	}
}

func TestChmod(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 42, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/a.txt", ParentPath: "/", Name: "a.txt", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	fixedTime := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	if _, err := s.DB().ExecContext(ctx, `UPDATE inodes SET mtime = ?, confirmed_at = ? WHERE inode_id = ?`, fixedTime, fixedTime, "f1"); err != nil {
		t.Fatal(err)
	}
	var beforeMtime time.Time
	var beforeConfirmedAt time.Time
	if err := s.DB().QueryRowContext(ctx, `SELECT mtime, confirmed_at FROM inodes WHERE inode_id = ?`, "f1").Scan(&beforeMtime, &beforeConfirmedAt); err != nil {
		t.Fatal(err)
	}

	if err := s.Chmod(ctx, "/a.txt", 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != 0o600 {
		t.Errorf("mode=%o, want 0o600", got.Mode)
	}
	if got.Revision != 2 {
		t.Errorf("revision=%d, want 2", got.Revision)
	}
	var afterMtime time.Time
	var afterConfirmedAt time.Time
	if err := s.DB().QueryRowContext(ctx, `SELECT mtime, confirmed_at FROM inodes WHERE inode_id = ?`, "f1").Scan(&afterMtime, &afterConfirmedAt); err != nil {
		t.Fatal(err)
	}
	if !afterMtime.After(beforeMtime) {
		t.Errorf("mtime=%s, want after %s", afterMtime, beforeMtime)
	}
	if !afterConfirmedAt.After(beforeConfirmedAt) {
		t.Errorf("confirmed_at=%s, want after %s", afterConfirmedAt, beforeConfirmedAt)
	}
}

func TestChmodRejectsHistoricalRootDentry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	const inodeID = "root-inode"
	const originalMode = 0o755

	if err := s.InsertInode(ctx, &Inode{
		InodeID:   inodeID,
		SizeBytes: 0,
		Revision:  1,
		Mode:      originalMode,
		Status:    StatusConfirmed,
		CreatedAt: now,
		Mtime:     now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx, `
		INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, inode_id, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?)`,
		"root-node", "/", "/", "root-alias", inodeID, now); err != nil {
		t.Fatal(err)
	}

	err := s.Chmod(ctx, "/", 0o700)
	if !errors.Is(err, ErrInvalidRootDentry) {
		t.Fatalf("Chmod(/) error = %v, want %v", err, ErrInvalidRootDentry)
	}
	var mode uint32
	if err := s.DB().QueryRowContext(ctx, `SELECT mode FROM inodes WHERE inode_id = ?`, inodeID).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != originalMode {
		t.Fatalf("root inode mode = %o, want unchanged %o", mode, originalMode)
	}
}

func TestChmodPreservesFileTypeBits(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		SizeBytes:   6,
		Revision:    1,
		Mode:        0o120777,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/link", ParentPath: "/", Name: "link", FileID: "f1", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	if err := s.Chmod(ctx, "/link", 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != 0o120600 {
		t.Errorf("mode=%o, want 0o120600", got.Mode)
	}
}

func TestChmodNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	err := s.Chmod(ctx, "/nonexistent.txt", 0o600)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestChmodDirectory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertNode(ctx, &FileNode{NodeID: "n1", Path: "/dir/", ParentPath: "/", Name: "dir", IsDirectory: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	err := s.Chmod(ctx, "/dir/", 0o700)
	if err == nil {
		t.Fatal("expected error for chmod on directory")
	}
}

func TestConfirmPendingFileTx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.ConfirmPendingFileTx(tx, "f1", StorageS3, "/s3/f1", "text/plain", 42, "desc"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusConfirmed {
		t.Errorf("status=%s, want CONFIRMED", got.Status)
	}
	if got.SizeBytes != 42 {
		t.Errorf("size=%d, want 42", got.SizeBytes)
	}
	if got.StorageType != StorageS3 {
		t.Errorf("storage_type=%s, want s3", got.StorageType)
	}
	if got.Description != "desc" {
		t.Errorf("description=%q, want desc", got.Description)
	}
}

func TestConfirmPendingFileAutoEmbeddingTx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.ConfirmPendingFileAutoEmbeddingTx(tx, "f1", StorageS3, "/s3/f1", "text/plain", 42, "auto-desc"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusConfirmed {
		t.Errorf("status=%s, want CONFIRMED", got.Status)
	}
	if got.SizeBytes != 42 {
		t.Errorf("size=%d, want 42", got.SizeBytes)
	}
	if got.Description != "auto-desc" {
		t.Errorf("description=%q, want auto-desc", got.Description)
	}
}

func TestDeletePendingFileTx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 10, Revision: 1, Status: StatusPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.DeletePendingFileTx(tx, "f1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusDeleted {
		t.Errorf("status=%s, want DELETED", got.Status)
	}
	if got.StorageRef != "" {
		t.Errorf("storage_ref=%q, want empty", got.StorageRef)
	}
}

func TestGetFileStorageMetaForUpdateTx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageS3, StorageRef: "/s3/f1",
		ContentType: "text/plain", SizeBytes: 42, Revision: 3, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	meta, err := s.GetFileStorageMetaForUpdateTx(tx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if meta.StorageType != StorageS3 {
		t.Errorf("storage_type=%s, want s3", meta.StorageType)
	}
	if meta.StorageRef != "/s3/f1" {
		t.Errorf("storage_ref=%q, want /s3/f1", meta.StorageRef)
	}
	if meta.Revision != 3 {
		t.Errorf("revision=%d, want 3", meta.Revision)
	}
	if meta.SizeBytes != 42 {
		t.Errorf("size=%d, want 42", meta.SizeBytes)
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("content_type=%q, want text/plain", meta.ContentType)
	}
}

func TestGetFileStorageMetaForUpdateTxNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = s.GetFileStorageMetaForUpdateTx(tx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestModeDefault(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	// Insert without Mode set (defaults to 0).
	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 42, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != 0o644 {
		t.Errorf("mode=%o, want 0o644", got.Mode)
	}
}

func TestModeRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	if err := s.InsertFile(ctx, &File{FileID: "f1", StorageType: StorageDB9, StorageRef: "/blobs/f1",
		SizeBytes: 42, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != 0o755 {
		t.Errorf("mode=%o, want 0o755", got.Mode)
	}
}

// TestInsertFileWithoutLegacyTable verifies that a store without the legacy
// `files` table writes only to split tables and can read data back correctly.
func TestInsertFileWithoutLegacyTable(t *testing.T) {
	// Open a store without creating the files table.
	initDatastoreSchema(t, testDSN)
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`DROP TABLE IF EXISTS files`); err != nil {
		t.Fatalf("drop files: %v", err)
	}
	testmysql.ResetDBWithoutFiles(t, db)
	_ = db.Close()

	s, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if s.HasLegacyFiles() {
		t.Fatal("expected HasLegacyFiles() = false without files table")
	}

	ctx := context.Background()
	now := time.Now()
	f := &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		SizeBytes:   42,
		Revision:    1,
		Status:      StatusConfirmed,
		Mode:        0o600,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}
	if err := s.InsertFile(ctx, f); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Verify read from split tables.
	got, err := s.GetFile(ctx, "f1")
	if err != nil {
		t.Fatalf("get file: %v", err)
	}
	if got.SizeBytes != 42 {
		t.Errorf("size=%d, want 42", got.SizeBytes)
	}
	if got.Mode != 0o600 {
		t.Errorf("mode=%o, want 0o600", got.Mode)
	}

	// Verify files table was NOT written.
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM files`).Scan(&count); err == nil {
		t.Error("expected error querying files table (should not exist), got nil")
	}
}
