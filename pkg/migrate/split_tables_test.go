package migrate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
)

func TestSplitTablesMigrator(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	// Insert a file via the old path (this also dual-writes to split tables)
	f := &datastore.File{
		FileID:      "file1",
		StorageType: datastore.StorageDB9,
		StorageRef:  "ref1",
		SizeBytes:   100,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   time.Now(),
	}
	if err := s.InsertFile(ctx, f); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Clear split tables so we can test migration backfill
	db := s.DB()
	if _, err := db.Exec("DELETE FROM inodes"); err != nil {
		t.Fatalf("clear inodes: %v", err)
	}
	if _, err := db.Exec("DELETE FROM contents"); err != nil {
		t.Fatalf("clear contents: %v", err)
	}
	if _, err := db.Exec("DELETE FROM semantic"); err != nil {
		t.Fatalf("clear semantic: %v", err)
	}

	// Run migration
	m := NewSplitTablesMigrator(db)
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if res.InodesMigrated != 1 {
		t.Errorf("inodes migrated = %d, want 1", res.InodesMigrated)
	}
	if res.ContentsMigrated != 1 {
		t.Errorf("contents migrated = %d, want 1", res.ContentsMigrated)
	}
	if res.SemanticMigrated != 1 {
		t.Errorf("semantic migrated = %d, want 1", res.SemanticMigrated)
	}

	// Verify inode exists
	inode, err := s.GetInode(ctx, "file1")
	if err != nil {
		t.Fatalf("get inode after migration: %v", err)
	}
	if inode.SizeBytes != 100 {
		t.Errorf("inode size = %d, want 100", inode.SizeBytes)
	}

	// Verify content exists
	content, err := s.GetContent(ctx, "file1")
	if err != nil {
		t.Fatalf("get content after migration: %v", err)
	}
	if content.StorageRef != "ref1" {
		t.Errorf("content storage_ref = %q, want ref1", content.StorageRef)
	}

	// Idempotency: running again should report 0 changes
	res2, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("second migration failed: %v", err)
	}
	if res2.InodesMigrated != 0 {
		t.Errorf("second run inodes = %d, want 0", res2.InodesMigrated)
	}
}

func TestSplitTablesMigratorDirectoryInodes(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	// Insert a directory node (no files row)
	if err := s.InsertNode(ctx, &datastore.FileNode{
		NodeID:      "dir1",
		Path:        "/testdir/",
		ParentPath:  "/",
		Name:        "testdir",
		IsDirectory: true,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("insert node: %v", err)
	}

	m := NewSplitTablesMigrator(s.DB())
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if res.DirInodesCreated != 1 {
		t.Errorf("dir inodes created = %d, want 1", res.DirInodesCreated)
	}

	inode, err := s.GetInode(ctx, "dir1")
	if err != nil {
		t.Fatalf("get dir inode: %v", err)
	}
	if inode.Status != datastore.StatusConfirmed {
		t.Errorf("dir inode status = %q, want CONFIRMED", inode.Status)
	}
	if inode.SizeBytes != 0 {
		t.Errorf("dir inode size = %d, want 0", inode.SizeBytes)
	}
}

func TestSplitTablesMigratorPartialMigrationRerun(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	// Insert a file via the old path
	f := &datastore.File{
		FileID:      "file1",
		StorageType: datastore.StorageDB9,
		StorageRef:  "ref1",
		SizeBytes:   100,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   time.Now(),
	}
	if err := s.InsertFile(ctx, f); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	db := s.DB()

	// Simulate partial migration: only inodes migrated, contents/semantic empty
	if _, err := db.Exec("DELETE FROM contents"); err != nil {
		t.Fatalf("clear contents: %v", err)
	}
	if _, err := db.Exec("DELETE FROM semantic"); err != nil {
		t.Fatalf("clear semantic: %v", err)
	}

	m := NewSplitTablesMigrator(db)
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	if res.InodesMigrated != 0 {
		t.Errorf("inodes migrated = %d, want 0 (already present)", res.InodesMigrated)
	}
	if res.ContentsMigrated != 1 {
		t.Errorf("contents migrated = %d, want 1", res.ContentsMigrated)
	}
	if res.SemanticMigrated != 1 {
		t.Errorf("semantic migrated = %d, want 1", res.SemanticMigrated)
	}
}

func TestSplitTablesMigratorAlreadyComplete(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	// Insert a file via the old path (this also dual-writes to split tables)
	f := &datastore.File{
		FileID:      "file1",
		StorageType: datastore.StorageDB9,
		StorageRef:  "ref1",
		SizeBytes:   100,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   time.Now(),
	}
	if err := s.InsertFile(ctx, f); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Clear split tables so the first Run has actual work to do.
	db := s.DB()
	if _, err := db.Exec("DELETE FROM inodes"); err != nil {
		t.Fatalf("clear inodes: %v", err)
	}
	if _, err := db.Exec("DELETE FROM contents"); err != nil {
		t.Fatalf("clear contents: %v", err)
	}
	if _, err := db.Exec("DELETE FROM semantic"); err != nil {
		t.Fatalf("clear semantic: %v", err)
	}

	m := NewSplitTablesMigrator(db)

	// First run: migrates data
	res1, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("first migration failed: %v", err)
	}
	if res1.InodesMigrated != 1 {
		t.Errorf("first run inodes = %d, want 1", res1.InodesMigrated)
	}

	// Second run: should detect completion and skip all work
	res2, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("second migration failed: %v", err)
	}
	if res2.InodesMigrated != 0 {
		t.Errorf("second run inodes = %d, want 0 (already complete)", res2.InodesMigrated)
	}
	if res2.ContentsMigrated != 0 {
		t.Errorf("second run contents = %d, want 0 (already complete)", res2.ContentsMigrated)
	}
	if res2.SemanticMigrated != 0 {
		t.Errorf("second run semantic = %d, want 0 (already complete)", res2.SemanticMigrated)
	}
	if res2.DirInodesCreated != 0 {
		t.Errorf("second run dir inodes = %d, want 0 (already complete)", res2.DirInodesCreated)
	}
	if res2.SharedColsUpdated != 0 {
		t.Errorf("second run shared cols = %d, want 0 (already complete)", res2.SharedColsUpdated)
	}
}

func TestSplitTablesMigratorPartialStep5(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	// Insert a file via the old path (this creates files + inodes + contents + semantic)
	f := &datastore.File{
		FileID:      "file1",
		StorageType: datastore.StorageDB9,
		StorageRef:  "ref1",
		SizeBytes:   100,
		Revision:    1,
		Status:      datastore.StatusConfirmed,
		CreatedAt:   time.Now(),
	}
	if err := s.InsertFile(ctx, f); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	// Insert a file_nodes row with inode_id NULL to simulate pre-migration state
	// where shared backfill has not run.
	if err := s.InsertNode(ctx, &datastore.FileNode{
		NodeID:      f.FileID,
		Path:        "/test.txt",
		ParentPath:  "/",
		Name:        "test.txt",
		IsDirectory: false,
		FileID:      f.FileID,
		InodeID:     "", // explicitly empty
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("insert file node: %v", err)
	}

	db := s.DB()
	m := NewSplitTablesMigrator(db)

	// Run steps 1-4 manually, skipping step 5
	if _, err := m.migrateInodes(ctx); err != nil {
		t.Fatalf("migrate inodes: %v", err)
	}
	if _, err := m.migrateContents(ctx); err != nil {
		t.Fatalf("migrate contents: %v", err)
	}
	if _, err := m.migrateSemantic(ctx); err != nil {
		t.Fatalf("migrate semantic: %v", err)
	}
	if _, err := m.createDirInodes(ctx); err != nil {
		t.Fatalf("create dir inodes: %v", err)
	}
	// Deliberately skip backfillSharedInodeID to simulate a crash after step 4.

	// isMigrationComplete should detect the missing shared backfill
	complete, err := m.isMigrationComplete(ctx)
	if err != nil {
		t.Fatalf("check completion: %v", err)
	}
	if complete {
		t.Fatal("expected incomplete due to missing shared backfill")
	}

	// Full Run should now repair step 5
	res, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	if res.SharedColsUpdated == 0 {
		t.Errorf("shared cols updated = %d, want > 0", res.SharedColsUpdated)
	}
}

func TestSplitTablesMigratorMissingTables(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	db := s.DB()

	// Drop the semantic table to simulate uninitialized schema
	if _, err := db.Exec("DROP TABLE semantic"); err != nil {
		t.Fatalf("drop semantic: %v", err)
	}

	m := NewSplitTablesMigrator(db)
	_, err = m.Run(ctx)
	if err == nil {
		t.Fatal("expected error when semantic table is missing")
	}
	if !strings.Contains(err.Error(), "required split tables") {
		t.Errorf("error message did not mention missing tables: %v", err)
	}
}
