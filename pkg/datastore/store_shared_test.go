package datastore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// installSharedCoreFSNoLegacy swaps the Core FS tables to the shared (fs_id)
// shape and drops the legacy files table: shared databases never carry it, so
// the shared store must run with useLegacyFiles=false. The installSharedTables
// cleanup restores the full standalone schema, files table included.
func installSharedCoreFSNoLegacy(t *testing.T) {
	t.Helper()
	installSharedCoreFSSchema(t)
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`DROP TABLE IF EXISTS files`); err != nil {
		t.Fatalf("drop legacy files table: %v", err)
	}
}

var vaultSharedTables = []string{
	"vault_audit_log", "vault_grants", "vault_tokens", "vault_secret_fields",
	"vault_secrets", "vault_policies", "vault_deks",
}

// installSharedVaultSchema swaps the 7 vault tables to the shared (fs_id)
// shape so SanitizeForkRuntimeState can be exercised against its full table
// list: in shared shape its wholesale DELETEs must carry fs_id predicates.
func installSharedVaultSchema(t *testing.T) {
	t.Helper()
	installSharedTables(t, vaultSharedTables, schema.VaultMySQLSharedSchemaStatements())
}

// requireOnlyFsIDRows asserts tbl holds at least one row and that every row
// carries fsID: a shared table must never contain rows stamped with another
// tenant's fs_id.
func requireOnlyFsIDRows(t *testing.T, s *Store, tbl string, fsID int64) {
	t.Helper()
	var foreign int64
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id != ?", fsID).Scan(&foreign); err != nil {
		t.Fatalf("count %s rows with foreign fs_id: %v", tbl, err)
	}
	if foreign != 0 {
		t.Fatalf("%s has %d rows with fs_id != %d", tbl, foreign, fsID)
	}
	var total int64
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&total); err != nil {
		t.Fatalf("count %s: %v", tbl, err)
	}
	if total == 0 {
		t.Fatalf("%s is empty; scenario should have written rows", tbl)
	}
}

// countFsIDRows returns the number of rows in tbl stamped with fsID.
func countFsIDRows(t *testing.T, s *Store, tbl string, fsID int64) int64 {
	t.Helper()
	var n int64
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id = ?", fsID).Scan(&n); err != nil {
		t.Fatalf("count %s rows for fs_id %d: %v", tbl, fsID, err)
	}
	return n
}

// runStoreCoreScenario exercises the Core FS flows — create file (inode,
// content, semantic, dentry), get/list by path and by parent, overwrite with
// revision bump, tag replace/query, upload reservation lifecycle, ref-checked
// delete with GC enqueue — against a store. It runs
// against the shared schema shape to prove behavioral parity with the
// standalone flows covered by store_test.go / file_tx_test.go.
func runStoreCoreScenario(t *testing.T, s *Store, pfx string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()

	// Create file: inserts inode/content/semantic rows plus the dentry.
	fileID := pfx + "-file"
	filePath := "/" + pfx + "/docs/a.txt"
	if err := s.InsertFile(ctx, &File{
		FileID: fileID, StorageType: StorageDB9, StorageRef: "inline:" + pfx + ":v1",
		ContentBlob: []byte("hello " + pfx), ContentType: "text/plain",
		SizeBytes: 6, Revision: 1, Status: StatusConfirmed,
		ContentText: "hello " + pfx, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}
	if err := s.EnsureParentDirs(ctx, filePath, genID); err != nil {
		t.Fatalf("EnsureParentDirs: %v", err)
	}
	if err := s.InsertNode(ctx, &FileNode{
		NodeID: fileID + "-node", Path: filePath, ParentPath: "/" + pfx + "/docs/",
		Name: "a.txt", FileID: fileID, CreatedAt: now,
	}); err != nil {
		t.Fatalf("InsertNode: %v", err)
	}

	// Get/list by path and by parent.
	node, err := s.GetNode(ctx, filePath)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.NodeID != fileID+"-node" || node.FileID != fileID {
		t.Fatalf("node = %+v", node)
	}
	if exists, err := s.NodeExists(ctx, filePath); err != nil || !exists {
		t.Fatalf("NodeExists = %v, %v; want true, nil", exists, err)
	}
	nodes, err := s.ListNodes(ctx, "/"+pfx+"/docs/")
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Path != filePath {
		t.Fatalf("nodes = %+v", nodes)
	}
	stat, err := s.Stat(ctx, filePath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.File == nil || stat.File.SizeBytes != 6 || stat.File.ContentText != "hello "+pfx {
		t.Fatalf("stat = %+v", stat)
	}
	lite, err := s.StatLite(ctx, filePath)
	if err != nil {
		t.Fatalf("StatLite: %v", err)
	}
	if lite.File == nil || lite.File.Revision != 1 {
		t.Fatalf("stat lite = %+v", lite)
	}
	forRead, err := s.StatForRead(ctx, filePath)
	if err != nil {
		t.Fatalf("StatForRead: %v", err)
	}
	if forRead.File == nil || string(forRead.File.ContentBlob) != "hello "+pfx {
		t.Fatalf("stat for read = %+v", forRead)
	}
	fallback, err := s.StatPathFallback(ctx, "/"+pfx+"/missing.txt", filePath)
	if err != nil {
		t.Fatalf("StatPathFallback: %v", err)
	}
	if fallback.Node.Path != filePath {
		t.Fatalf("stat fallback path = %q, want %q", fallback.Node.Path, filePath)
	}
	entries, err := s.ListDir(ctx, "/"+pfx+"/docs/")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Node.Name != "a.txt" || entries[0].File == nil {
		t.Fatalf("dir entries = %+v", entries)
	}
	got, err := s.GetFile(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if got.StorageType != StorageDB9 || got.ContentText != "hello "+pfx || got.Revision != 1 {
		t.Fatalf("file = %+v", got)
	}

	// Overwrite: content update bumps the revision across the split tables.
	newRev, err := s.UpdateFileContent(ctx, fileID, StorageDB9, "inline:"+pfx+":v2",
		"text/plain", "sum2", "hello v2 "+pfx, []byte("hello v2 "+pfx), 10, "")
	if err != nil {
		t.Fatalf("UpdateFileContent: %v", err)
	}
	if newRev != 2 {
		t.Fatalf("new revision = %d, want 2", newRev)
	}
	got, err = s.GetFile(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFile after overwrite: %v", err)
	}
	if got.Revision != 2 || got.SizeBytes != 10 || got.ContentText != "hello v2 "+pfx || got.StorageRef != "inline:"+pfx+":v2" {
		t.Fatalf("file after overwrite = %+v", got)
	}

	// Tags: full replace, then a second replace that shrinks the set.
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, fileID, map[string]string{"owner": pfx, "topic": "shared"})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx initial: %v", err)
	}
	tags, err := s.GetFileTags(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFileTags: %v", err)
	}
	if len(tags) != 2 || tags["owner"] != pfx || tags["topic"] != "shared" {
		t.Fatalf("tags = %+v", tags)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.ReplaceFileTagsTx(tx, fileID, map[string]string{"owner": pfx + "-2"})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx replace: %v", err)
	}
	tags, err = s.GetFileTags(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFileTags after replace: %v", err)
	}
	if len(tags) != 1 || tags["owner"] != pfx+"-2" {
		t.Fatalf("tags after replace = %+v", tags)
	}

	// Upload reservation lifecycle: insert, reserve quota, complete; then a
	// second upload that is aborted.
	upload := &Upload{
		UploadID: pfx + "-upload", FileID: pfx + "-up-file", TargetPath: "/" + pfx + "/up.bin",
		S3UploadID: "s3up-" + pfx, S3Key: "key-" + pfx, TotalSize: 100, PartSize: 50, PartsTotal: 2,
		Status: UploadUploading, IdempotencyKey: "idem-" + pfx, CreatedAt: now, UpdatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
	if err := s.InsertUpload(ctx, upload); err != nil {
		t.Fatalf("InsertUpload: %v", err)
	}
	dup := *upload
	dup.UploadID = pfx + "-upload-dup"
	if err := s.InsertUpload(ctx, &dup); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("InsertUpload dup err = %v, want ErrIdempotencyConflict", err)
	}
	gotUpload, err := s.GetUploadByPath(ctx, "/"+pfx+"/up.bin")
	if err != nil {
		t.Fatalf("GetUploadByPath: %v", err)
	}
	if gotUpload.UploadID != upload.UploadID || gotUpload.Status != UploadUploading {
		t.Fatalf("upload = %+v", gotUpload)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		reserved, err := s.ActiveUploadReservedBytesTx(tx)
		if err != nil {
			return err
		}
		if reserved != 100 {
			t.Fatalf("reserved bytes = %d, want 100", reserved)
		}
		confirmed, err := s.ConfirmedStorageBytesTx(tx)
		if err != nil {
			return err
		}
		if confirmed != 10 {
			t.Fatalf("confirmed bytes = %d, want 10", confirmed)
		}
		return nil
	}); err != nil {
		t.Fatalf("quota tx: %v", err)
	}
	if err := s.CompleteUpload(ctx, upload.UploadID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	gotUpload, err = s.GetUpload(ctx, upload.UploadID)
	if err != nil {
		t.Fatalf("GetUpload after complete: %v", err)
	}
	if gotUpload.Status != UploadCompleted {
		t.Fatalf("upload status = %q, want COMPLETED", gotUpload.Status)
	}
	if _, err := s.GetUploadByPath(ctx, "/"+pfx+"/up.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetUploadByPath after complete err = %v, want ErrNotFound", err)
	}
	abortedUpload := &Upload{
		UploadID: pfx + "-upload-2", FileID: pfx + "-up-file-2", TargetPath: "/" + pfx + "/up2.bin",
		S3UploadID: "s3up2-" + pfx, S3Key: "key2-" + pfx, TotalSize: 8, PartSize: 4, PartsTotal: 2,
		Status: UploadUploading, IdempotencyKey: "idem2-" + pfx, CreatedAt: now, UpdatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
	if err := s.InsertUpload(ctx, abortedUpload); err != nil {
		t.Fatalf("InsertUpload 2: %v", err)
	}
	if err := s.AbortUpload(ctx, abortedUpload.UploadID); err != nil {
		t.Fatalf("AbortUpload: %v", err)
	}
	gotUpload, err = s.GetUpload(ctx, abortedUpload.UploadID)
	if err != nil {
		t.Fatalf("GetUpload after abort: %v", err)
	}
	if gotUpload.Status != UploadAborted {
		t.Fatalf("upload 2 status = %q, want ABORTED", gotUpload.Status)
	}

	// Delete with ref check: a second link keeps the file alive; deleting the
	// last link marks the inode DELETED and enqueues the GC task.
	linkPath := "/" + pfx + "/docs/b.txt"
	if err := s.LinkFileNode(ctx, filePath, linkPath, "/"+pfx+"/docs/", "b.txt", fileID+"-link-node", now); err != nil {
		t.Fatalf("LinkFileNode: %v", err)
	}
	deleted, err := s.DeleteFileWithRefCheck(ctx, filePath)
	if err != nil {
		t.Fatalf("DeleteFileWithRefCheck first link: %v", err)
	}
	if deleted != nil {
		t.Fatalf("deleted = %+v, want nil (file should survive with refs)", deleted)
	}
	if _, err := s.GetNode(ctx, filePath); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetNode deleted link err = %v, want ErrNotFound", err)
	}
	if _, err := s.GetNode(ctx, linkPath); err != nil {
		t.Fatalf("remaining link node: %v", err)
	}
	got, err = s.GetFile(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFile after first delete: %v", err)
	}
	if got.Status != StatusConfirmed {
		t.Fatalf("file status after first delete = %q, want CONFIRMED", got.Status)
	}
	deleted, err = s.DeleteFileWithRefCheck(ctx, linkPath)
	if err != nil {
		t.Fatalf("DeleteFileWithRefCheck last link: %v", err)
	}
	if deleted == nil || deleted.Status != StatusDeleted {
		t.Fatalf("deleted = %+v, want DELETED file", deleted)
	}
	got, err = s.GetFile(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFile after last delete: %v", err)
	}
	if got.Status != StatusDeleted {
		t.Fatalf("file status after last delete = %q, want DELETED", got.Status)
	}
	task, err := s.GetFileGCTaskByFileID(ctx, fileID)
	if err != nil {
		t.Fatalf("GetFileGCTaskByFileID: %v", err)
	}
	if task.Status != FileGCTaskQueued || task.StorageRef != "inline:"+pfx+":v2" {
		t.Fatalf("gc task = %+v", task)
	}
}

// TestStoreSharedShapeParity runs the Core FS scenario against the shared
// (fs_id) schema shape without the legacy files table, mirroring the
// standalone coverage in store_test.go / file_tx_test.go.
func TestStoreSharedShapeParity(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	store := newSharedStore(t, 4300001)
	if store.HasLegacyFiles() {
		t.Fatal("shared store must run without the legacy files table")
	}
	runStoreCoreScenario(t, store, "shpar")
}

// TestStoreSharedShapeCrossTenantIsolation proves rows of one fs_id are
// invisible to another fs_id on the same shared tables: the same path can
// coexist under two fs_ids (UNIQUE (fs_id, path_hash)), and reads, writes,
// deletes, and uploads never cross the tenant boundary.
func TestStoreSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	ctx := context.Background()
	storeA := newSharedStore(t, 4300002)
	storeB := newSharedStore(t, 4300003)
	now := time.Now()

	// Same path created under two fs_ids must coexist.
	for _, tc := range []struct {
		store  *Store
		fileID string
		text   string
	}{
		{storeA, "iso-a-file", "hello iso a"},
		{storeB, "iso-b-file", "hello iso b"},
	} {
		if err := tc.store.InsertFile(ctx, &File{
			FileID: tc.fileID, StorageType: StorageDB9, StorageRef: "inline:" + tc.fileID,
			ContentType: "text/plain", SizeBytes: 4, Revision: 1, Status: StatusConfirmed,
			ContentText: tc.text, CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Fatalf("InsertFile %s: %v", tc.fileID, err)
		}
		if err := tc.store.EnsureParentDirs(ctx, "/iso/shared.txt", genID); err != nil {
			t.Fatalf("EnsureParentDirs %s: %v", tc.fileID, err)
		}
		if err := tc.store.InsertNode(ctx, &FileNode{
			NodeID: tc.fileID + "-node", Path: "/iso/shared.txt", ParentPath: "/iso/",
			Name: "shared.txt", FileID: tc.fileID, CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertNode %s: %v", tc.fileID, err)
		}
		if err := tc.store.InTx(ctx, func(tx *sql.Tx) error {
			return tc.store.ReplaceFileTagsTx(tx, tc.fileID, map[string]string{"owner": tc.fileID})
		}); err != nil {
			t.Fatalf("ReplaceFileTagsTx %s: %v", tc.fileID, err)
		}
		// Same upload target path reserved under both fs_ids must coexist
		// (idx_uploads_active is per fs_id).
		if err := tc.store.InsertUpload(ctx, &Upload{
			UploadID: tc.fileID + "-upload", FileID: tc.fileID + "-up", TargetPath: "/iso/up.bin",
			S3UploadID: "s3up-" + tc.fileID, S3Key: "key-" + tc.fileID, TotalSize: 10, PartSize: 5, PartsTotal: 2,
			Status: UploadUploading, IdempotencyKey: "idem-" + tc.fileID, CreatedAt: now, UpdatedAt: now,
			ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("InsertUpload %s: %v", tc.fileID, err)
		}
	}

	// Each store resolves the shared path to its own dentry and file.
	nodeA, err := storeA.GetNode(ctx, "/iso/shared.txt")
	if err != nil {
		t.Fatalf("GetNode A: %v", err)
	}
	if nodeA.FileID != "iso-a-file" {
		t.Fatalf("A node = %+v, want iso-a-file", nodeA)
	}
	nodeB, err := storeB.GetNode(ctx, "/iso/shared.txt")
	if err != nil {
		t.Fatalf("GetNode B: %v", err)
	}
	if nodeB.FileID != "iso-b-file" {
		t.Fatalf("B node = %+v, want iso-b-file", nodeB)
	}
	nodesB, err := storeB.ListNodes(ctx, "/iso/")
	if err != nil {
		t.Fatalf("ListNodes B: %v", err)
	}
	if len(nodesB) != 1 || nodesB[0].FileID != "iso-b-file" {
		t.Fatalf("B nodes = %+v, want only iso-b-file", nodesB)
	}

	// A write under A must not leak into B: A bumps its revision, B is unchanged.
	if _, err := storeA.UpdateFileContent(ctx, "iso-a-file", StorageDB9, "inline:iso-a-file:v2",
		"text/plain", "sum", "hello iso a v2", []byte("hello iso a v2"), 9, ""); err != nil {
		t.Fatalf("UpdateFileContent A: %v", err)
	}
	fileB, err := storeB.GetFile(ctx, "iso-b-file")
	if err != nil {
		t.Fatalf("GetFile B: %v", err)
	}
	if fileB.Revision != 1 || fileB.ContentText != "hello iso b" || fileB.SizeBytes != 4 {
		t.Fatalf("B file after A write = %+v, want unchanged", fileB)
	}

	// Tag replace under A must not touch B's tags.
	if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
		return storeA.ReplaceFileTagsTx(tx, "iso-a-file", map[string]string{"owner": "replaced"})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx A: %v", err)
	}
	tagsB, err := storeB.GetFileTags(ctx, "iso-b-file")
	if err != nil {
		t.Fatalf("GetFileTags B: %v", err)
	}
	if len(tagsB) != 1 || tagsB["owner"] != "iso-b-file" {
		t.Fatalf("B tags after A replace = %+v, want unchanged", tagsB)
	}

	// Upload reads/aborts stay per-tenant.
	upB, err := storeB.GetUploadByPath(ctx, "/iso/up.bin")
	if err != nil {
		t.Fatalf("GetUploadByPath B: %v", err)
	}
	if upB.UploadID != "iso-b-file-upload" {
		t.Fatalf("B upload = %+v, want iso-b-file-upload", upB)
	}
	if aborted, err := storeA.AbortUploadV2(ctx, "iso-a-file-upload"); err != nil || !aborted {
		t.Fatalf("AbortUploadV2 A = %v, %v; want true, nil", aborted, err)
	}
	upB, err = storeB.GetUpload(ctx, "iso-b-file-upload")
	if err != nil {
		t.Fatalf("GetUpload B: %v", err)
	}
	if upB.Status != UploadUploading {
		t.Fatalf("B upload status after A abort = %q, want UPLOADING", upB.Status)
	}

	// A delete under A must not touch B's dentry or file.
	if err := storeA.DeleteNode(ctx, "/iso/shared.txt"); err != nil {
		t.Fatalf("DeleteNode A: %v", err)
	}
	if _, err := storeA.GetNode(ctx, "/iso/shared.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("A GetNode after delete err = %v, want ErrNotFound", err)
	}
	if _, err := storeB.GetNode(ctx, "/iso/shared.txt"); err != nil {
		t.Fatalf("B GetNode after A delete: %v", err)
	}
	fileB, err = storeB.GetFile(ctx, "iso-b-file")
	if err != nil {
		t.Fatalf("B GetFile after A delete: %v", err)
	}
	if fileB.Status != StatusConfirmed {
		t.Fatalf("B file status after A delete = %q, want CONFIRMED", fileB.Status)
	}
}

// TestStoreSharedShapeStoresFsID asserts every Core FS table row written by
// the scenario carries the scope's fs_id as its tenant discriminator.
func TestStoreSharedShapeStoresFsID(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	const fsID int64 = 4300004
	store := newSharedStore(t, fsID)
	runStoreCoreScenario(t, store, "shfsid")

	// The scenario's ref-checked delete removes its file's tags; leave one
	// tagged file behind so file_tags is covered by the fs_id row check.
	ctx := context.Background()
	now := time.Now()
	if err := store.InsertFile(ctx, &File{
		FileID: "shfsid-keep", StorageType: StorageDB9, StorageRef: "inline:shfsid-keep",
		SizeBytes: 1, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		t.Fatalf("InsertFile keep: %v", err)
	}
	if err := store.InTx(ctx, func(tx *sql.Tx) error {
		return store.ReplaceFileTagsTx(tx, "shfsid-keep", map[string]string{"owner": "shfsid"})
	}); err != nil {
		t.Fatalf("ReplaceFileTagsTx keep: %v", err)
	}

	// The scenario writes file_nodes, inodes, contents, semantic, file_tags,
	// and uploads directly; the ref-checked delete additionally enqueues a
	// file_gc_tasks row.
	for _, tbl := range []string{
		"file_nodes", "inodes", "contents", "semantic", "file_tags",
		"uploads", "file_gc_tasks",
	} {
		requireOnlyFsIDRows(t, store, tbl, fsID)
	}
}

// TestSanitizeForkRuntimeStateSharedShape proves the fork sanitize only wipes
// its own fs_id's runtime state on the shared schema: pending upload state,
// uploads, task tables, and vault rows of the other tenant must survive
// untouched.
func TestSanitizeForkRuntimeStateSharedShape(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	installSharedVaultSchema(t)
	ctx := context.Background()
	const fsA, fsB int64 = 4300010, 4300011
	storeA := newSharedStore(t, fsA)
	storeB := newSharedStore(t, fsB)
	now := time.Now()

	// Seed the same runtime state for two tenants.
	for _, tc := range []struct {
		store *Store
		pfx   string
		fsID  int64
	}{
		{storeA, "san-a", fsA},
		{storeB, "san-b", fsB},
	} {
		// Pending upload-time file: sanitize must delete its dentry, wipe its
		// content ref, and mark the inode DELETED.
		if err := tc.store.InsertFile(ctx, &File{
			FileID: tc.pfx + "-pending", StorageType: StorageS3, StorageRef: "s3://" + tc.pfx + "/pending",
			SizeBytes: 5, Revision: 1, Status: StatusPending, CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertFile pending %s: %v", tc.pfx, err)
		}
		if err := tc.store.InsertNode(ctx, &FileNode{
			NodeID: tc.pfx + "-pending-node", Path: "/" + tc.pfx + "/pending.bin", ParentPath: "/",
			Name: "pending.bin", FileID: tc.pfx + "-pending", CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertNode pending %s: %v", tc.pfx, err)
		}
		// Confirmed file: sanitize must leave it untouched.
		if err := tc.store.InsertFile(ctx, &File{
			FileID: tc.pfx + "-confirmed", StorageType: StorageDB9, StorageRef: "inline:" + tc.pfx,
			SizeBytes: 3, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Fatalf("InsertFile confirmed %s: %v", tc.pfx, err)
		}
		if err := tc.store.InsertUpload(ctx, &Upload{
			UploadID: tc.pfx + "-upload", FileID: tc.pfx + "-up-file", TargetPath: "/" + tc.pfx + "/up.bin",
			S3UploadID: "s3up-" + tc.pfx, S3Key: "key-" + tc.pfx, TotalSize: 10, PartSize: 5, PartsTotal: 2,
			Status: UploadUploading, IdempotencyKey: "idem-" + tc.pfx, CreatedAt: now, UpdatedAt: now,
			ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("InsertUpload %s: %v", tc.pfx, err)
		}
		// Rows in the task/vault tables that sanitize wipes wholesale.
		if _, err := tc.store.DB().Exec(`INSERT INTO file_gc_tasks (fs_id, task_id, file_id, storage_type, storage_ref, status)
			VALUES (?, ?, ?, 'db9', 'ref', 'QUEUED')`, tc.fsID, "gc-"+tc.pfx, "file-"+tc.pfx); err != nil {
			t.Fatalf("insert file_gc_tasks %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.DB().Exec(`INSERT INTO semantic_tasks (fs_id, task_id, task_type, resource_id, resource_version, status)
			VALUES (?, ?, 'embed', ?, 1, 'PENDING')`, tc.fsID, "st-"+tc.pfx, "res-"+tc.pfx); err != nil {
			t.Fatalf("insert semantic_tasks %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.DB().Exec(`INSERT INTO vault_deks (fs_id, wrapped_dek) VALUES (?, X'01')`, tc.fsID); err != nil {
			t.Fatalf("insert vault_deks %s: %v", tc.pfx, err)
		}
	}

	if err := storeA.SanitizeForkRuntimeState(ctx); err != nil {
		t.Fatalf("SanitizeForkRuntimeState A: %v", err)
	}

	// Tenant A runtime state is gone; confirmed data survives.
	if _, err := storeA.GetNode(ctx, "/san-a/pending.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("A pending node err = %v, want ErrNotFound", err)
	}
	inodeA, err := storeA.GetInode(ctx, "san-a-pending")
	if err != nil {
		t.Fatalf("A GetInode: %v", err)
	}
	if inodeA.Status != StatusDeleted || inodeA.ExpiresAt == nil {
		t.Fatalf("A pending inode = %+v, want DELETED with expires_at", inodeA)
	}
	contentA, err := storeA.GetContent(ctx, "san-a-pending")
	if err != nil {
		t.Fatalf("A GetContent: %v", err)
	}
	if contentA.StorageRef != "" {
		t.Fatalf("A pending content storage_ref = %q, want wiped", contentA.StorageRef)
	}
	if _, err := storeA.GetFile(ctx, "san-a-confirmed"); err != nil {
		t.Fatalf("A confirmed file must survive sanitize: %v", err)
	}
	if _, err := storeA.GetUpload(ctx, "san-a-upload"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("A upload err = %v, want ErrNotFound", err)
	}
	for _, tbl := range []string{"uploads", "file_gc_tasks", "semantic_tasks", "vault_deks"} {
		if n := countFsIDRows(t, storeA, tbl, fsA); n != 0 {
			t.Fatalf("A %s rows after sanitize = %d, want 0", tbl, n)
		}
	}

	// Tenant B is fully intact.
	if _, err := storeB.GetNode(ctx, "/san-b/pending.bin"); err != nil {
		t.Fatalf("B pending node: %v", err)
	}
	inodeB, err := storeB.GetInode(ctx, "san-b-pending")
	if err != nil {
		t.Fatalf("B GetInode: %v", err)
	}
	if inodeB.Status != StatusPending {
		t.Fatalf("B pending inode status = %q, want PENDING", inodeB.Status)
	}
	if _, err := storeB.GetUpload(ctx, "san-b-upload"); err != nil {
		t.Fatalf("B upload: %v", err)
	}
	for _, tbl := range []string{"uploads", "file_gc_tasks", "semantic_tasks", "vault_deks"} {
		if n := countFsIDRows(t, storeB, tbl, fsB); n != 1 {
			t.Fatalf("B %s rows = %d, want 1", tbl, n)
		}
	}
}
