package datastore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestStoreSharedShapeCollidingIDs is the regression suite for the shared
// scoping contract: entity ids (inode_id / file_id) are unique only within a
// tenant, so two fs_ids may legitimately hold the SAME entity id. Every
// multi-table relationship must correlate fs_id on all aliases/subqueries —
// before that was enforced, a colliding id let one tenant read another
// tenant's metadata, suppress its GC, accept a stale embedding writeback, or
// wipe its rows during fork sanitization.
func TestStoreSharedShapeCollidingIDs(t *testing.T) {
	installSharedCoreFSNoLegacy(t)
	ctx := context.Background()
	const fsA, fsB int64 = 4400001, 4400002
	now := time.Now()

	insert := func(store *Store, fileID string, status FileStatus, size int64, ref string) {
		t.Helper()
		f := &File{
			FileID: fileID, StorageType: StorageS3, StorageRef: ref,
			ContentType: "text/plain", SizeBytes: size, Revision: 1, Status: status,
			CreatedAt: now,
		}
		if status == StatusConfirmed {
			f.ConfirmedAt = &now
		}
		if err := store.InsertFile(ctx, f); err != nil {
			t.Fatalf("InsertFile %s: %v", fileID, err)
		}
	}
	link := func(store *Store, path, fileID string) {
		t.Helper()
		if err := store.EnsureParentDirs(ctx, path, genID); err != nil {
			t.Fatalf("EnsureParentDirs %s: %v", path, err)
		}
		name := path
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] == '/' {
				name = path[i+1:]
				break
			}
		}
		parent := path[:len(path)-len(name)]
		if err := store.InsertNode(ctx, &FileNode{
			NodeID: fileID + "-node", Path: path, ParentPath: parent,
			Name: name, FileID: fileID, CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertNode %s: %v", path, err)
		}
	}

	t.Run("StatDoesNotLeakAcrossCollidingIDs", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// A has only a dentry + pending inode; B holds a confirmed inode under
		// the SAME id. A's stat must not pick up B's confirmed metadata.
		insert(storeA, "col-stat", StatusPending, 0, "s3://a/pending")
		link(storeA, "/colvec/stat.bin", "col-stat")
		insert(storeB, "col-stat", StatusConfirmed, 999, "s3://b/confirmed")

		nf, err := storeA.StatTx(ctx, storeA.db, "/colvec/stat.bin")
		if err != nil {
			t.Fatalf("A StatTx: %v", err)
		}
		if nf.File != nil {
			t.Fatalf("A StatTx attached another tenant's file: %+v", nf.File)
		}
		lite, err := storeA.StatLite(ctx, "/colvec/stat.bin")
		if err != nil {
			t.Fatalf("A StatLite: %v", err)
		}
		if lite.File != nil {
			t.Fatalf("A StatLite attached another tenant's file: %+v", lite.File)
		}

		// Once A confirms its own inode under the same id, A's stat must
		// deterministically return A's row, never B's.
		if _, err := storeA.db.Exec(`UPDATE inodes SET status = 'CONFIRMED', size_bytes = 5, confirmed_at = ?
			WHERE fs_id = ? AND inode_id = 'col-stat'`, now, fsA); err != nil {
			t.Fatalf("confirm A inode: %v", err)
		}
		nf, err = storeA.StatTx(ctx, storeA.db, "/colvec/stat.bin")
		if err != nil {
			t.Fatalf("A StatTx after confirm: %v", err)
		}
		if nf.File == nil || nf.File.SizeBytes != 5 || nf.File.StorageRef != "s3://a/pending" {
			t.Fatalf("A StatTx = %+v, want A's own confirmed row", nf.File)
		}
	})

	t.Run("StorageRefCheckIgnoresOtherTenants", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// Only B has a confirmed S3 file at ref R. A's GC liveness check on R
		// must report dead so A never suppresses collection of its own object
		// because of B's row.
		insert(storeB, "col-ref", StatusConfirmed, 1, "s3://shared-bucket/object-r")

		ok, err := storeA.HasConfirmedS3StorageRef(ctx, StorageRefHash("s3://shared-bucket/object-r"), "s3://shared-bucket/object-r")
		if err != nil {
			t.Fatalf("A HasConfirmedS3StorageRef: %v", err)
		}
		if ok {
			t.Fatal("A sees B's confirmed storage ref as live")
		}
		ok, err = storeB.HasConfirmedS3StorageRef(ctx, StorageRefHash("s3://shared-bucket/object-r"), "s3://shared-bucket/object-r")
		if err != nil {
			t.Fatalf("B HasConfirmedS3StorageRef: %v", err)
		}
		if !ok {
			t.Fatal("B lost sight of its own confirmed storage ref")
		}
	})

	t.Run("MediaCountAndQuotaReadsAreIsolated", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// Same inode id, media content, confirmed — but only under B.
		insert(storeB, "col-media", StatusConfirmed, 10, "s3://b/img")
		if _, err := storeB.db.Exec(`UPDATE contents SET content_type = 'image/png' WHERE fs_id = ? AND inode_id = 'col-media'`, fsB); err != nil {
			t.Fatalf("mark B content image: %v", err)
		}

		var countA int64
		if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			countA, err = storeA.ConfirmedMediaFileCountTx(tx)
			return err
		}); err != nil {
			t.Fatalf("A ConfirmedMediaFileCountTx: %v", err)
		}
		if countA != 0 {
			t.Fatalf("A media count = %d, want 0 (B has the media file)", countA)
		}

		// A has a dentry whose pending inode id collides with B's confirmed
		// inode; A's confirmed-size read at that path must be zero.
		insert(storeA, "col-media", StatusPending, 0, "s3://a/pending")
		link(storeA, "/colvec/media.bin", "col-media")
		var sizeA int64
		if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			sizeA, err = storeA.ConfirmedFileSizeByPathTx(tx, "/colvec/media.bin")
			return err
		}); err != nil {
			t.Fatalf("A ConfirmedFileSizeByPathTx: %v", err)
		}
		if sizeA != 0 {
			t.Fatalf("A confirmed size = %d, want 0 (A's inode is pending)", sizeA)
		}
	})

	t.Run("UploadReservationIgnoresOtherTenants", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// B has a confirmed 100-byte file at the exact path A is uploading to.
		// A's 10-byte reservation must stay 10: joining B's dentry/inode would
		// subtract B's size and understate A's quota.
		insert(storeB, "col-up", StatusConfirmed, 100, "s3://b/existing")
		link(storeB, "/colvec/up.bin", "col-up")
		if err := storeA.InsertUpload(ctx, &Upload{
			UploadID: "col-up-upload", FileID: "col-up", TargetPath: "/colvec/up.bin",
			S3UploadID: "s3up-col", S3Key: "key-col", TotalSize: 10, PartSize: 5, PartsTotal: 2,
			Status: UploadUploading, IdempotencyKey: "idem-col-up", CreatedAt: now, UpdatedAt: now,
			ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("A InsertUpload: %v", err)
		}

		var reserved int64
		if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			reserved, err = storeA.ActiveUploadReservedBytesTx(tx)
			return err
		}); err != nil {
			t.Fatalf("A ActiveUploadReservedBytesTx: %v", err)
		}
		if reserved != 10 {
			t.Fatalf("A reserved = %d, want 10 (B's 100-byte file must not offset it)", reserved)
		}
	})

	t.Run("ListConfirmedReadsAreIsolated", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		insert(storeB, "col-list-1", StatusConfirmed, 1, "s3://b/l1")
		insert(storeB, "col-list-2", StatusConfirmed, 1, "s3://b/l2")

		summaries, _, err := storeA.ListConfirmedFileSummaries(ctx, "", 100)
		if err != nil {
			t.Fatalf("A ListConfirmedFileSummaries: %v", err)
		}
		for _, s := range summaries {
			if s.FileID == "col-list-1" || s.FileID == "col-list-2" {
				t.Fatalf("A summaries contain B's file: %+v", s)
			}
		}
		refs, _, err := storeA.ListConfirmedS3Refs(ctx, "", 100)
		if err != nil {
			t.Fatalf("A ListConfirmedS3Refs: %v", err)
		}
		for _, r := range refs {
			if r.StorageRef == "s3://b/l1" || r.StorageRef == "s3://b/l2" {
				t.Fatalf("A refs contain B's ref: %+v", r)
			}
		}
	})

	t.Run("OrphanScanIgnoresOtherTenantsDentries", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// A has a confirmed inode with no dentry of its own; B has a dentry
		// pointing at the SAME id. From A's perspective the inode is an orphan
		// and must be GC-able — B's dentry must not suppress that.
		insert(storeA, "col-orph", StatusConfirmed, 7, "s3://a/orphan")
		insert(storeB, "col-orph", StatusConfirmed, 7, "s3://b/live")
		link(storeB, "/colvec/orph.bin", "col-orph")

		var orphans []*File
		if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			orphans, err = storeA.scanOrphanedFilesByIDTx(ctx, tx, []string{"col-orph"})
			return err
		}); err != nil {
			t.Fatalf("A scanOrphanedFilesByIDTx: %v", err)
		}
		if len(orphans) != 1 || orphans[0].FileID != "col-orph" || orphans[0].StorageRef != "s3://a/orphan" {
			t.Fatalf("A orphans = %+v, want A's own col-orph row", orphans)
		}
	})

	t.Run("EmbeddingWritebackChecksOwnTenantRevision", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// Same inode id, different revisions: A at rev 1, B at rev 2. A
		// writeback gated on rev 2 must not fire by matching B's inode.
		insert(storeA, "col-emb", StatusConfirmed, 1, "s3://a/emb")
		insert(storeB, "col-emb", StatusConfirmed, 1, "s3://b/emb")
		if _, err := storeB.db.Exec(`UPDATE inodes SET revision = 2 WHERE fs_id = ? AND inode_id = 'col-emb'`, fsB); err != nil {
			t.Fatalf("bump B revision: %v", err)
		}

		updated, err := storeA.UpdateFileEmbedding(ctx, "col-emb", 2, []float32{0.1, 0.2})
		if err != nil {
			t.Fatalf("A UpdateFileEmbedding rev2: %v", err)
		}
		if updated {
			t.Fatal("A embedding writeback matched B's revision-2 inode")
		}
		updated, err = storeA.UpdateFileEmbedding(ctx, "col-emb", 1, []float32{0.1, 0.2})
		if err != nil {
			t.Fatalf("A UpdateFileEmbedding rev1: %v", err)
		}
		if !updated {
			t.Fatal("A embedding writeback lost its own revision-1 inode")
		}
	})

	t.Run("GCScanReadsOwnTenantRow", func(t *testing.T) {
		storeA := newSharedStore(t, fsA)
		storeB := newSharedStore(t, fsB)
		// Same id confirmed under both tenants with different refs: A's GC
		// scan must deterministically read A's ref, never B's.
		insert(storeA, "col-gc", StatusConfirmed, 1, "s3://a/gc")
		insert(storeB, "col-gc", StatusConfirmed, 1, "s3://b/gc")

		var f *File
		if err := storeA.InTx(ctx, func(tx *sql.Tx) error {
			var err error
			f, err = storeA.scanFileForGCTx(tx, "col-gc")
			return err
		}); err != nil {
			t.Fatalf("A scanFileForGCTx: %v", err)
		}
		if f.StorageRef != "s3://a/gc" {
			t.Fatalf("A GC scan storage_ref = %q, want s3://a/gc", f.StorageRef)
		}
	})

	t.Run("SanitizeDoesNotTouchCollidingIDs", func(t *testing.T) {
		// A real shared DB also carries the shared vault tables; sanitize
		// wipes them per fs_id, so install them in shared shape.
		installSharedVaultSchema(t)
		// Fresh fs_ids so earlier subtests' rows cannot interfere.
		const fsC, fsD int64 = 4400003, 4400004
		storeC := newSharedStore(t, fsC)
		storeD := newSharedStore(t, fsD)
		// C: pending inode + dentry under id X (upload-time runtime state).
		// D: confirmed file + dentry under the SAME id X. Sanitizing C must
		// leave every D row untouched.
		insert(storeC, "col-san", StatusPending, 0, "s3://c/pending")
		link(storeC, "/colvec/c-pending.bin", "col-san")
		insert(storeD, "col-san", StatusConfirmed, 42, "s3://d/confirmed")
		link(storeD, "/colvec/d-confirmed.bin", "col-san")

		if err := storeC.SanitizeForkRuntimeState(ctx); err != nil {
			t.Fatalf("C SanitizeForkRuntimeState: %v", err)
		}

		if _, err := storeD.GetNode(ctx, "/colvec/d-confirmed.bin"); err != nil {
			t.Fatalf("D dentry destroyed by C's sanitize: %v", err)
		}
		inodeD, err := storeD.GetInode(ctx, "col-san")
		if err != nil {
			t.Fatalf("D GetInode: %v", err)
		}
		if inodeD.Status != StatusConfirmed {
			t.Fatalf("D inode status = %q, want CONFIRMED", inodeD.Status)
		}
		contentD, err := storeD.GetContent(ctx, "col-san")
		if err != nil {
			t.Fatalf("D GetContent: %v", err)
		}
		if contentD.StorageRef != "s3://d/confirmed" {
			t.Fatalf("D content ref = %q, want untouched", contentD.StorageRef)
		}
		// C's own runtime state is gone.
		if _, err := storeC.GetNode(ctx, "/colvec/c-pending.bin"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("C pending dentry err = %v, want ErrNotFound", err)
		}
	})
}
