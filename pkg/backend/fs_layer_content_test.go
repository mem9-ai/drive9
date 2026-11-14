package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

// setQuotaForTest pins the tenant's file-count allowance at the given row count so the
// creation-only admission is exercised deterministically.
func setQuotaForTest(t *testing.T, b *Dat9Backend, fake *fakeMetaQuotaStore, tenantID string, used, max int64) {
	t.Helper()
	fake.mu.Lock()
	fake.ensureUsageLocked(tenantID).FileCount = used
	fake.config[tenantID] = &QuotaConfigView{MaxFileCount: max}
	fake.mu.Unlock()
	// Fresh caches: the admission reads the pinned config and usage, not an earlier snapshot.
	b.quotaConfigCache = newQuotaConfigCache(tenantID, b.tidbCloudOrgID, fake)
	b.quotaUsageCache = newQuotaUsageCache(tenantID, b.tidbCloudOrgID, fake, quotaUsageCacheTTL)
}

func objectLayerEntry(path, ref string, size int64) *datastore.FSLayerEntry {
	return &datastore.FSLayerEntry{
		Path:        path,
		Op:          datastore.FSLayerEntryOpUpsert,
		Kind:        datastore.FSLayerEntryKindFile,
		StorageType: string(datastore.StorageS3),
		StorageRef:  ref,
		SizeBytes:   size,
	}
}

// A tenant at its file-count cap must still be able to overwrite an existing object-backed
// file: an overwrite adds no file, so the creation-only admission may not run before the
// apply has found out that the path is taken.
func TestStoredObjectApplyOverwriteIgnoresFileCountCap(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()
	const tenantID = "quota-overwrite-tenant"
	fake := newFakeMetaQuotaStore()
	b.SetMetaQuotaStore(ctx, tenantID, fake)

	if _, err := b.WriteCtx(ctx, "/zone/upload.bin", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}

	// Control: at the cap, creating a new file is refused. The admission is live, so the
	// overwrite below is meaningful.
	setQuotaForTest(t, b, fake, tenantID, 1, 1)
	if _, _, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/new.bin", objectLayerEntry("/zone/new.bin", "layers/layer-quota/o1", 3)); !errors.Is(err, ErrFileCountQuotaExceeded) {
		t.Fatalf("create at the file-count cap err=%v, want ErrFileCountQuotaExceeded", err)
	}

	// The overwrite replaces the existing file's payload and reports no created inode.
	rev, createdIDs, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/upload.bin", objectLayerEntry("/zone/upload.bin", "layers/layer-quota/o2", 9))
	if err != nil {
		t.Fatalf("overwrite at the file-count cap: %v", err)
	}
	if rev <= 0 {
		t.Fatalf("overwrite revision = %d, want a committed revision", rev)
	}
	if len(createdIDs) != 0 {
		t.Fatalf("overwrite reported created ids %v, want none", createdIDs)
	}
	nf, err := b.StatNodeCtx(ctx, "/zone/upload.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil || nf.File.StorageRef != "layers/layer-quota/o2" || nf.File.SizeBytes != 9 {
		t.Fatalf("committed file = %+v, want the applied payload", nf.File)
	}
}

// The same ordering matters for a plan that stages one path twice: with a single free slot
// the first apply creates the file and the second must overwrite it instead of asking for a
// second slot.
func TestStoredObjectApplyRepeatedUpsertWithOneFreeSlot(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()
	const tenantID = "quota-repeat-tenant"
	fake := newFakeMetaQuotaStore()
	b.SetMetaQuotaStore(ctx, tenantID, fake)
	setQuotaForTest(t, b, fake, tenantID, 0, 1)

	if _, createdIDs, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/append.bin", objectLayerEntry("/zone/append.bin", "layers/layer-quota-r/o1", 3)); err != nil {
		t.Fatalf("first upsert with one free slot: %v", err)
	} else if len(createdIDs) == 0 {
		t.Fatal("first upsert reported no created inode")
	}
	if _, createdIDs, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/append.bin", objectLayerEntry("/zone/append.bin", "layers/layer-quota-r/o2", 5)); err != nil {
		t.Fatalf("second upsert of the same path: %v", err)
	} else if len(createdIDs) != 0 {
		t.Fatalf("second upsert reported created ids %v, want none", createdIDs)
	}
}

// A write whose base revision says the path existed must not resurrect a file removed since:
// the object-backed apply refuses it exactly like the inline twin.
func TestStoredObjectApplyConflictsWhenBasePathIsGone(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()
	entry := objectLayerEntry("/zone/gone.bin", "layers/layer-gone/o1", 3)
	entry.BaseRevision = 1
	if _, _, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/gone.bin", entry); !errors.Is(err, datastore.ErrRevisionConflict) {
		t.Fatalf("apply with a base revision at a missing path err=%v, want ErrRevisionConflict", err)
	}
	if _, err := b.StatNodeCtx(ctx, "/zone/gone.bin"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("the refused apply created the path: %v", err)
	}
	entry.BaseRevision = 0
	if _, createdIDs, err := b.WriteStoredObjectCreatedCtxIfRevision(ctx, "/zone/gone.bin", entry); err != nil {
		t.Fatalf("creation without a base revision: %v", err)
	} else if len(createdIDs) == 0 {
		t.Fatal("creation reported no created inode")
	}
}
